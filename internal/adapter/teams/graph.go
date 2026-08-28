package teams

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aenix-io/slack-connectors/internal/bridge"
	"github.com/aenix-io/slack-connectors/internal/config"
	"github.com/aenix-io/slack-connectors/internal/model"
	"github.com/aenix-io/slack-connectors/internal/store"
)

// ThrottledError reports that Graph refused a request for rate limiting and
// how long it asked the caller to wait. Nothing was created, so the request is
// safe to repeat.
type ThrottledError struct {
	RetryAfter time.Duration
	Status     string
	Code       string
	Message    string
}

func (e *ThrottledError) Error() string {
	return fmt.Sprintf("graph %s: %s: %s (retry after %s)",
		e.Status, e.Code, e.Message, e.RetryAfter)
}

// Unwrap makes a throttle answer the same question every other refusal does:
// the message did not reach Teams.
func (e *ThrottledError) Unwrap() error { return bridge.ErrNotSent }

// SubscriptionLifetime is the hard maximum Graph allows for channel message
// subscriptions.
// Kept strictly under one hour: Graph requires a lifecycleNotificationUrl on
// subscriptions expiring more than an hour out, and the bridge does not
// implement lifecycle notifications — the renewal loop is its lifecycle.
const SubscriptionLifetime = 55 * time.Minute

// DefaultRenewInterval is deliberately well short of SubscriptionLifetime so
// several attempts fit before expiry. Renewing once at 45 minutes against a
// 60-minute lifetime made a single transient error fatal: the next attempt
// landed after expiry, where it could only ever 404, and the bridge went deaf
// for good. Renewal is idempotent and cheap, so the margin costs little.
const DefaultRenewInterval = 15 * time.Minute

// GraphClient talks to the Microsoft Graph subscriptions endpoint.
type GraphClient struct {
	baseURL string
	http    *http.Client
}

// NewGraphClient builds a client. httpClient is expected to carry
// authentication, for example an oauth2 client-credentials transport.
func NewGraphClient(baseURL string, httpClient *http.Client) *GraphClient {
	return &GraphClient{baseURL: baseURL, http: httpClient}
}

// SubscriptionRequest is the create-subscription payload.
type SubscriptionRequest struct {
	Resource                string
	ChangeType              string
	NotificationURL         string
	ClientState             string
	IncludeResourceData     bool
	EncryptionCertificate   string
	EncryptionCertificateID string
	ExpirationDateTime      time.Time
}

// Subscription is the subset of the Graph response we retain.
type Subscription struct {
	ID                 string    `json:"id"`
	Resource           string    `json:"resource"`
	ExpirationDateTime time.Time `json:"expirationDateTime"`
}

type graphError struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// CreateSubscription registers a change notification subscription.
func (c *GraphClient) CreateSubscription(ctx context.Context, req SubscriptionRequest) (Subscription, error) {
	body := map[string]any{
		"resource":                req.Resource,
		"changeType":              req.ChangeType,
		"notificationUrl":         req.NotificationURL,
		"clientState":             req.ClientState,
		"includeResourceData":     req.IncludeResourceData,
		"encryptionCertificate":   req.EncryptionCertificate,
		"encryptionCertificateId": req.EncryptionCertificateID,
		"expirationDateTime":      req.ExpirationDateTime.UTC().Format(time.RFC3339),
	}
	return c.do(ctx, http.MethodPost, c.baseURL+"/subscriptions", body)
}

// RenewSubscription extends an existing subscription's expiry.
func (c *GraphClient) RenewSubscription(ctx context.Context, id string, expiry time.Time) (Subscription, error) {
	body := map[string]any{
		"expirationDateTime": expiry.UTC().Format(time.RFC3339),
	}
	return c.do(ctx, http.MethodPatch, c.baseURL+"/subscriptions/"+id, body)
}

func (c *GraphClient) do(ctx context.Context, method, url string, body map[string]any) (Subscription, error) {
	payload, err := c.send(ctx, method, url, body)
	if err != nil {
		return Subscription{}, err
	}
	var sub Subscription
	if err := json.Unmarshal(payload, &sub); err != nil {
		return Subscription{}, fmt.Errorf("decode subscription: %w", err)
	}
	return sub, nil
}

// send performs one JSON request and returns the response payload, turning a
// Graph error response into an error carrying its code and message.
//
// Errors are classified for the caller: anything that happened before the
// request left, and any response in which Graph itself refused, wraps
// bridge.ErrNotSent because nothing was created. A transport error or an
// unreadable response body does not — Graph may have committed the message and
// only the answer been lost, and saying otherwise would let the message be
// posted a second time.
func (c *GraphClient) send(ctx context.Context, method, url string, body map[string]any) ([]byte, error) {
	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("encode request: %w: %w", err, bridge.ErrNotSent)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(encoded))
	if err != nil {
		return nil, fmt.Errorf("build request: %w: %w", err, bridge.ErrNotSent)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call graph: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	payload, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode >= 300 {
		var ge graphError
		_ = json.Unmarshal(payload, &ge)

		if resp.StatusCode == http.StatusTooManyRequests {
			return nil, &ThrottledError{
				RetryAfter: retryAfter(resp.Header.Get("Retry-After")),
				Status:     resp.Status,
				Code:       ge.Error.Code,
				Message:    ge.Error.Message,
			}
		}
		if ge.Error.Message != "" {
			return nil, fmt.Errorf("graph %s: %s: %s: %w",
				resp.Status, ge.Error.Code, ge.Error.Message, bridge.ErrNotSent)
		}
		return nil, fmt.Errorf("graph %s: %w", resp.Status, bridge.ErrNotSent)
	}
	return payload, nil
}

// defaultRetryAfter is used when Graph throttles without saying for how long.
const defaultRetryAfter = 2 * time.Second

// maxRetryAfter caps how long a single wait may be. A delivery runs while
// holding its pair's lock, so an outsized value would stall the whole channel;
// past this point it is better to give up and let the message be retried by
// whatever replays it.
const maxRetryAfter = 30 * time.Second

// retryAfter reads the Retry-After header, which Graph sends as a number of
// seconds.
func retryAfter(header string) time.Duration {
	seconds, err := strconv.Atoi(strings.TrimSpace(header))
	if err != nil || seconds < 0 {
		return defaultRetryAfter
	}
	d := time.Duration(seconds) * time.Second
	if d > maxRetryAfter {
		return maxRetryAfter
	}
	return d
}

// DeleteSubscription removes a Graph subscription. A 404 response means the
// subscription already expired on its own and is treated as success, not an
// error — there is nothing left to tear down.
func (c *GraphClient) DeleteSubscription(ctx context.Context, id string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.baseURL+"/subscriptions/"+id, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("call graph: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		return nil
	}

	payload, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode >= 300 {
		var ge graphError
		if json.Unmarshal(payload, &ge) == nil && ge.Error.Message != "" {
			return fmt.Errorf("graph %s: %s: %s", resp.Status, ge.Error.Code, ge.Error.Message)
		}
		return fmt.Errorf("graph %s", resp.Status)
	}
	return nil
}

// ManagerOptions configures the subscription lifecycle.
type ManagerOptions struct {
	NotificationURL         string
	ClientState             string
	EncryptionCertificate   string
	EncryptionCertificateID string
	// RenewInterval defaults to DefaultRenewInterval when zero.
	RenewInterval time.Duration
}

// SubscriptionStore persists each pair's current Graph subscription so a
// restart can find and delete the previous one instead of leaking it.
type SubscriptionStore interface {
	SaveSubscription(ctx context.Context, pairID string, sub store.Subscription) error
	LoadSubscription(ctx context.Context, pairID string) (store.Subscription, error)
	DeleteSubscription(ctx context.Context, pairID string) error
}

// SubscriptionMetrics records subscription lifecycle observations for
// alerting. Defined here rather than importing internal/metrics directly, so
// the manager stays testable without a Prometheus registry.
type SubscriptionMetrics interface {
	// RecordRenewalFailure notes that a pair's renewal attempt failed. A
	// stalled renewal loop makes the bridge go deaf within an hour with no
	// other symptom, so this must be alertable.
	RecordRenewalFailure(pair string)

	// SetExpiry records the unix time at which a pair's current subscription
	// expires, so an alert can fire when it falls below the current time.
	SetExpiry(pair string, expiresAt time.Time)
}

// SubscriptionManager creates one subscription per configured pair and keeps
// them alive. It must run as a singleton: two instances race and create
// duplicate subscriptions, which double every message.
type SubscriptionManager struct {
	client  *GraphClient
	cfg     config.Config
	subs    SubscriptionStore
	metrics SubscriptionMetrics
	opts    ManagerOptions
	log     *slog.Logger

	mu     sync.Mutex
	active map[string]string // pair id -> subscription id
}

// NewSubscriptionManager builds a manager.
func NewSubscriptionManager(
	client *GraphClient,
	cfg config.Config,
	subs SubscriptionStore,
	metrics SubscriptionMetrics,
	opts ManagerOptions,
	log *slog.Logger,
) *SubscriptionManager {
	if opts.RenewInterval == 0 {
		opts.RenewInterval = DefaultRenewInterval
	}
	return &SubscriptionManager{
		client:  client,
		cfg:     cfg,
		subs:    subs,
		metrics: metrics,
		opts:    opts,
		log:     log,
		active:  make(map[string]string, len(cfg.PairsFor(model.PlatformTeams))),
	}
}

// Run creates the subscriptions and keeps them alive until ctx is cancelled.
func (m *SubscriptionManager) Run(ctx context.Context) error {
	m.createAll(ctx)

	ticker := time.NewTicker(m.opts.RenewInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			m.reconcile(ctx)
		}
	}
}

// Healthy reports nil when every configured pair has a live subscription, and
// otherwise an error naming those that do not.
//
// This is what readiness should reflect for the Teams direction: a pod without
// a subscription receives nothing, and answering Ready anyway is exactly how
// the bridge went deaf with no symptom an operator could see.
func (m *SubscriptionManager) Healthy() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var missing []string
	for _, pair := range m.cfg.PairsFor(model.PlatformTeams) {
		if _, live := m.active[pair.ID]; !live {
			missing = append(missing, pair.ID)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("no live graph subscription for pair(s): %s",
			strings.Join(missing, ", "))
	}
	return nil
}

// reconcile brings every configured pair back to having a live subscription.
//
// A pair with none is created rather than skipped: a creation that failed at
// startup used to leave the pair absent from the active set forever, and the
// tick only ever renewed what was already there — so the bridge stayed deaf
// for the life of the process with nothing but two error lines at t=0.
func (m *SubscriptionManager) reconcile(ctx context.Context) {
	m.renewAll(ctx)

	for _, pair := range m.cfg.PairsFor(model.PlatformTeams) {
		m.mu.Lock()
		_, live := m.active[pair.ID]
		m.mu.Unlock()
		if live {
			continue
		}
		m.log.Info("pair has no live subscription; creating one", "pair", pair.ID)
		m.createPair(ctx, pair)
	}
}

func (m *SubscriptionManager) createAll(ctx context.Context) {
	for _, pair := range m.cfg.PairsFor(model.PlatformTeams) {
		m.createPair(ctx, pair)
	}
}

// createPair registers a fresh subscription for one pair, replacing whatever
// was stored for it before.
func (m *SubscriptionManager) createPair(ctx context.Context, pair config.Pair) {
	m.deleteStale(ctx, pair.ID)

	resource := fmt.Sprintf("/teams/%s/channels/%s/messages", pair.Teams.TeamID, pair.Teams.ChannelID)
	sub, err := m.client.CreateSubscription(ctx, SubscriptionRequest{
		Resource:                resource,
		ChangeType:              "created",
		NotificationURL:         m.opts.NotificationURL,
		ClientState:             m.opts.ClientState,
		IncludeResourceData:     true,
		EncryptionCertificate:   m.opts.EncryptionCertificate,
		EncryptionCertificateID: m.opts.EncryptionCertificateID,
		ExpirationDateTime:      time.Now().Add(SubscriptionLifetime),
	})
	if err != nil {
		// Left absent from the active set on purpose: the next reconcile tick
		// finds it missing and tries again.
		m.log.Error("create subscription", "pair", pair.ID, "error", err)
		return
	}
	m.mu.Lock()
	m.active[pair.ID] = sub.ID
	m.mu.Unlock()
	m.log.Info("subscription created",
		"pair", pair.ID, "subscription_id", sub.ID, "expires", sub.ExpirationDateTime)
	m.persist(ctx, pair.ID, sub)
}

// deleteStale tears down whatever subscription was stored for pairID before
// the caller creates a fresh one. Graph subscriptions outlive this process,
// so without this every restart leaves the previous subscription delivering
// to the same URL for up to another hour — every message arrives twice.
//
// A failure here is logged and never stops the new subscription from being
// created: going deaf is worse than a window of double delivery.
func (m *SubscriptionManager) deleteStale(ctx context.Context, pairID string) {
	prev, err := m.subs.LoadSubscription(ctx, pairID)
	if errors.Is(err, store.ErrNotFound) {
		return
	}
	if err != nil {
		m.log.Error("load stored subscription", "pair", pairID, "error", err)
		return
	}

	if err := m.client.DeleteSubscription(ctx, prev.ID); err != nil {
		// The Graph subscription is still live under prev.ID. Deleting the
		// stored record now would strand it: nothing would remain to find or
		// remove it on a later restart, and it would keep delivering to this
		// URL for up to another hour. Leave the record in place — the next
		// deleteStale call will try again.
		m.log.Error("delete stale subscription", "pair", pairID, "subscription_id", prev.ID, "error", err)
		return
	}
	if err := m.subs.DeleteSubscription(ctx, pairID); err != nil {
		m.log.Error("delete stored subscription record", "pair", pairID, "error", err)
	}
}

// persist stores the created or renewed subscription and updates the expiry
// gauge so an alert can fire before it lapses.
func (m *SubscriptionManager) persist(ctx context.Context, pairID string, sub Subscription) {
	if err := m.subs.SaveSubscription(ctx, pairID, store.Subscription{
		ID:        sub.ID,
		ExpiresAt: sub.ExpirationDateTime,
	}); err != nil {
		m.log.Error("persist subscription", "pair", pairID, "error", err)
	}
	m.metrics.SetExpiry(pairID, sub.ExpirationDateTime)
}

func (m *SubscriptionManager) renewAll(ctx context.Context) {
	m.mu.Lock()
	current := make(map[string]string, len(m.active))
	for pairID, subID := range m.active {
		current[pairID] = subID
	}
	m.mu.Unlock()

	for pairID, subID := range current {
		sub, err := m.client.RenewSubscription(ctx, subID, time.Now().Add(SubscriptionLifetime))
		if err != nil {
			// A stalled renewal loop makes the bridge go deaf within an
			// hour with no other symptom. This must be alertable.
			m.log.Error("renew subscription", "pair", pairID, "subscription_id", subID, "error", err)
			m.metrics.RecordRenewalFailure(pairID)

			// Drop the id so reconcile creates a fresh subscription. Once
			// Graph has forgotten a subscription, patching it can only ever
			// fail, so retrying the same id forever is how the bridge used to
			// stay deaf after a single transient renewal error.
			m.mu.Lock()
			delete(m.active, pairID)
			m.mu.Unlock()
			continue
		}
		m.log.Info("subscription renewed",
			"pair", pairID, "subscription_id", sub.ID, "expires", sub.ExpirationDateTime)
		m.persist(ctx, pairID, sub)
	}
}

// MessagePage is one page of a channel message listing.
type MessagePage struct {
	Messages []chatMessage `json:"value"`
	NextLink string        `json:"@odata.nextLink"`
}

// FetchMessagePage reads one page of channel history, newest first. url is
// either an absolute nextLink returned by a previous call, or a path relative
// to the Graph base URL for the first call.
func (c *GraphClient) FetchMessagePage(ctx context.Context, url string) (MessagePage, error) {
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		url = c.baseURL + url
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return MessagePage{}, fmt.Errorf("build message request: %w", err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return MessagePage{}, fmt.Errorf("call messages: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	payload, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return MessagePage{}, fmt.Errorf("read message page: %w", err)
	}
	if resp.StatusCode >= 300 {
		var ge graphError
		if json.Unmarshal(payload, &ge) == nil && ge.Error.Message != "" {
			return MessagePage{}, fmt.Errorf("graph %s: %s: %s",
				resp.Status, ge.Error.Code, ge.Error.Message)
		}
		return MessagePage{}, fmt.Errorf("graph %s", resp.Status)
	}

	var page MessagePage
	if err := json.Unmarshal(payload, &page); err != nil {
		return MessagePage{}, fmt.Errorf("decode message page: %w", err)
	}
	return page, nil
}

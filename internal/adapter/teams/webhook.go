package teams

import (
	"context"
	"crypto/rsa"
	"crypto/subtle"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"sync"
	"time"

	"github.com/aenix-io/slack-connectors/internal/config"
	"github.com/aenix-io/slack-connectors/internal/model"
)

// maxNotificationBody caps the request body we are willing to read.
const maxNotificationBody = 4 << 20 // 4 MiB

// maxConcurrentProcessing bounds how many notifications are decrypted and
// dispatched at once, so a burst cannot exhaust memory.
const maxConcurrentProcessing = 32

// maxValidationToken caps the handshake token we are willing to echo back.
// Graph's tokens are short; without an explicit bound the only limit is an
// incidental Go server default that this code neither chose nor documents.
const maxValidationToken = 2048

// channelPattern extracts the channel id from a notification resource path
// such as teams('<team>')/channels('<channel>')/messages('<id>').
var channelPattern = regexp.MustCompile(`channels\('([^']+)'\)`)

// Handler serves the Microsoft Graph change notification endpoint.
type Handler struct {
	cfg         config.Config
	priv        *rsa.PrivateKey
	clientState string
	dispatch    func(context.Context, model.Message) error
	log         *slog.Logger

	// inFlight tracks asynchronous processing so shutdown can wait for it.
	inFlight sync.WaitGroup
	// sem bounds concurrent processing.
	sem chan struct{}
}

// NewHandler builds the notification endpoint. dispatch receives every
// accepted message and must not block the caller for long.
func NewHandler(
	cfg config.Config,
	priv *rsa.PrivateKey,
	clientState string,
	dispatch func(context.Context, model.Message) error,
	log *slog.Logger,
) *Handler {
	return &Handler{
		cfg:         cfg,
		priv:        priv,
		clientState: clientState,
		dispatch:    dispatch,
		log:         log,
		sem:         make(chan struct{}, maxConcurrentProcessing),
	}
}

// Drain waits for in-flight notification processing to finish, or until ctx
// expires. Without it, shutdown discards notifications that were acknowledged
// with 202 but not yet delivered — the bridge would have promised Microsoft it
// took responsibility for a message and then dropped it.
func (h *Handler) Drain(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		h.inFlight.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// notificationEnvelope is the changeNotificationCollection wire format.
type notificationEnvelope struct {
	Value []notification `json:"value"`
}

type notification struct {
	SubscriptionID   string           `json:"subscriptionId"`
	ChangeType       string           `json:"changeType"`
	ClientState      string           `json:"clientState"`
	Resource         string           `json:"resource"`
	EncryptedContent EncryptedContent `json:"encryptedContent"`
}

// chatMessage is the subset of the Graph chatMessage resource we consume.
type chatMessage struct {
	ID                   string `json:"id"`
	ReplyToID            string `json:"replyToId"`
	MessageType          string `json:"messageType"`
	CreatedDateTime      string `json:"createdDateTime"`
	LastModifiedDateTime string `json:"lastModifiedDateTime"`
	// Replies is populated only by recovery's $expand=replies listing;
	// change-notification payloads leave it empty.
	Replies []chatMessage `json:"replies"`
	Body    struct {
		ContentType string `json:"contentType"`
		Content     string `json:"content"`
	} `json:"body"`
	From struct {
		User *struct {
			ID          string `json:"id"`
			DisplayName string `json:"displayName"`
		} `json:"user"`
		Application *struct {
			ID          string `json:"id"`
			DisplayName string `json:"displayName"`
		} `json:"application"`
	} `json:"from"`
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Content-Type-Options", "nosniff")

	// Subscription validation handshake: echo the token in plain text.
	if token := r.URL.Query().Get("validationToken"); token != "" {
		if len(token) > maxValidationToken {
			h.log.Warn("rejecting oversized validation token", "length", len(token))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		//nolint:gosec // Echoing the token verbatim is the documented Graph
		// handshake. Content-Type is text/plain and nosniff is set above.
		if _, err := io.WriteString(w, token); err != nil {
			h.log.Error("write validation response", "error", err)
		}
		return
	}

	// Claim a processing slot BEFORE reading the body. Bounding only the
	// concurrent work would still let every accepted request spawn a
	// goroutine pinning up to maxNotificationBody bytes while it waits, which
	// on an internet-facing endpoint is an unbounded-memory vector. Under
	// saturation a 503 tells Graph to retry, so nothing is lost; answering
	// 202 and queueing without bound would promise delivery we cannot keep.
	select {
	case h.sem <- struct{}{}:
	default:
		h.log.Warn("notification processing saturated; asking Graph to retry")
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxNotificationBody))
	if err != nil {
		<-h.sem
		h.log.Error("read notification body", "error", err)
		w.WriteHeader(http.StatusAccepted)
		return
	}

	// Microsoft is explicit: acknowledge before validating or processing.
	// Answering late causes redundant retries and reveals validation
	// outcomes to whoever sent the request.
	w.WriteHeader(http.StatusAccepted)

	// Processing continues after the response, so it must survive the
	// request context but still be waited on at shutdown.
	ctx := context.WithoutCancel(r.Context())
	h.inFlight.Add(1)
	go func() {
		defer h.inFlight.Done()
		defer func() { <-h.sem }()
		h.process(ctx, body)
	}()
}

func (h *Handler) process(ctx context.Context, body []byte) {
	var env notificationEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		h.log.Error("decode notification envelope", "error", err)
		return
	}

	for _, n := range env.Value {
		if subtle.ConstantTimeCompare([]byte(n.ClientState), []byte(h.clientState)) != 1 {
			h.log.Warn("rejecting notification with unexpected clientState",
				"subscription_id", n.SubscriptionID)
			continue
		}

		channelID := extractChannelID(n.Resource)
		pair, ok := h.cfg.PairFor(model.PlatformTeams, channelID)
		if !ok {
			h.log.Debug("ignoring notification for an unconfigured channel",
				"channel_id", channelID)
			continue
		}

		plaintext, err := Decrypt(h.priv, n.EncryptedContent)
		if err != nil {
			h.log.Error("decrypt resource data",
				"subscription_id", n.SubscriptionID, "error", err)
			continue
		}

		var cm chatMessage
		if err := json.Unmarshal(plaintext, &cm); err != nil {
			h.log.Error("decode chat message", "subscription_id", n.SubscriptionID, "error", err)
			continue
		}

		// Only real user or app messages are mirrored. Join and rename
		// events arrive as systemEventMessage and would read as noise.
		if cm.MessageType != "" && cm.MessageType != "message" {
			h.log.Debug("skipping non-message event", "message_type", cm.MessageType)
			continue
		}

		// The outcome is the caller's to record; this endpoint already
		// acknowledged the notification and has nothing left to report to
		// Graph either way.
		_ = h.dispatch(ctx, toCanonical(cm, pair.ID, n.ChangeType))
	}
}

func toCanonical(cm chatMessage, pairID, changeType string) model.Message {
	msg := model.Message{
		Platform:  model.PlatformTeams,
		ID:        cm.ID,
		ParentID:  cm.ReplyToID,
		PairID:    pairID,
		Text:      cm.Body.Content,
		Kind:      eventKind(changeType),
		Timestamp: parseTime(cm.CreatedDateTime),
	}
	switch {
	case cm.From.User != nil:
		msg.Author = model.Author{
			ExternalID:  cm.From.User.ID,
			DisplayName: cm.From.User.DisplayName,
		}
	case cm.From.Application != nil:
		msg.Author = model.Author{
			ExternalID:  cm.From.Application.ID,
			DisplayName: cm.From.Application.DisplayName,
		}
		// Loop prevention layer one: anything posted by an application is
		// assumed to be ours and is never mirrored back.
		msg.FromBridge = true
	}
	return msg
}

func eventKind(changeType string) model.EventKind {
	switch changeType {
	case "updated":
		return model.EventEdited
	case "deleted":
		return model.EventDeleted
	default:
		return model.EventCreated
	}
}

func parseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

func extractChannelID(resource string) string {
	m := channelPattern.FindStringSubmatch(resource)
	if len(m) != 2 {
		return ""
	}
	return m[1]
}

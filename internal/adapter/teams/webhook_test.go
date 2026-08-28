package teams

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aenix-io/slack-connectors/internal/config"
	"github.com/aenix-io/slack-connectors/internal/model"
)

const testClientState = "s3cr3t-client-state"

func webhookConfig() config.Config {
	return config.Config{Pairs: []config.Pair{{
		ID:    "acme",
		Teams: &config.TeamsEndpoint{TeamID: "00000000-0000-0000-0000-000000000000", ChannelID: "19:xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx@thread.tacv2"},
		Slack: &config.SlackEndpoint{ChannelID: "C01ABCDEF"},
	}}}
}

// collector captures dispatched messages.
type collector struct {
	mu   sync.Mutex
	msgs []model.Message
	done chan struct{}
	once sync.Once
}

func newCollector() *collector {
	return &collector{done: make(chan struct{})}
}

func (c *collector) dispatch(_ context.Context, m model.Message) error {
	c.mu.Lock()
	c.msgs = append(c.msgs, m)
	c.mu.Unlock()
	c.once.Do(func() { close(c.done) })
	return nil
}

func (c *collector) wait(t *testing.T) []model.Message {
	t.Helper()
	select {
	case <-c.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a dispatched message")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]model.Message(nil), c.msgs...)
}

func newHandler(t *testing.T, key *rsa.PrivateKey, c *collector) *Handler {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewHandler(webhookConfig(), key, testClientState, c.dispatch, log)
}

func TestValidationHandshake(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	h := newHandler(t, key, newCollector())

	req := httptest.NewRequest(http.MethodPost, "/webhook/graph?validationToken=abc%20123", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Body.String(); got != "abc 123" {
		t.Fatalf("body = %q, want the decoded validation token", got)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("Content-Type = %q, want text/plain", ct)
	}
}

func notificationBody(t *testing.T, key *rsa.PrivateKey, resource, clientState string, payload map[string]any) string {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	enc := encryptLikeGraph(t, &key.PublicKey, raw)
	body := map[string]any{
		"value": []map[string]any{{
			"subscriptionId":   "76222963-cc7b-42d2-882d-8aaa69cb2ba3",
			"changeType":       "created",
			"clientState":      clientState,
			"resource":         resource,
			"encryptedContent": enc,
		}},
	}
	out, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	return string(out)
}

func chatMessagePayload() map[string]any {
	return map[string]any{
		"id":              "1565293727947",
		"createdDateTime": "2026-07-24T09:00:00Z",
		"messageType":     "message",
		"body": map[string]any{
			"contentType": "html",
			"content":     "<p>hello <strong>world</strong></p>",
		},
		"from": map[string]any{
			"user": map[string]any{
				"id":          "aaaabbbb-0000-cccc-1111-dddd2222eeee",
				"displayName": "Ivan Petrov",
			},
		},
	}
}

const testResource = `teams('00000000-0000-0000-0000-000000000000')/channels('19:xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx@thread.tacv2')/messages('1565293727947')`

func TestNotificationRespondsAccepted(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	c := newCollector()
	h := newHandler(t, key, c)

	body := notificationBody(t, key, testResource, testClientState, chatMessagePayload())
	req := httptest.NewRequest(http.MethodPost, "/webhook/graph", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rec.Code)
	}
}

func TestNotificationDispatchesCanonicalMessage(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	c := newCollector()
	h := newHandler(t, key, c)

	body := notificationBody(t, key, testResource, testClientState, chatMessagePayload())
	req := httptest.NewRequest(http.MethodPost, "/webhook/graph", strings.NewReader(body))
	h.ServeHTTP(httptest.NewRecorder(), req)

	msgs := c.wait(t)
	if len(msgs) != 1 {
		t.Fatalf("dispatched %d messages, want 1", len(msgs))
	}
	got := msgs[0]
	if got.Platform != model.PlatformTeams {
		t.Fatalf("Platform = %q, want teams", got.Platform)
	}
	if got.ID != "1565293727947" {
		t.Fatalf("ID = %q, want 1565293727947", got.ID)
	}
	if got.PairID != "acme" {
		t.Fatalf("PairID = %q, want acme", got.PairID)
	}
	if got.Author.DisplayName != "Ivan Petrov" {
		t.Fatalf("Author.DisplayName = %q", got.Author.DisplayName)
	}
	if got.Text != "<p>hello <strong>world</strong></p>" {
		t.Fatalf("Text = %q", got.Text)
	}
	if got.Kind != model.EventCreated {
		t.Fatalf("Kind = %q, want created", got.Kind)
	}
	if got.ParentID != "" {
		t.Fatalf("ParentID = %q, want empty for a thread root", got.ParentID)
	}
}

func TestNotificationExtractsReplyParent(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	c := newCollector()
	h := newHandler(t, key, c)

	const replyResource = `teams('00000000-0000-0000-0000-000000000000')/channels('19:xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx@thread.tacv2')/messages('1565045424600')/replies('1565047490246')`
	payload := chatMessagePayload()
	payload["id"] = "1565047490246"
	payload["replyToId"] = "1565045424600"

	body := notificationBody(t, key, replyResource, testClientState, payload)
	req := httptest.NewRequest(http.MethodPost, "/webhook/graph", strings.NewReader(body))
	h.ServeHTTP(httptest.NewRecorder(), req)

	msgs := c.wait(t)
	if msgs[0].ParentID != "1565045424600" {
		t.Fatalf("ParentID = %q, want 1565045424600", msgs[0].ParentID)
	}
}

func TestNotificationRejectsWrongClientState(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	c := newCollector()
	h := newHandler(t, key, c)

	body := notificationBody(t, key, testResource, "wrong-state", chatMessagePayload())
	req := httptest.NewRequest(http.MethodPost, "/webhook/graph", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 even for a rejected notification", rec.Code)
	}
	select {
	case <-c.done:
		t.Fatal("a notification with the wrong clientState must not be dispatched")
	case <-time.After(300 * time.Millisecond):
	}
}

func TestNotificationIgnoresUnknownChannel(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	c := newCollector()
	h := newHandler(t, key, c)

	const foreign = `teams('00000000-0000-0000-0000-000000000000')/channels('19:unknown@thread.tacv2')/messages('1')`
	body := notificationBody(t, key, foreign, testClientState, chatMessagePayload())
	req := httptest.NewRequest(http.MethodPost, "/webhook/graph", strings.NewReader(body))
	h.ServeHTTP(httptest.NewRecorder(), req)

	select {
	case <-c.done:
		t.Fatal("a notification for an unconfigured channel must not be dispatched")
	case <-time.After(300 * time.Millisecond):
	}
}

func TestNotificationSkipsSystemMessages(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	c := newCollector()
	h := newHandler(t, key, c)

	payload := chatMessagePayload()
	payload["messageType"] = "systemEventMessage"
	body := notificationBody(t, key, testResource, testClientState, payload)
	req := httptest.NewRequest(http.MethodPost, "/webhook/graph", strings.NewReader(body))
	h.ServeHTTP(httptest.NewRecorder(), req)

	select {
	case <-c.done:
		t.Fatal("system event messages must not be mirrored")
	case <-time.After(300 * time.Millisecond):
	}
}

func TestNotificationMarksApplicationAuthoredMessages(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	c := newCollector()
	h := newHandler(t, key, c)

	payload := chatMessagePayload()
	payload["from"] = map[string]any{
		"application": map[string]any{
			"id":          "925bff9f-f6e2-4a69-b858-f71ea2b9b6d0",
			"displayName": "Slack Bridge",
		},
	}
	body := notificationBody(t, key, testResource, testClientState, payload)
	req := httptest.NewRequest(http.MethodPost, "/webhook/graph", strings.NewReader(body))
	h.ServeHTTP(httptest.NewRecorder(), req)

	msgs := c.wait(t)
	if !msgs[0].FromBridge {
		t.Fatal("an application-authored message must be marked FromBridge")
	}
}

func TestNotificationAsksGraphToRetryWhenSaturated(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	c := newCollector()
	h := newHandler(t, key, c)

	// Occupy every processing slot.
	for i := 0; i < cap(h.sem); i++ {
		h.sem <- struct{}{}
	}
	t.Cleanup(func() {
		for i := 0; i < cap(h.sem); i++ {
			<-h.sem
		}
	})

	body := notificationBody(t, key, testResource, testClientState, chatMessagePayload())
	req := httptest.NewRequest(http.MethodPost, "/webhook/graph", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	// 503 rather than 202: Graph retries, so the message is delayed rather
	// than accepted-and-dropped, and memory stays bounded.
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 under saturation", rec.Code)
	}
	select {
	case <-c.done:
		t.Fatal("a rejected notification must not be dispatched")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestOversizedValidationTokenIsRejected(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	h := newHandler(t, key, newCollector())

	token := strings.Repeat("a", maxValidationToken+1)
	req := httptest.NewRequest(http.MethodPost, "/webhook/graph?validationToken="+token, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for an oversized token", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("body = %q, want an oversized token not to be echoed", rec.Body.String())
	}
}

func TestResponsesDisableContentSniffing(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	h := newHandler(t, key, newCollector())

	handshake := httptest.NewRecorder()
	h.ServeHTTP(handshake, httptest.NewRequest(http.MethodPost, "/webhook/graph?validationToken=abc", nil))
	if got := handshake.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("handshake X-Content-Type-Options = %q, want nosniff", got)
	}

	body := notificationBody(t, key, testResource, testClientState, chatMessagePayload())
	notification := httptest.NewRecorder()
	h.ServeHTTP(notification, httptest.NewRequest(http.MethodPost, "/webhook/graph", strings.NewReader(body)))
	if got := notification.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("notification X-Content-Type-Options = %q, want nosniff", got)
	}
}

func TestDrainWaitsForInFlightProcessing(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	release := make(chan struct{})
	var dispatched atomic.Bool
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := NewHandler(webhookConfig(), key, testClientState,
		func(context.Context, model.Message) error {
			<-release
			dispatched.Store(true)
			return nil
		}, log)

	body := notificationBody(t, key, testResource, testClientState, chatMessagePayload())
	req := httptest.NewRequest(http.MethodPost, "/webhook/graph", strings.NewReader(body))
	h.ServeHTTP(httptest.NewRecorder(), req)

	drained := make(chan error, 1)
	go func() { drained <- h.Drain(context.Background()) }()

	select {
	case <-drained:
		t.Fatal("Drain() returned while a notification was still being processed")
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-drained:
		if err != nil {
			t.Fatalf("Drain(): %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Drain() did not return after processing finished")
	}
	if !dispatched.Load() {
		t.Fatal("the in-flight message was dropped instead of processed")
	}
}

func TestDrainRespectsContextDeadline(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := NewHandler(webhookConfig(), key, testClientState,
		func(context.Context, model.Message) error { <-release; return nil }, log)

	body := notificationBody(t, key, testResource, testClientState, chatMessagePayload())
	req := httptest.NewRequest(http.MethodPost, "/webhook/graph", strings.NewReader(body))
	h.ServeHTTP(httptest.NewRecorder(), req)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := h.Drain(ctx); err == nil {
		t.Fatal("Drain() error = nil, want the deadline to surface rather than hang shutdown")
	}
}

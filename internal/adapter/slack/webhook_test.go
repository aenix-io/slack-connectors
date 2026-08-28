package slack

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aenix-io/slack-connectors/internal/config"
	"github.com/aenix-io/slack-connectors/internal/model"
)

const testSigningSecret = "8f742231b10e8888abcd99yyyzzz85a5"

func webhookConfig() config.Config {
	return config.Config{Pairs: []config.Pair{{
		ID:    "pair-a",
		Teams: &config.TeamsEndpoint{TeamID: "team-1", ChannelID: "channel-1"},
		Slack: &config.SlackEndpoint{ChannelID: "C01ABCDEF"},
	}}}
}

// collector records dispatched messages.
type collector struct {
	mu   sync.Mutex
	msgs []model.Message
}

func (c *collector) dispatch(_ context.Context, msg model.Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.msgs = append(c.msgs, msg)
	return nil
}

func (c *collector) all() []model.Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]model.Message, len(c.msgs))
	copy(out, c.msgs)
	return out
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// signedRequest builds a request signed the way Slack signs it.
func signedRequest(t *testing.T, body string, ts time.Time) *http.Request {
	t.Helper()
	stamp := strconv.FormatInt(ts.Unix(), 10)
	mac := hmac.New(sha256.New, []byte(testSigningSecret))
	mac.Write([]byte("v0:" + stamp + ":" + body))
	sig := "v0=" + hex.EncodeToString(mac.Sum(nil))

	req := httptest.NewRequest(http.MethodPost, "/webhook/slack", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Slack-Request-Timestamp", stamp)
	req.Header.Set("X-Slack-Signature", sig)
	return req
}

// messageEvent builds an event_callback payload from inline event fields.
func messageEvent(fields string) string {
	return fmt.Sprintf(`{"type":"event_callback","event":{%s}}`, fields)
}

// serve runs one request through a handler and drains it, so assertions see
// the result of asynchronous processing.
func serve(t *testing.T, h *Handler, req *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := h.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	return rec
}

func TestHandlerURLVerificationEchoesChallenge(t *testing.T) {
	var got collector
	h := NewHandler(testSigningSecret, webhookConfig(), nopDirectory{}, got.dispatch, testLogger())

	body := `{"type":"url_verification","challenge":"3eZbrw1aB"}`
	rec := serve(t, h, signedRequest(t, body, time.Now()))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec.Body.String() != "3eZbrw1aB" {
		t.Fatalf("body = %q, want the challenge verbatim", rec.Body.String())
	}
	if len(got.all()) != 0 {
		t.Fatal("a handshake must not dispatch a message")
	}
}

func TestHandlerValidSignatureIsDispatched(t *testing.T) {
	var got collector
	h := NewHandler(testSigningSecret, webhookConfig(), nopDirectory{}, got.dispatch, testLogger())

	body := messageEvent(`"type":"message","channel":"C01ABCDEF","user":"U123",` +
		`"text":"hello","ts":"1700000000.000100"`)
	rec := serve(t, h, signedRequest(t, body, time.Now()))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	msgs := got.all()
	if len(msgs) != 1 {
		t.Fatalf("dispatched %d messages, want 1", len(msgs))
	}
	m := msgs[0]
	if m.Platform != model.PlatformSlack {
		t.Fatalf("Platform = %q, want %q", m.Platform, model.PlatformSlack)
	}
	if m.ID != "1700000000.000100" {
		t.Fatalf("ID = %q, want the event ts", m.ID)
	}
	if m.PairID != "pair-a" {
		t.Fatalf("PairID = %q, want pair-a", m.PairID)
	}
	if m.Author.ExternalID != "U123" {
		t.Fatalf("Author.ExternalID = %q, want U123", m.Author.ExternalID)
	}
	if m.Text != "hello" {
		t.Fatalf("Text = %q, want hello", m.Text)
	}
	if m.Kind != model.EventCreated {
		t.Fatalf("Kind = %q, want %q", m.Kind, model.EventCreated)
	}
	if m.FromBridge {
		t.Fatal("FromBridge = true, want false for a human message")
	}
	if want := time.Unix(1700000000, 100000); !m.Timestamp.Equal(want) {
		t.Fatalf("Timestamp = %v, want %v", m.Timestamp, want)
	}
}

func TestHandlerWrongSignatureIsRejected(t *testing.T) {
	var got collector
	h := NewHandler(testSigningSecret, webhookConfig(), nopDirectory{}, got.dispatch, testLogger())

	body := messageEvent(`"type":"message","channel":"C01ABCDEF","user":"U123",` +
		`"text":"hello","ts":"1700000000.000100"`)
	req := signedRequest(t, body, time.Now())
	req.Header.Set("X-Slack-Signature", "v0=deadbeef")
	rec := serve(t, h, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	if len(got.all()) != 0 {
		t.Fatal("a request with a bad signature must not dispatch")
	}
}

func TestHandlerStaleTimestampIsRejected(t *testing.T) {
	var got collector
	h := NewHandler(testSigningSecret, webhookConfig(), nopDirectory{}, got.dispatch, testLogger())

	body := messageEvent(`"type":"message","channel":"C01ABCDEF","user":"U123",` +
		`"text":"hello","ts":"1700000000.000100"`)
	rec := serve(t, h, signedRequest(t, body, time.Now().Add(-6*time.Minute)))

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	if len(got.all()) != 0 {
		t.Fatal("a replayed request must not dispatch")
	}
}

func TestHandlerBotMessageIsMarkedFromBridge(t *testing.T) {
	cases := []struct {
		name   string
		fields string
	}{
		{
			name: "bot_id",
			fields: `"type":"message","channel":"C01ABCDEF","bot_id":"B999",` +
				`"text":"echo","ts":"1700000000.000200"`,
		},
		{
			name: "bot_message subtype",
			fields: `"type":"message","subtype":"bot_message","channel":"C01ABCDEF",` +
				`"text":"echo","ts":"1700000000.000300"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got collector
			h := NewHandler(testSigningSecret, webhookConfig(), nopDirectory{}, got.dispatch, testLogger())

			body := messageEvent(tc.fields)
			serve(t, h, signedRequest(t, body, time.Now()))

			msgs := got.all()
			if len(msgs) != 1 {
				t.Fatalf("dispatched %d messages, want 1", len(msgs))
			}
			if !msgs[0].FromBridge {
				t.Fatal("FromBridge = false, want true for a bot message")
			}
		})
	}
}

func TestHandlerSubtypedMessageIsDropped(t *testing.T) {
	var got collector
	h := NewHandler(testSigningSecret, webhookConfig(), nopDirectory{}, got.dispatch, testLogger())

	body := messageEvent(`"type":"message","subtype":"channel_join","channel":"C01ABCDEF",` +
		`"user":"U123","text":"joined","ts":"1700000000.000400"`)
	serve(t, h, signedRequest(t, body, time.Now()))

	if n := len(got.all()); n != 0 {
		t.Fatalf("dispatched %d messages, want 0 for a subtyped event", n)
	}
}

func TestHandlerUnconfiguredChannelIsDropped(t *testing.T) {
	var got collector
	h := NewHandler(testSigningSecret, webhookConfig(), nopDirectory{}, got.dispatch, testLogger())

	body := messageEvent(`"type":"message","channel":"CUNKNOWN","user":"U123",` +
		`"text":"hello","ts":"1700000000.000500"`)
	serve(t, h, signedRequest(t, body, time.Now()))

	if n := len(got.all()); n != 0 {
		t.Fatalf("dispatched %d messages, want 0 for an unconfigured channel", n)
	}
}

func TestHandlerNonMessageEventIsDropped(t *testing.T) {
	var got collector
	h := NewHandler(testSigningSecret, webhookConfig(), nopDirectory{}, got.dispatch, testLogger())

	body := messageEvent(`"type":"reaction_added","channel":"C01ABCDEF","user":"U123",` +
		`"ts":"1700000000.000600"`)
	serve(t, h, signedRequest(t, body, time.Now()))

	if n := len(got.all()); n != 0 {
		t.Fatalf("dispatched %d messages, want 0 for a non-message event", n)
	}
}

func TestHandlerThreadTS(t *testing.T) {
	cases := []struct {
		name     string
		fields   string
		wantPare string
	}{
		{
			name: "thread root repeats its own ts",
			fields: `"type":"message","channel":"C01ABCDEF","user":"U123","text":"root",` +
				`"ts":"1700000000.000700","thread_ts":"1700000000.000700"`,
			wantPare: "",
		},
		{
			name: "reply carries the parent ts",
			fields: `"type":"message","channel":"C01ABCDEF","user":"U123","text":"reply",` +
				`"ts":"1700000000.000800","thread_ts":"1700000000.000700"`,
			wantPare: "1700000000.000700",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got collector
			h := NewHandler(testSigningSecret, webhookConfig(), nopDirectory{}, got.dispatch, testLogger())

			serve(t, h, signedRequest(t, messageEvent(tc.fields), time.Now()))

			msgs := got.all()
			if len(msgs) != 1 {
				t.Fatalf("dispatched %d messages, want 1", len(msgs))
			}
			if msgs[0].ParentID != tc.wantPare {
				t.Fatalf("ParentID = %q, want %q", msgs[0].ParentID, tc.wantPare)
			}
		})
	}
}

func TestHandlerDrainWaitsForInFlightWork(t *testing.T) {
	release := make(chan struct{})
	done := make(chan struct{})
	dispatch := func(_ context.Context, _ model.Message) error {
		<-release
		close(done)
		return nil
	}
	h := NewHandler(testSigningSecret, webhookConfig(), nopDirectory{}, dispatch, testLogger())

	body := messageEvent(`"type":"message","channel":"C01ABCDEF","user":"U123",` +
		`"text":"hello","ts":"1700000000.000900"`)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, signedRequest(t, body, time.Now()))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	drained := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		drained <- h.Drain(ctx)
	}()

	select {
	case err := <-drained:
		t.Fatalf("Drain returned %v before the work finished", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	if err := <-drained; err != nil {
		t.Fatalf("Drain: %v", err)
	}
	select {
	case <-done:
	default:
		t.Fatal("Drain returned before dispatch completed")
	}
}

func TestHandlerSaturationAsksSlackToRetry(t *testing.T) {
	release := make(chan struct{})
	dispatch := func(_ context.Context, _ model.Message) error { <-release; return nil }
	h := NewHandler(testSigningSecret, webhookConfig(), nopDirectory{}, dispatch, testLogger())

	body := messageEvent(`"type":"message","channel":"C01ABCDEF","user":"U123",` +
		`"text":"hello","ts":"1700000000.001000"`)
	for i := 0; i < maxConcurrentProcessing; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, signedRequest(t, body, time.Now()))
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200", i, rec.Code)
		}
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, signedRequest(t, body, time.Now()))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("saturated status = %d, want 503", rec.Code)
	}

	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := h.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
}

// stubDirectory resolves a fixed set of names and counts lookups.
type stubDirectory struct {
	mu      sync.Mutex
	names   map[string]string
	lookups []string
}

func (s *stubDirectory) DisplayName(_ context.Context, userID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lookups = append(s.lookups, userID)
	return s.names[userID]
}

func (s *stubDirectory) calls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.lookups))
	copy(out, s.lookups)
	return out
}

// A Slack author must reach Teams by name. Without this the Teams sink falls
// back for every message and the whole workspace shows up as "Slack".
func TestHandlerResolvesAuthorDisplayName(t *testing.T) {
	var got collector
	dir := &stubDirectory{names: map[string]string{"U123": "Ada Lovelace"}}
	h := NewHandler(testSigningSecret, webhookConfig(), dir, got.dispatch, testLogger())

	body := messageEvent(`"type":"message","channel":"C01ABCDEF","user":"U123",` +
		`"text":"hello","ts":"1700000000.001100"`)
	serve(t, h, signedRequest(t, body, time.Now()))

	msgs := got.all()
	if len(msgs) != 1 {
		t.Fatalf("dispatched %d messages, want 1", len(msgs))
	}
	if msgs[0].Author.DisplayName != "Ada Lovelace" {
		t.Fatalf("Author.DisplayName = %q, want %q", msgs[0].Author.DisplayName, "Ada Lovelace")
	}
	if msgs[0].Author.ExternalID != "U123" {
		t.Fatalf("Author.ExternalID = %q, want U123", msgs[0].Author.ExternalID)
	}
}

// An unresolvable author must still be delivered: a missing name is worth a
// fallback attribution, never a dropped message.
func TestHandlerDeliversWhenTheNameCannotBeResolved(t *testing.T) {
	var got collector
	dir := &stubDirectory{names: map[string]string{}}
	h := NewHandler(testSigningSecret, webhookConfig(), dir, got.dispatch, testLogger())

	body := messageEvent(`"type":"message","channel":"C01ABCDEF","user":"U404",` +
		`"text":"hello","ts":"1700000000.001200"`)
	serve(t, h, signedRequest(t, body, time.Now()))

	msgs := got.all()
	if len(msgs) != 1 {
		t.Fatalf("dispatched %d messages, want 1", len(msgs))
	}
	if msgs[0].Author.DisplayName != "" {
		t.Fatalf("Author.DisplayName = %q, want empty", msgs[0].Author.DisplayName)
	}
}

// The name lookup must happen after the acknowledgement, not before it: Slack
// retries anything unanswered within three seconds, and users.info is a
// network call.
func TestHandlerAcknowledgesBeforeResolvingTheName(t *testing.T) {
	acknowledged := make(chan struct{})
	blocking := &blockingDirectory{entered: make(chan struct{}), release: make(chan struct{})}
	h := NewHandler(testSigningSecret, webhookConfig(), blocking,
		func(context.Context, model.Message) error { return nil }, testLogger())

	body := messageEvent(`"type":"message","channel":"C01ABCDEF","user":"U123",` +
		`"text":"hello","ts":"1700000000.001300"`)
	rec := httptest.NewRecorder()
	go func() {
		h.ServeHTTP(rec, signedRequest(t, body, time.Now()))
		close(acknowledged)
	}()

	<-blocking.entered
	select {
	case <-acknowledged:
	case <-time.After(2 * time.Second):
		t.Fatal("ServeHTTP had not returned while the name lookup was still running")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	close(blocking.release)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := h.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
}

// blockingDirectory parks inside DisplayName until released.
type blockingDirectory struct {
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (b *blockingDirectory) DisplayName(_ context.Context, _ string) string {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	return ""
}

// A bot post carries no user id. Looking one up would spend an API call to
// learn nothing about a message the bridge is about to drop anyway.
func TestHandlerSkipsTheNameLookupForBotPosts(t *testing.T) {
	var got collector
	dir := &stubDirectory{names: map[string]string{}}
	h := NewHandler(testSigningSecret, webhookConfig(), dir, got.dispatch, testLogger())

	body := messageEvent(`"type":"message","channel":"C01ABCDEF","bot_id":"B999",` +
		`"text":"echo","ts":"1700000000.001400"`)
	serve(t, h, signedRequest(t, body, time.Now()))

	if calls := dir.calls(); len(calls) != 0 {
		t.Fatalf("directory was called %v, want no lookup for a bot post", calls)
	}
}

// A message in an unconfigured channel is not ours to mirror; resolving its
// author would leak API calls on traffic the bridge ignores.
func TestHandlerSkipsTheNameLookupForUnconfiguredChannels(t *testing.T) {
	var got collector
	dir := &stubDirectory{names: map[string]string{"U123": "Ada Lovelace"}}
	h := NewHandler(testSigningSecret, webhookConfig(), dir, got.dispatch, testLogger())

	body := messageEvent(`"type":"message","channel":"CUNKNOWN","user":"U123",` +
		`"text":"hello","ts":"1700000000.001500"`)
	serve(t, h, signedRequest(t, body, time.Now()))

	if calls := dir.calls(); len(calls) != 0 {
		t.Fatalf("directory was called %v, want no lookup for an unconfigured channel", calls)
	}
}

// nopDirectory resolves nothing, for the tests that are not about names.
type nopDirectory struct{}

func (nopDirectory) DisplayName(context.Context, string) string { return "" }

// A file share carries the prose the person typed alongside the file. The
// file itself is Phase 3, but discarding the accompanying text is a silent
// loss of conversation, not a deferral.
func TestHandlerFileShareKeepsItsText(t *testing.T) {
	var got collector
	dir := &stubDirectory{names: map[string]string{"U123": "Ada Lovelace"}}
	h := NewHandler(testSigningSecret, webhookConfig(), dir, got.dispatch, testLogger())

	body := messageEvent(`"type":"message","subtype":"file_share","channel":"C01ABCDEF",` +
		`"user":"U123","text":"here is the report","ts":"1700000000.001600"`)
	serve(t, h, signedRequest(t, body, time.Now()))

	msgs := got.all()
	if len(msgs) != 1 {
		t.Fatalf("dispatched %d messages, want 1", len(msgs))
	}
	if msgs[0].Text != "here is the report" {
		t.Fatalf("Text = %q, want the prose that accompanied the file", msgs[0].Text)
	}
	if msgs[0].Author.DisplayName != "Ada Lovelace" {
		t.Fatalf("Author.DisplayName = %q, want it resolved like any other message", msgs[0].Author.DisplayName)
	}
}

// A file with no caption is still a file: what a reader needs is the
// document, not the sentence that did not accompany it.
func TestHandlerFileShareWithoutTextIsMirrored(t *testing.T) {
	var got collector
	h := NewHandler(testSigningSecret, webhookConfig(), nopDirectory{}, got.dispatch, testLogger())
	h.OnDownload(func(context.Context, string) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("pdf bytes")), nil
	})

	body := messageEvent(`"type":"message","subtype":"file_share","channel":"C01ABCDEF",` +
		`"user":"U123","text":"","ts":"1700000000.001700",` +
		`"files":[{"id":"F1","name":"q3.pdf","mimetype":"application/pdf",` +
		`"size":2048,"url_private":"https://files.slack.com/q3.pdf"}]`)
	serve(t, h, signedRequest(t, body, time.Now()))

	msgs := got.all()
	if len(msgs) != 1 {
		t.Fatalf("dispatched %d messages, want 1", len(msgs))
	}
	if len(msgs[0].Attachments) != 1 {
		t.Fatalf("got %d attachments, want 1", len(msgs[0].Attachments))
	}
	a := msgs[0].Attachments[0]
	if a.Name != "q3.pdf" || a.SizeBytes != 2048 {
		t.Fatalf("attachment = %+v", a)
	}
	// url_private answers with a login page unless the request carries the
	// bot token, so the far side is never handed the URL — only the bytes.
	body2, err := a.Open(context.Background())
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	defer func() { _ = body2.Close() }()
	content, _ := io.ReadAll(body2)
	if string(content) != "pdf bytes" {
		t.Fatalf("content = %q", content)
	}
}

// Neither text nor files is nothing at all: mirroring it would post a body
// consisting of the author's name.
func TestHandlerEmptyFileShareIsDropped(t *testing.T) {
	var got collector
	h := NewHandler(testSigningSecret, webhookConfig(), nopDirectory{}, got.dispatch, testLogger())

	body := messageEvent(`"type":"message","subtype":"file_share","channel":"C01ABCDEF",` +
		`"user":"U123","text":"","ts":"1700000000.001700"`)
	serve(t, h, signedRequest(t, body, time.Now()))

	if n := len(got.all()); n != 0 {
		t.Fatalf("dispatched %d messages, want 0", n)
	}
}

// Slack names a reaction and Signal sends the character. Translating at the
// boundary is also what makes loop prevention work: the record of a mirrored
// reaction is keyed on the emoji, and a shortcode would never match the
// character it stands for — so the bridge would mirror its own reaction back.
func TestHandlerTranslatesReactionNames(t *testing.T) {
	cases := []struct {
		name      string
		reaction  string
		wantEmoji string
		wantAny   bool
	}{
		{name: "thumbsup", reaction: "thumbsup", wantEmoji: "👍", wantAny: true},
		{name: "plus one is the same character", reaction: "+1", wantEmoji: "👍", wantAny: true},
		{name: "skin tone falls back to the base", reaction: "wave::skin-tone-3", wantEmoji: "👋", wantAny: true},
		// A custom emoji is uploaded to one workspace and has no character at
		// all, so it stops here rather than travelling as a name.
		{name: "custom emoji is dropped", reaction: "aenix-logo", wantAny: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got []model.Reaction
			h := NewHandler(testSigningSecret, webhookConfig(), nopDirectory{}, nil, testLogger())
			h.OnReaction(func(_ context.Context, r model.Reaction) error {
				got = append(got, r)
				return nil
			})

			body := messageEvent(`"type":"reaction_added","user":"U123",` +
				`"reaction":"` + tc.reaction + `",` +
				`"item":{"type":"message","channel":"C01ABCDEF","ts":"1700000000.000100"}`)
			serve(t, h, signedRequest(t, body, time.Now()))

			if !tc.wantAny {
				if len(got) != 0 {
					t.Fatalf("dispatched %+v, want nothing", got)
				}
				return
			}
			if len(got) != 1 {
				t.Fatalf("dispatched %d reactions, want 1", len(got))
			}
			if got[0].Emoji != tc.wantEmoji {
				t.Fatalf("Emoji = %q, want %q — the core never sees a shortcode",
					got[0].Emoji, tc.wantEmoji)
			}
			if got[0].TargetID != "1700000000.000100" {
				t.Fatalf("TargetID = %q", got[0].TargetID)
			}
		})
	}
}

func TestHandlerReactionRemoval(t *testing.T) {
	var got []model.Reaction
	h := NewHandler(testSigningSecret, webhookConfig(), nopDirectory{}, nil, testLogger())
	h.OnReaction(func(_ context.Context, r model.Reaction) error {
		got = append(got, r)
		return nil
	})

	body := messageEvent(`"type":"reaction_removed","user":"U123","reaction":"tada",` +
		`"item":{"type":"message","channel":"C01ABCDEF","ts":"1700000000.000100"}`)
	serve(t, h, signedRequest(t, body, time.Now()))

	if len(got) != 1 || !got[0].Remove {
		t.Fatalf("reactions = %+v, want one withdrawal", got)
	}
}

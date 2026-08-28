package teams

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aenix-io/slack-connectors/internal/config"
	"github.com/aenix-io/slack-connectors/internal/store"
)

// fakeSubscriptionStore is an in-memory SubscriptionStore.
type fakeSubscriptionStore struct {
	mu    sync.Mutex
	saved map[string]store.Subscription
}

func newFakeSubscriptionStore() *fakeSubscriptionStore {
	return &fakeSubscriptionStore{saved: make(map[string]store.Subscription)}
}

func (f *fakeSubscriptionStore) SaveSubscription(_ context.Context, pairID string, sub store.Subscription) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saved[pairID] = sub
	return nil
}

func (f *fakeSubscriptionStore) LoadSubscription(_ context.Context, pairID string) (store.Subscription, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	sub, ok := f.saved[pairID]
	if !ok {
		return store.Subscription{}, store.ErrNotFound
	}
	return sub, nil
}

func (f *fakeSubscriptionStore) DeleteSubscription(_ context.Context, pairID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.saved, pairID)
	return nil
}

// set seeds a stored subscription directly, bypassing SaveSubscription, so
// tests can control the exact state a run starts from.
func (f *fakeSubscriptionStore) set(pairID string, sub store.Subscription) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saved[pairID] = sub
}

func (f *fakeSubscriptionStore) get(pairID string) (store.Subscription, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	sub, ok := f.saved[pairID]
	return sub, ok
}

// fakeSubscriptionMetrics is an in-memory SubscriptionMetrics.
type fakeSubscriptionMetrics struct {
	mu       sync.Mutex
	failures map[string]int
	expiries map[string]time.Time
}

func newFakeSubscriptionMetrics() *fakeSubscriptionMetrics {
	return &fakeSubscriptionMetrics{failures: make(map[string]int), expiries: make(map[string]time.Time)}
}

func (f *fakeSubscriptionMetrics) RecordRenewalFailure(pair string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failures[pair]++
}

func (f *fakeSubscriptionMetrics) SetExpiry(pair string, expiresAt time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.expiries[pair] = expiresAt
}

func (f *fakeSubscriptionMetrics) failureCount(pair string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.failures[pair]
}

func TestCreateSubscription(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("POST /subscriptions", func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{
			"id": "sub-1",
			"expirationDateTime": "2026-07-24T10:00:00Z",
			"resource": "/teams/t/channels/c/messages"
		}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := NewGraphClient(srv.URL, srv.Client())
	sub, err := client.CreateSubscription(context.Background(), SubscriptionRequest{
		Resource:                "/teams/t/channels/c/messages",
		ChangeType:              "created,updated,deleted",
		NotificationURL:         "https://bridge.example.com/webhook/graph",
		ClientState:             "secret",
		IncludeResourceData:     true,
		EncryptionCertificate:   "BASE64CERT",
		EncryptionCertificateID: "bridge-key-1",
		ExpirationDateTime:      time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("CreateSubscription(): %v", err)
	}
	if sub.ID != "sub-1" {
		t.Fatalf("ID = %q, want sub-1", sub.ID)
	}
	if gotBody["includeResourceData"] != true {
		t.Fatal("includeResourceData must be true — without it no message content arrives")
	}
	if gotBody["encryptionCertificate"] != "BASE64CERT" {
		t.Fatalf("encryptionCertificate = %v", gotBody["encryptionCertificate"])
	}
	if gotBody["clientState"] != "secret" {
		t.Fatalf("clientState = %v", gotBody["clientState"])
	}
}

func TestCreateSubscriptionSurfacesAPIErrors(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /subscriptions", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"error":{"code":"Forbidden","message":"missing consent"}}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := NewGraphClient(srv.URL, srv.Client())
	_, err := client.CreateSubscription(context.Background(), SubscriptionRequest{})
	if err == nil {
		t.Fatal("CreateSubscription() error = nil, want an error")
	}
	if !strings.Contains(err.Error(), "missing consent") {
		t.Fatalf("error = %q, want it to carry the Graph message", err)
	}
}

func TestRenewSubscription(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("PATCH /subscriptions/sub-1", func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"sub-1","expirationDateTime":"2026-07-24T11:00:00Z"}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := NewGraphClient(srv.URL, srv.Client())
	sub, err := client.RenewSubscription(context.Background(), "sub-1",
		time.Date(2026, 7, 24, 11, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("RenewSubscription(): %v", err)
	}
	if sub.ExpirationDateTime.IsZero() {
		t.Fatal("ExpirationDateTime is zero, want the renewed expiry")
	}
	if gotBody["expirationDateTime"] == nil {
		t.Fatal("PATCH body must carry expirationDateTime")
	}
}

func TestManagerCreatesOneSubscriptionPerPair(t *testing.T) {
	var mu sync.Mutex
	var created []string
	var changeTypes []string

	mux := http.NewServeMux()
	mux.HandleFunc("POST /subscriptions", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode request: %v", err)
		}
		resource, ok := body["resource"].(string)
		if !ok {
			t.Errorf("request body carried no resource string: %v", body["resource"])
		}
		changeType, _ := body["changeType"].(string)
		mu.Lock()
		created = append(created, resource)
		changeTypes = append(changeTypes, changeType)
		id := len(created)
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = fmt.Fprintf(w, `{"id":"sub-%d","expirationDateTime":"2026-07-24T10:00:00Z"}`, id)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cfg := config.Config{Pairs: []config.Pair{
		{ID: "a", Teams: &config.TeamsEndpoint{TeamID: "t1", ChannelID: "c1"}, Slack: &config.SlackEndpoint{ChannelID: "s1"}},
		{ID: "b", Teams: &config.TeamsEndpoint{TeamID: "t2", ChannelID: "c2"}, Slack: &config.SlackEndpoint{ChannelID: "s2"}},
	}}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewSubscriptionManager(NewGraphClient(srv.URL, srv.Client()), cfg,
		newFakeSubscriptionStore(), newFakeSubscriptionMetrics(), ManagerOptions{
			NotificationURL:         "https://bridge.example.com/webhook/graph",
			ClientState:             "secret",
			EncryptionCertificate:   "BASE64CERT",
			EncryptionCertificateID: "bridge-key-1",
			RenewInterval:           time.Hour, // long enough that no renewal fires
		}, log)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = mgr.Run(ctx)

	mu.Lock()
	defer mu.Unlock()
	if len(created) != 2 {
		t.Fatalf("created %d subscriptions, want 2", len(created))
	}
	want := map[string]bool{
		"/teams/t1/channels/c1/messages": true,
		"/teams/t2/channels/c2/messages": true,
	}
	for _, res := range created {
		if !want[res] {
			t.Fatalf("unexpected resource %q", res)
		}
	}
	for _, ct := range changeTypes {
		if ct != "created" {
			t.Fatalf("changeType = %q, want exactly %q — Phase 1 is created-only, and Graph also fires "+
				"\"updated\" for reactions and \"deleted\" with an empty body Slack rejects", ct, "created")
		}
	}
}

func TestManagerRenewsBeforeExpiry(t *testing.T) {
	var mu sync.Mutex
	renewals := 0

	mux := http.NewServeMux()
	mux.HandleFunc("POST /subscriptions", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"id":"sub-1","expirationDateTime":"2026-07-24T10:00:00Z"}`)
	})
	mux.HandleFunc("PATCH /subscriptions/sub-1", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		renewals++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"sub-1","expirationDateTime":"2026-07-24T11:00:00Z"}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cfg := config.Config{Pairs: []config.Pair{
		{ID: "a", Teams: &config.TeamsEndpoint{TeamID: "t1", ChannelID: "c1"}, Slack: &config.SlackEndpoint{ChannelID: "s1"}},
	}}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewSubscriptionManager(NewGraphClient(srv.URL, srv.Client()), cfg,
		newFakeSubscriptionStore(), newFakeSubscriptionMetrics(), ManagerOptions{
			NotificationURL:         "https://bridge.example.com/webhook/graph",
			ClientState:             "secret",
			EncryptionCertificate:   "BASE64CERT",
			EncryptionCertificateID: "bridge-key-1",
			RenewInterval:           50 * time.Millisecond,
		}, log)

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	_ = mgr.Run(ctx)

	mu.Lock()
	defer mu.Unlock()
	if renewals < 2 {
		t.Fatalf("renewals = %d, want at least 2 — a stalled renewal loop makes the bridge go deaf", renewals)
	}
}

func TestDeleteSubscription(t *testing.T) {
	var gotMethod string
	mux := http.NewServeMux()
	mux.HandleFunc("/subscriptions/sub-1", func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		w.WriteHeader(http.StatusNoContent)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := NewGraphClient(srv.URL, srv.Client())
	if err := client.DeleteSubscription(context.Background(), "sub-1"); err != nil {
		t.Fatalf("DeleteSubscription(): %v", err)
	}
	if gotMethod != http.MethodDelete {
		t.Fatalf("method = %q, want DELETE", gotMethod)
	}
}

func TestDeleteSubscriptionTreatsMissingAsSuccess(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/subscriptions/gone", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := NewGraphClient(srv.URL, srv.Client())
	if err := client.DeleteSubscription(context.Background(), "gone"); err != nil {
		t.Fatalf("DeleteSubscription(404) error = %v, want nil — 404 means it already expired", err)
	}
}

func TestDeleteSubscriptionSurfacesOtherErrors(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/subscriptions/sub-1", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `{"error":{"code":"InternalServerError","message":"try again"}}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := NewGraphClient(srv.URL, srv.Client())
	err := client.DeleteSubscription(context.Background(), "sub-1")
	if err == nil {
		t.Fatal("DeleteSubscription() error = nil, want an error")
	}
	if !strings.Contains(err.Error(), "try again") {
		t.Fatalf("error = %q, want it to carry the Graph message", err)
	}
}

func TestManagerDeletesPreviousSubscription(t *testing.T) {
	var mu sync.Mutex
	var events []string

	mux := http.NewServeMux()
	mux.HandleFunc("DELETE /subscriptions/old-1", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		events = append(events, "delete")
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /subscriptions", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		events = append(events, "create")
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"id":"sub-new","expirationDateTime":"2026-07-24T10:00:00Z"}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cfg := config.Config{Pairs: []config.Pair{
		{ID: "a", Teams: &config.TeamsEndpoint{TeamID: "t1", ChannelID: "c1"}, Slack: &config.SlackEndpoint{ChannelID: "s1"}},
	}}
	subs := newFakeSubscriptionStore()
	subs.set("a", store.Subscription{ID: "old-1", ExpiresAt: time.Now()})
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewSubscriptionManager(NewGraphClient(srv.URL, srv.Client()), cfg,
		subs, newFakeSubscriptionMetrics(), ManagerOptions{
			NotificationURL: "https://bridge.example.com/webhook/graph",
			ClientState:     "secret",
		}, log)

	mgr.createAll(context.Background())

	mu.Lock()
	defer mu.Unlock()
	if len(events) != 2 || events[0] != "delete" || events[1] != "create" {
		t.Fatalf("events = %v, want [delete create] — the stale subscription must be deleted "+
			"before the new one is created", events)
	}
}

func TestManagerTreatsMissingPreviousSubscriptionAsDeleted(t *testing.T) {
	var mu sync.Mutex
	created := false

	mux := http.NewServeMux()
	mux.HandleFunc("DELETE /subscriptions/old-1", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	mux.HandleFunc("POST /subscriptions", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		created = true
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"id":"sub-new","expirationDateTime":"2026-07-24T10:00:00Z"}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cfg := config.Config{Pairs: []config.Pair{
		{ID: "a", Teams: &config.TeamsEndpoint{TeamID: "t1", ChannelID: "c1"}, Slack: &config.SlackEndpoint{ChannelID: "s1"}},
	}}
	subs := newFakeSubscriptionStore()
	subs.set("a", store.Subscription{ID: "old-1", ExpiresAt: time.Now()})
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewSubscriptionManager(NewGraphClient(srv.URL, srv.Client()), cfg,
		subs, newFakeSubscriptionMetrics(), ManagerOptions{
			NotificationURL: "https://bridge.example.com/webhook/graph",
			ClientState:     "secret",
		}, log)

	mgr.createAll(context.Background())

	mu.Lock()
	defer mu.Unlock()
	if !created {
		t.Fatal("new subscription was not created after a 404 deleting the stale one; " +
			"a 404 means it already expired and startup must proceed")
	}
}

func TestManagerContinuesWhenStaleDeleteFails(t *testing.T) {
	var mu sync.Mutex
	created := false

	mux := http.NewServeMux()
	mux.HandleFunc("DELETE /subscriptions/old-1", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	mux.HandleFunc("POST /subscriptions", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		created = true
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"id":"sub-new","expirationDateTime":"2026-07-24T10:00:00Z"}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cfg := config.Config{Pairs: []config.Pair{
		{ID: "a", Teams: &config.TeamsEndpoint{TeamID: "t1", ChannelID: "c1"}, Slack: &config.SlackEndpoint{ChannelID: "s1"}},
	}}
	subs := newFakeSubscriptionStore()
	subs.set("a", store.Subscription{ID: "old-1", ExpiresAt: time.Now()})
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewSubscriptionManager(NewGraphClient(srv.URL, srv.Client()), cfg,
		subs, newFakeSubscriptionMetrics(), ManagerOptions{
			NotificationURL: "https://bridge.example.com/webhook/graph",
			ClientState:     "secret",
		}, log)

	mgr.createAll(context.Background())

	mu.Lock()
	defer mu.Unlock()
	if !created {
		t.Fatal("new subscription was not created after Graph failed to delete the stale one; " +
			"going deaf is worse than a short window of double delivery")
	}
}

// TestManagerKeepsStoredSubscriptionWhenDeleteAndCreateBothFail covers the
// case a passing "create still succeeds" assertion cannot: if the stored
// record for the stale subscription is deleted unconditionally, and the
// subsequent create ALSO fails, the live orphaned subscription's id is gone
// from the store with nothing left pointing at it — no later restart can find
// or remove it, and it keeps delivering to the same URL for up to an hour.
func TestManagerKeepsStoredSubscriptionWhenDeleteAndCreateBothFail(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("DELETE /subscriptions/old-1", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	mux.HandleFunc("POST /subscriptions", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `{"error":{"code":"InternalServerError","message":"try again"}}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cfg := config.Config{Pairs: []config.Pair{
		{ID: "a", Teams: &config.TeamsEndpoint{TeamID: "t1", ChannelID: "c1"}, Slack: &config.SlackEndpoint{ChannelID: "s1"}},
	}}
	subs := newFakeSubscriptionStore()
	subs.set("a", store.Subscription{ID: "old-1", ExpiresAt: time.Now()})
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewSubscriptionManager(NewGraphClient(srv.URL, srv.Client()), cfg,
		subs, newFakeSubscriptionMetrics(), ManagerOptions{
			NotificationURL: "https://bridge.example.com/webhook/graph",
			ClientState:     "secret",
		}, log)

	mgr.createAll(context.Background())

	got, ok := subs.get("a")
	if !ok {
		t.Fatal("stored subscription record for pair a was deleted despite the Graph delete failing " +
			"and the replacement create also failing; the orphaned live subscription id is now unrecoverable")
	}
	if got.ID != "old-1" {
		t.Fatalf("stored subscription id = %q, want old-1 to be preserved", got.ID)
	}
}

func TestManagerContinuesAfterPairCreateFailure(t *testing.T) {
	var mu sync.Mutex
	var createdResources []string

	mux := http.NewServeMux()
	mux.HandleFunc("POST /subscriptions", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode request: %v", err)
		}
		resource, _ := body["resource"].(string)

		if resource == "/teams/t1/channels/c1/messages" {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, `{"error":{"code":"InternalServerError","message":"pair a is unavailable"}}`)
			return
		}

		mu.Lock()
		createdResources = append(createdResources, resource)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"id":"sub-b","expirationDateTime":"2026-07-24T10:00:00Z"}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cfg := config.Config{Pairs: []config.Pair{
		{ID: "a", Teams: &config.TeamsEndpoint{TeamID: "t1", ChannelID: "c1"}, Slack: &config.SlackEndpoint{ChannelID: "s1"}},
		{ID: "b", Teams: &config.TeamsEndpoint{TeamID: "t2", ChannelID: "c2"}, Slack: &config.SlackEndpoint{ChannelID: "s2"}},
	}}
	subs := newFakeSubscriptionStore()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewSubscriptionManager(NewGraphClient(srv.URL, srv.Client()), cfg,
		subs, newFakeSubscriptionMetrics(), ManagerOptions{
			NotificationURL: "https://bridge.example.com/webhook/graph",
			ClientState:     "secret",
		}, log)

	mgr.createAll(context.Background())

	mu.Lock()
	defer mu.Unlock()
	if len(createdResources) != 1 || createdResources[0] != "/teams/t2/channels/c2/messages" {
		t.Fatalf("createdResources = %v, want exactly pair b's resource — "+
			"a failure creating pair a's subscription must not stop pair b from getting one", createdResources)
	}
	if _, ok := subs.get("a"); ok {
		t.Fatal("pair a has a persisted subscription, want none — its create call failed")
	}
	got, ok := subs.get("b")
	if !ok {
		t.Fatal("pair b has no persisted subscription, want one")
	}
	if got.ID != "sub-b" {
		t.Fatalf("pair b subscription ID = %q, want sub-b", got.ID)
	}
}

func TestManagerPersistsSubscription(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /subscriptions", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"id":"sub-new","expirationDateTime":"2026-07-24T10:00:00Z"}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cfg := config.Config{Pairs: []config.Pair{
		{ID: "a", Teams: &config.TeamsEndpoint{TeamID: "t1", ChannelID: "c1"}, Slack: &config.SlackEndpoint{ChannelID: "s1"}},
	}}
	subs := newFakeSubscriptionStore()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewSubscriptionManager(NewGraphClient(srv.URL, srv.Client()), cfg,
		subs, newFakeSubscriptionMetrics(), ManagerOptions{
			NotificationURL: "https://bridge.example.com/webhook/graph",
			ClientState:     "secret",
		}, log)

	mgr.createAll(context.Background())

	got, ok := subs.get("a")
	if !ok {
		t.Fatal("no subscription persisted for pair a")
	}
	if got.ID != "sub-new" {
		t.Fatalf("persisted ID = %q, want sub-new", got.ID)
	}
	want := time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC)
	if !got.ExpiresAt.Equal(want) {
		t.Fatalf("persisted ExpiresAt = %v, want %v", got.ExpiresAt, want)
	}
}

func TestManagerRecordsRenewalFailure(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /subscriptions", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"id":"sub-1","expirationDateTime":"2026-07-24T10:00:00Z"}`)
	})
	mux.HandleFunc("PATCH /subscriptions/sub-1", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cfg := config.Config{Pairs: []config.Pair{
		{ID: "a", Teams: &config.TeamsEndpoint{TeamID: "t1", ChannelID: "c1"}, Slack: &config.SlackEndpoint{ChannelID: "s1"}},
	}}
	metrics := newFakeSubscriptionMetrics()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewSubscriptionManager(NewGraphClient(srv.URL, srv.Client()), cfg,
		newFakeSubscriptionStore(), metrics, ManagerOptions{
			NotificationURL: "https://bridge.example.com/webhook/graph",
			ClientState:     "secret",
		}, log)

	mgr.createAll(context.Background())
	mgr.renewAll(context.Background())

	if got := metrics.failureCount("a"); got != 1 {
		t.Fatalf("failureCount(a) = %d, want 1", got)
	}
}

// A subscription that could not be created at startup was never retried: the
// pair was simply absent from the manager's active set, and the renewal ticker
// iterated an empty map forever. The bridge stayed deaf for its whole life
// with a Ready pod and two error lines at t=0 as the only evidence.
func TestManagerRetriesCreationAfterFailure(t *testing.T) {
	var mu sync.Mutex
	attempts := 0

	mux := http.NewServeMux()
	mux.HandleFunc("POST /subscriptions", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		attempts++
		first := attempts == 1
		mu.Unlock()
		if first {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `{"error":{"code":"ServiceUnavailable","message":"try later"}}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"id":"sub-1","expirationDateTime":"2026-07-24T11:00:00Z"}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cfg := config.Config{Pairs: []config.Pair{
		{ID: "a", Teams: &config.TeamsEndpoint{TeamID: "t1", ChannelID: "c1"}, Slack: &config.SlackEndpoint{ChannelID: "s1"}},
	}}
	subs := newFakeSubscriptionStore()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewSubscriptionManager(NewGraphClient(srv.URL, srv.Client()), cfg,
		subs, newFakeSubscriptionMetrics(), ManagerOptions{
			NotificationURL: "https://bridge.example.com/webhook/graph",
			RenewInterval:   30 * time.Millisecond,
		}, log)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_ = mgr.Run(ctx)

	mu.Lock()
	defer mu.Unlock()
	if attempts < 2 {
		t.Fatalf("creation attempts = %d, want at least 2 — a failed creation was never retried", attempts)
	}
	if got, err := subs.LoadSubscription(context.Background(), "a"); err != nil {
		t.Fatalf("no subscription was ever stored for the pair: %v", err)
	} else if got.ID != "sub-1" {
		t.Fatalf("stored subscription id = %q, want sub-1", got.ID)
	}
}

// A renewal that fails leaves the subscription heading for expiry. Retrying
// the same id is useless once Graph has dropped it, so the pair must fall back
// to creating a fresh subscription rather than patching a corpse forever.
func TestManagerRecreatesAfterRenewalFailure(t *testing.T) {
	var mu sync.Mutex
	creates := 0

	mux := http.NewServeMux()
	mux.HandleFunc("POST /subscriptions", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		creates++
		id := fmt.Sprintf("sub-%d", creates)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = fmt.Fprintf(w, `{"id":%q,"expirationDateTime":"2026-07-24T11:00:00Z"}`, id)
	})
	// Every renewal fails, as it would once Graph has forgotten the id.
	mux.HandleFunc("PATCH /subscriptions/", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"error":{"code":"NotFound","message":"gone"}}`)
	})
	mux.HandleFunc("DELETE /subscriptions/", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cfg := config.Config{Pairs: []config.Pair{
		{ID: "a", Teams: &config.TeamsEndpoint{TeamID: "t1", ChannelID: "c1"}, Slack: &config.SlackEndpoint{ChannelID: "s1"}},
	}}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewSubscriptionManager(NewGraphClient(srv.URL, srv.Client()), cfg,
		newFakeSubscriptionStore(), newFakeSubscriptionMetrics(), ManagerOptions{
			NotificationURL: "https://bridge.example.com/webhook/graph",
			RenewInterval:   30 * time.Millisecond,
		}, log)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_ = mgr.Run(ctx)

	mu.Lock()
	defer mu.Unlock()
	if creates < 2 {
		t.Fatalf("creations = %d, want at least 2 — a failed renewal must lead to a fresh subscription", creates)
	}
}

// The renewal margin has to leave room for a failure. Renewing once at 45
// minutes against a 60-minute lifetime means a single transient error is fatal:
// the next attempt lands after expiry and can only ever 404.
func TestRenewIntervalLeavesRoomForRetries(t *testing.T) {
	if DefaultRenewInterval >= SubscriptionLifetime {
		t.Fatalf("DefaultRenewInterval %v must be shorter than the %v lifetime",
			DefaultRenewInterval, SubscriptionLifetime)
	}
	attemptsBeforeExpiry := int(SubscriptionLifetime / DefaultRenewInterval)
	if attemptsBeforeExpiry < 3 {
		t.Fatalf("only %d renewal attempts fit before expiry (interval %v, lifetime %v); "+
			"a single transient failure would make the bridge go deaf",
			attemptsBeforeExpiry, DefaultRenewInterval, SubscriptionLifetime)
	}
}

// Readiness needs to know whether the Teams direction can actually receive
// anything. A pod with no live subscription hears nothing, and reporting Ready
// anyway is how the bridge used to go deaf without a single alert firing.
func TestManagerHealthy(t *testing.T) {
	cfg := config.Config{Pairs: []config.Pair{
		{ID: "a", Teams: &config.TeamsEndpoint{TeamID: "t1", ChannelID: "c1"}, Slack: &config.SlackEndpoint{ChannelID: "s1"}},
		{ID: "b", Teams: &config.TeamsEndpoint{TeamID: "t2", ChannelID: "c2"}, Slack: &config.SlackEndpoint{ChannelID: "s2"}},
	}}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := NewSubscriptionManager(NewGraphClient("http://unused", http.DefaultClient), cfg,
		newFakeSubscriptionStore(), newFakeSubscriptionMetrics(), ManagerOptions{}, log)

	err := mgr.Healthy()
	if err == nil {
		t.Fatal("Healthy() = nil before any subscription exists, want an error")
	}
	if !strings.Contains(err.Error(), "a") || !strings.Contains(err.Error(), "b") {
		t.Fatalf("Healthy() = %v, want it to name both pairs still missing one", err)
	}

	mgr.mu.Lock()
	mgr.active["a"] = "sub-a"
	mgr.mu.Unlock()

	err = mgr.Healthy()
	if err == nil {
		t.Fatal("Healthy() = nil with one pair still missing a subscription, want an error")
	}
	if strings.Contains(err.Error(), `"a"`) {
		t.Fatalf("Healthy() = %v, want it to name only the pair that is missing one", err)
	}

	mgr.mu.Lock()
	mgr.active["b"] = "sub-b"
	mgr.mu.Unlock()

	if err := mgr.Healthy(); err != nil {
		t.Fatalf("Healthy() = %v, want nil once every pair has a subscription", err)
	}
}

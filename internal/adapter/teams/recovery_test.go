package teams

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/aenix-io/slack-connectors/internal/config"
	"github.com/aenix-io/slack-connectors/internal/model"
	"github.com/aenix-io/slack-connectors/internal/store"
)

// fakeCursors is an in-memory CursorStore.
type fakeCursors struct {
	mu      sync.Mutex
	entries map[string]store.DeltaCursor
}

func newFakeCursors() *fakeCursors {
	return &fakeCursors{entries: make(map[string]store.DeltaCursor)}
}

func (f *fakeCursors) SaveDeltaLink(_ context.Context, pairID, link string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries[pairID] = store.DeltaCursor{Link: link, UpdatedAt: time.Now()}
	return nil
}

func (f *fakeCursors) LoadDeltaCursor(_ context.Context, pairID string) (store.DeltaCursor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cursor, ok := f.entries[pairID]
	if !ok {
		return store.DeltaCursor{}, store.ErrNotFound
	}
	return cursor, nil
}

// set seeds a stored watermark directly, bypassing SaveDeltaLink's clock, so
// tests control the exact UpdatedAt a sweep resumes from.
func (f *fakeCursors) set(pairID string, cursor store.DeltaCursor) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries[pairID] = cursor
}

func recoveryConfig() config.Config {
	return config.Config{Pairs: []config.Pair{{
		ID:    "acme",
		Teams: &config.TeamsEndpoint{TeamID: "t1", ChannelID: "c1"},
		Slack: &config.SlackEndpoint{ChannelID: "s1"},
	}}}
}

type recorder struct {
	mu   sync.Mutex
	msgs []model.Message
	// err is returned for every dispatch, so a test can make delivery fail.
	err error
}

func (r *recorder) dispatch(_ context.Context, m model.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.msgs = append(r.msgs, m)
	return r.err
}

func (r *recorder) seen() []model.Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]model.Message(nil), r.msgs...)
}

// stamp formats a time the way Graph does in message payloads.
func stamp(t time.Time) string { return t.UTC().Format(time.RFC3339) }

func TestRecoveryFirstRunListsChannel(t *testing.T) {
	var requested []string
	var mu sync.Mutex

	mux := http.NewServeMux()
	mux.HandleFunc("/teams/t1/channels/c1/messages", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requested = append(requested, r.URL.Path)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"value": []}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cursors := newFakeCursors()
	rec := &recorder{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := NewRecovery(NewGraphClient(srv.URL, srv.Client()), recoveryConfig(), cursors, rec.dispatch, log)

	if err := r.SweepOnce(context.Background()); err != nil {
		t.Fatalf("SweepOnce(): %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(requested) != 1 {
		t.Fatalf("made %d list requests, want 1", len(requested))
	}
}

// Without the since-cutoff the first sweep would dump the customer's entire
// channel history into Slack on first boot.
func TestRecoveryFirstRunOnlyTracksMessagesFromStartup(t *testing.T) {
	startup := time.Date(2026, 7, 24, 9, 0, 0, 0, time.UTC)

	mux := http.NewServeMux()
	mux.HandleFunc("/teams/t1/channels/c1/messages", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, fmt.Sprintf(`{"value":[
			{"id":"new-1","messageType":"message","createdDateTime":%q,
				"body":{"content":"fresh"},"from":{"user":{"id":"u1","displayName":"Ivan"}}},
			{"id":"old-1","messageType":"message","createdDateTime":%q,
				"body":{"content":"history"},"from":{"user":{"id":"u1","displayName":"Ivan"}}}
		]}`, stamp(startup.Add(10*time.Minute)), stamp(startup.Add(-10*time.Minute))))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cursors := newFakeCursors()
	rec := &recorder{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := NewRecovery(NewGraphClient(srv.URL, srv.Client()), recoveryConfig(), cursors, rec.dispatch, log)
	r.now = func() time.Time { return startup }

	if err := r.SweepOnce(context.Background()); err != nil {
		t.Fatalf("SweepOnce(): %v", err)
	}

	msgs := rec.seen()
	if len(msgs) != 1 || msgs[0].ID != "new-1" {
		t.Fatalf("dispatched %+v, want only the message created after startup", msgs)
	}
}

func TestRecoveryDispatchesMissedMessagesAndExpandedReplies(t *testing.T) {
	fresh := stamp(time.Now())
	mux := http.NewServeMux()
	mux.HandleFunc("/teams/t1/channels/c1/messages", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, fmt.Sprintf(`{"value":[
			{"id":"missed-1","messageType":"message","createdDateTime":%q,
				"body":{"content":"<p>one</p>"},
				"from":{"user":{"id":"u1","displayName":"Ivan Petrov"}},
				"replies":[
					{"id":"missed-2","replyToId":"missed-1","messageType":"message",
						"createdDateTime":%q,"body":{"content":"<p>two</p>"},
						"from":{"user":{"id":"u2","displayName":"Anna Ivanova"}}}
				]}
		]}`, fresh, fresh))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cursors := newFakeCursors()
	cursors.set("acme", store.DeltaCursor{Link: recoveryWatermark, UpdatedAt: time.Now().Add(-time.Hour)})
	rec := &recorder{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := NewRecovery(NewGraphClient(srv.URL, srv.Client()), recoveryConfig(), cursors, rec.dispatch, log)

	if err := r.SweepOnce(context.Background()); err != nil {
		t.Fatalf("SweepOnce(): %v", err)
	}

	msgs := rec.seen()
	if len(msgs) != 2 {
		t.Fatalf("dispatched %d messages, want root + expanded reply", len(msgs))
	}
	if msgs[0].ID != "missed-1" || msgs[0].PairID != "acme" {
		t.Fatalf("first message = %+v", msgs[0])
	}
	if msgs[1].ParentID != "missed-1" {
		t.Fatalf("reply ParentID = %q, want missed-1", msgs[1].ParentID)
	}
	if msgs[0].Kind != model.EventCreated {
		t.Fatalf("Kind = %q, want created", msgs[0].Kind)
	}
}

// An edit bumps lastModifiedDateTime while createdDateTime stays old; Graph
// surfaces the edited chain at the top of the listing, and the window must
// judge by the change, not the creation.
func TestRecoveryReplaysEditedChains(t *testing.T) {
	watermark := time.Date(2026, 7, 20, 8, 0, 0, 0, time.UTC)
	mux := http.NewServeMux()
	mux.HandleFunc("/teams/t1/channels/c1/messages", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, fmt.Sprintf(`{"value":[
			{"id":"edited","messageType":"message","createdDateTime":%q,
				"lastModifiedDateTime":%q,"body":{"content":"v2"},
				"from":{"user":{"id":"u1","displayName":"Ivan"}}},
			{"id":"untouched","messageType":"message","createdDateTime":%q,
				"body":{"content":"old"},"from":{"user":{"id":"u1","displayName":"Ivan"}}}
		]}`,
			stamp(watermark.Add(-2*time.Hour)), stamp(watermark.Add(30*time.Minute)),
			stamp(watermark.Add(-90*time.Minute))))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cursors := newFakeCursors()
	cursors.set("acme", store.DeltaCursor{Link: recoveryWatermark, UpdatedAt: watermark})
	rec := &recorder{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := NewRecovery(NewGraphClient(srv.URL, srv.Client()), recoveryConfig(), cursors, rec.dispatch, log)

	if err := r.SweepOnce(context.Background()); err != nil {
		t.Fatalf("SweepOnce(): %v", err)
	}

	msgs := rec.seen()
	if len(msgs) != 1 || msgs[0].ID != "edited" {
		t.Fatalf("dispatched %+v, want only the edited message", msgs)
	}
}

func TestRecoveryPersistsWatermark(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/teams/t1/channels/c1/messages", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"value":[]}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cursors := newFakeCursors()
	rec := &recorder{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := NewRecovery(NewGraphClient(srv.URL, srv.Client()), recoveryConfig(), cursors, rec.dispatch, log)

	if err := r.SweepOnce(context.Background()); err != nil {
		t.Fatalf("SweepOnce(): %v", err)
	}

	got, err := cursors.LoadDeltaCursor(context.Background(), "acme")
	if err != nil {
		t.Fatalf("LoadDeltaCursor(): %v", err)
	}
	if got.Link != recoveryWatermark {
		t.Fatalf("stored link = %q, want the watermark sentinel", got.Link)
	}
	if got.UpdatedAt.IsZero() {
		t.Fatal("watermark UpdatedAt is zero; the resume point lives there")
	}
}

func TestRecoveryFollowsPagination(t *testing.T) {
	// The nextLink Graph returns is absolute, so the handler needs the
	// server's own URL. httptest.NewUnstartedServer lets us capture it
	// before the first request arrives.
	fresh := stamp(time.Now())
	srv := httptest.NewUnstartedServer(nil)
	mux := http.NewServeMux()
	mux.HandleFunc("/teams/t1/channels/c1/messages", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("page") == "" {
			_, _ = io.WriteString(w, `{
				"value": [{"id":"p1","messageType":"message","createdDateTime":"`+fresh+`",
					"body":{"content":"a"},"from":{"user":{"id":"u1","displayName":"Ivan"}}}],
				"@odata.nextLink": "`+srv.URL+`/teams/t1/channels/c1/messages?page=2"
			}`)
			return
		}
		_, _ = io.WriteString(w, `{
			"value": [{"id":"p2","messageType":"message","createdDateTime":"`+fresh+`",
				"body":{"content":"b"},"from":{"user":{"id":"u1","displayName":"Ivan"}}}]
		}`)
	})
	srv.Config.Handler = mux
	srv.Start()
	t.Cleanup(srv.Close)

	cursors := newFakeCursors()
	cursors.set("acme", store.DeltaCursor{Link: recoveryWatermark, UpdatedAt: time.Now().Add(-time.Hour)})
	rec := &recorder{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := NewRecovery(NewGraphClient(srv.URL, srv.Client()), recoveryConfig(), cursors, rec.dispatch, log)

	if err := r.SweepOnce(context.Background()); err != nil {
		t.Fatalf("SweepOnce(): %v", err)
	}

	if got := len(rec.seen()); got != 2 {
		t.Fatalf("dispatched %d messages across pages, want 2", got)
	}
}

// Pagination must stop at the lookback cutoff instead of walking the whole
// channel history page by page.
func TestRecoveryStopsPagingAtTheCutoff(t *testing.T) {
	watermark := time.Date(2026, 7, 20, 8, 0, 0, 0, time.UTC)
	var pages int
	var mu sync.Mutex

	srv := httptest.NewUnstartedServer(nil)
	mux := http.NewServeMux()
	mux.HandleFunc("/teams/t1/channels/c1/messages", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		pages++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		// Every page is ancient and still advertises a next page; only the
		// cutoff can end this walk.
		_, _ = io.WriteString(w, fmt.Sprintf(`{
			"value": [{"id":"ancient","messageType":"message","createdDateTime":%q,
				"body":{"content":"x"},"from":{"user":{"id":"u1","displayName":"Ivan"}}}],
			"@odata.nextLink": %q
		}`, stamp(watermark.Add(-30*24*time.Hour)), srv.URL+"/teams/t1/channels/c1/messages?page=next"))
	})
	srv.Config.Handler = mux
	srv.Start()
	t.Cleanup(srv.Close)

	cursors := newFakeCursors()
	cursors.set("acme", store.DeltaCursor{Link: recoveryWatermark, UpdatedAt: watermark})
	rec := &recorder{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := NewRecovery(NewGraphClient(srv.URL, srv.Client()), recoveryConfig(), cursors, rec.dispatch, log)

	if err := r.SweepOnce(context.Background()); err != nil {
		t.Fatalf("SweepOnce(): %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if pages != 1 {
		t.Fatalf("fetched %d pages, want 1 — the first ancient root must end the walk", pages)
	}
	if got := len(rec.seen()); got != 0 {
		t.Fatalf("dispatched %d ancient messages, want 0", got)
	}
}

func TestRecoverySkipsSystemMessages(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/teams/t1/channels/c1/messages", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, fmt.Sprintf(`{
			"value": [{"id":"sys-1","messageType":"systemEventMessage",
				"createdDateTime":%q,"body":{"content":""}}]
		}`, stamp(time.Now())))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cursors := newFakeCursors()
	cursors.set("acme", store.DeltaCursor{Link: recoveryWatermark, UpdatedAt: time.Now().Add(-time.Hour)})
	rec := &recorder{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := NewRecovery(NewGraphClient(srv.URL, srv.Client()), recoveryConfig(), cursors, rec.dispatch, log)

	if err := r.SweepOnce(context.Background()); err != nil {
		t.Fatalf("SweepOnce(): %v", err)
	}
	if got := len(rec.seen()); got != 0 {
		t.Fatalf("dispatched %d system messages, want 0", got)
	}
}

func TestRecoveryContinuesAfterPairFailure(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/teams/t1/channels/c1/messages", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	mux.HandleFunc("/teams/t2/channels/c2/messages", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, fmt.Sprintf(`{
			"value": [{"id":"ok-1","messageType":"message","createdDateTime":%q,
				"body":{"content":"x"},"from":{"user":{"id":"u1","displayName":"Ivan"}}}]
		}`, stamp(time.Now())))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cfg := config.Config{Pairs: []config.Pair{
		{ID: "broken", Teams: &config.TeamsEndpoint{TeamID: "t1", ChannelID: "c1"}, Slack: &config.SlackEndpoint{ChannelID: "s1"}},
		{ID: "healthy", Teams: &config.TeamsEndpoint{TeamID: "t2", ChannelID: "c2"}, Slack: &config.SlackEndpoint{ChannelID: "s2"}},
	}}
	cursors := newFakeCursors()
	rec := &recorder{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := NewRecovery(NewGraphClient(srv.URL, srv.Client()), cfg, cursors, rec.dispatch, log)
	r.now = func() time.Time { return time.Now().Add(-time.Hour) }

	if err := r.SweepOnce(context.Background()); err != nil {
		t.Fatalf("SweepOnce() error = %v, want nil — one broken pair must not stop the others", err)
	}
	msgs := rec.seen()
	if len(msgs) != 1 || msgs[0].PairID != "healthy" {
		t.Fatalf("recovered %+v, want the healthy pair only", msgs)
	}
}

// TestRecoverySweepsRepeatedly proves Run's repeats are actually driven by
// its ticker, not just that "some number >= 3" of requests happened somehow.
// The server caps its own count at wantSweeps and signals a channel the
// instant that count is reached, so the assertion below can only be satisfied
// by at least wantSweeps-1 genuine ticker-driven round trips through Run —
// removing the ticker case makes this test hang until its timeout and fail,
// deterministically, with no wall-clock guessing.
func TestRecoverySweepsRepeatedly(t *testing.T) {
	const wantSweeps = 4 // 1 initial synchronous sweep + 3 driven by Run's ticker
	var (
		mu        sync.Mutex
		requests  int
		closeOnce sync.Once
	)
	done := make(chan struct{})

	mux := http.NewServeMux()
	mux.HandleFunc("/teams/t1/channels/c1/messages", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		if requests < wantSweeps {
			requests++
		}
		n := requests
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"value":[]}`)
		if n == wantSweeps {
			closeOnce.Do(func() { close(done) })
		}
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cursors := newFakeCursors()
	rec := &recorder{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := NewRecovery(NewGraphClient(srv.URL, srv.Client()), recoveryConfig(), cursors, rec.dispatch, log)
	r.SweepInterval = 5 * time.Millisecond

	// Perform the initial sweep synchronously; Run must not repeat it.
	if err := r.SweepOnce(context.Background()); err != nil {
		t.Fatalf("SweepOnce(): %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- r.Run(ctx) }()

	select {
	case <-done:
		// Reached wantSweeps entirely through Run's ticker-driven repeats.
	case <-time.After(2 * time.Second):
		mu.Lock()
		got := requests
		mu.Unlock()
		t.Fatalf("made %d total sweeps in 2s, want %d — Run must repeat SweepOnce on its "+
			"ticker, not just run it once at startup", got, wantSweeps)
	}
	cancel()

	select {
	case err := <-runErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Run(): %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not return within a second of cancellation")
	}

	mu.Lock()
	defer mu.Unlock()
	if requests != wantSweeps {
		t.Fatalf("made %d total sweeps, want exactly %d (1 initial + %d ticker-driven)",
			requests, wantSweeps, wantSweeps-1)
	}
}

func TestRecoveryRunReturnsOnCancel(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/teams/t1/channels/c1/messages", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"value":[]}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cursors := newFakeCursors()
	rec := &recorder{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := NewRecovery(NewGraphClient(srv.URL, srv.Client()), recoveryConfig(), cursors, rec.dispatch, log)
	r.SweepInterval = time.Hour // long enough that only cancellation can end Run

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not return within a second of cancellation")
	}
}

// The sweep is the only retry mechanism in the system: both webhooks answer
// before processing, so neither platform ever redelivers. If the watermark
// advances past messages that failed to deliver, the next sweep's window
// starts after them and they are lost with nothing but a counter to show
// for it.
func TestRecoveryKeepsWatermarkWhenDeliveryFails(t *testing.T) {
	fresh := stamp(time.Now())
	mux := http.NewServeMux()
	mux.HandleFunc("/teams/t1/channels/c1/messages", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"value":[`+
			`{"id":"1","messageType":"message","createdDateTime":"`+fresh+`","body":{"contentType":"html","content":"a"}},`+
			`{"id":"2","messageType":"message","createdDateTime":"`+fresh+`","body":{"contentType":"html","content":"b"}}`+
			`]}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	seededAt := time.Now().Add(-time.Hour).Truncate(time.Second)
	cursors := newFakeCursors()
	cursors.set("acme", store.DeltaCursor{Link: recoveryWatermark, UpdatedAt: seededAt})
	rec := &recorder{err: errors.New("deliver to slack: rate limited")}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := NewRecovery(NewGraphClient(srv.URL, srv.Client()), recoveryConfig(), cursors, rec.dispatch, log)

	if err := r.SweepOnce(context.Background()); err != nil {
		t.Fatalf("SweepOnce(): %v", err)
	}
	if n := len(rec.seen()); n != 2 {
		t.Fatalf("dispatched %d messages, want 2", n)
	}

	got, err := cursors.LoadDeltaCursor(context.Background(), "acme")
	if err != nil {
		t.Fatalf("LoadDeltaCursor(): %v", err)
	}
	if !got.UpdatedAt.Equal(seededAt) {
		t.Fatalf("watermark = %v, want it left at %v — advancing past messages "+
			"that never arrived loses them permanently", got.UpdatedAt, seededAt)
	}
}

// The mirror of the above: when every message is handled the watermark must
// advance, or every sweep replays the same range forever.
func TestRecoveryAdvancesWatermarkWhenDeliverySucceeds(t *testing.T) {
	fresh := stamp(time.Now())
	mux := http.NewServeMux()
	mux.HandleFunc("/teams/t1/channels/c1/messages", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"value":[`+
			`{"id":"1","messageType":"message","createdDateTime":"`+fresh+`","body":{"contentType":"html","content":"a"}}`+
			`]}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	seededAt := time.Now().Add(-time.Hour)
	cursors := newFakeCursors()
	cursors.set("acme", store.DeltaCursor{Link: recoveryWatermark, UpdatedAt: seededAt})
	rec := &recorder{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := NewRecovery(NewGraphClient(srv.URL, srv.Client()), recoveryConfig(), cursors, rec.dispatch, log)

	if err := r.SweepOnce(context.Background()); err != nil {
		t.Fatalf("SweepOnce(): %v", err)
	}
	got, err := cursors.LoadDeltaCursor(context.Background(), "acme")
	if err != nil {
		t.Fatalf("LoadDeltaCursor(): %v", err)
	}
	if !got.UpdatedAt.After(seededAt) {
		t.Fatalf("watermark = %v, want it advanced past %v", got.UpdatedAt, seededAt)
	}
}

// A sweep dispatches through the full delivery path, so at shutdown it may be
// mid-delivery while holding the database pool. Nothing waited for it: the
// process closed the pool underneath, the claim release failed against a closed
// pool, and the message stayed claimed forever — lost, with one error line.
func TestRecoveryDrainWaitsForAnInFlightSweep(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/teams/t1/channels/c1/messages", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"value":[`+
			`{"id":"1","messageType":"message","createdDateTime":"`+stamp(time.Now())+`","body":{"contentType":"html","content":"a"}}`+
			`]}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	entered := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	var once sync.Once
	dispatch := func(context.Context, model.Message) error {
		once.Do(func() { close(entered) })
		<-release
		close(finished)
		return nil
	}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := NewRecovery(NewGraphClient(srv.URL, srv.Client()), recoveryConfig(),
		newFakeCursors(), dispatch, log)
	r.SweepInterval = 10 * time.Millisecond
	r.now = func() time.Time { return time.Now().Add(-time.Hour) }

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = r.Run(ctx) }()

	<-entered
	// Shutdown arrives while the sweep is mid-delivery.
	cancel()

	drained := make(chan error, 1)
	go func() {
		drainCtx, drainCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer drainCancel()
		drained <- r.Drain(drainCtx)
	}()

	select {
	case err := <-drained:
		t.Fatalf("Drain returned %v while a sweep was still delivering", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	if err := <-drained; err != nil {
		t.Fatalf("Drain: %v", err)
	}
	select {
	case <-finished:
	default:
		t.Fatal("Drain returned before the in-flight delivery completed")
	}
}

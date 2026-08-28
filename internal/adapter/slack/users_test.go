package slack

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	goslack "github.com/slack-go/slack"
)

// userServer serves users.info, counting calls so caching can be asserted.
func userServer(t *testing.T, payload string) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var calls atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("/users.info", func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write([]byte(payload)); err != nil {
			t.Errorf("write response: %v", err)
		}
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, &calls
}

func newTestDirectory(t *testing.T, srv *httptest.Server) *APIUserDirectory {
	t.Helper()
	return NewUserDirectory(goslack.New("xoxb-test", goslack.OptionAPIURL(srv.URL+"/")))
}

func TestUserDirectoryPrefersDisplayName(t *testing.T) {
	srv, _ := userServer(t, `{"ok":true,"user":{"id":"U123","name":"ada",
		"real_name":"Ada L","profile":{"display_name":"Ada Lovelace","real_name":"Ada L"}}}`)
	dir := newTestDirectory(t, srv)

	if got := dir.DisplayName(context.Background(), "U123"); got != "Ada Lovelace" {
		t.Fatalf("DisplayName() = %q, want %q", got, "Ada Lovelace")
	}
}

func TestUserDirectoryFallsBackThroughRealNameToHandle(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		want    string
	}{
		{
			name: "real name when the display name is unset",
			payload: `{"ok":true,"user":{"id":"U123","name":"ada","real_name":"Ada Lovelace",
				"profile":{"display_name":"","real_name":"Ada Lovelace"}}}`,
			want: "Ada Lovelace",
		},
		{
			name:    "handle when neither name is set",
			payload: `{"ok":true,"user":{"id":"U123","name":"ada","real_name":"","profile":{}}}`,
			want:    "ada",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := userServer(t, tc.payload)
			dir := newTestDirectory(t, srv)

			if got := dir.DisplayName(context.Background(), "U123"); got != tc.want {
				t.Fatalf("DisplayName() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestUserDirectoryCachesResolvedNames(t *testing.T) {
	srv, calls := userServer(t, `{"ok":true,"user":{"id":"U123","name":"ada",
		"profile":{"display_name":"Ada Lovelace"}}}`)
	dir := newTestDirectory(t, srv)

	for i := 0; i < 3; i++ {
		if got := dir.DisplayName(context.Background(), "U123"); got != "Ada Lovelace" {
			t.Fatalf("call %d: DisplayName() = %q", i, got)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("users.info called %d times, want 1 — a lookup per message would burn the rate limit", got)
	}
}

// A failed lookup must not be cached: a transient outage would otherwise
// leave every author of that user anonymous for the lifetime of the process.
func TestUserDirectoryDoesNotCacheFailures(t *testing.T) {
	srv, calls := userServer(t, `{"ok":false,"error":"user_not_found"}`)
	dir := newTestDirectory(t, srv)

	if got := dir.DisplayName(context.Background(), "U123"); got != "" {
		t.Fatalf("DisplayName() = %q, want empty on failure", got)
	}
	if got := dir.DisplayName(context.Background(), "U123"); got != "" {
		t.Fatalf("DisplayName() = %q, want empty on failure", got)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("users.info called %d times, want 2 — a failure must not be cached", got)
	}
}

func TestUserDirectoryEmptyUserIDDoesNotCallTheAPI(t *testing.T) {
	srv, calls := userServer(t, `{"ok":true,"user":{"id":"U123","name":"ada"}}`)
	dir := newTestDirectory(t, srv)

	if got := dir.DisplayName(context.Background(), ""); got != "" {
		t.Fatalf("DisplayName(\"\") = %q, want empty", got)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("users.info called %d times for an empty id, want 0", got)
	}
}

func TestUserDirectoryIsSafeUnderConcurrency(t *testing.T) {
	srv, _ := userServer(t, `{"ok":true,"user":{"id":"U123","name":"ada",
		"profile":{"display_name":"Ada Lovelace"}}}`)
	dir := newTestDirectory(t, srv)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got := dir.DisplayName(context.Background(), "U123"); got != "Ada Lovelace" {
				t.Errorf("DisplayName() = %q", got)
			}
		}()
	}
	wg.Wait()
}

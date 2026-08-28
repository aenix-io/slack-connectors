package slack

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	goslack "github.com/slack-go/slack"

	"github.com/aenix-io/slack-connectors/internal/bridge"
	"github.com/aenix-io/slack-connectors/internal/format/teamshtml"
	"github.com/aenix-io/slack-connectors/internal/model"
)

// captureServer records the form values of the last chat.postMessage call.
func captureServer(t *testing.T, got *url.Values) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/chat.postMessage", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Errorf("ParseForm(): %v", err)
		}
		*got = r.Form
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"channel":"C01ABCDEF","ts":"1700000000.000100"}`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func newTestSink(t *testing.T, srv *httptest.Server) *Sink {
	t.Helper()
	client := goslack.New("xoxb-test", goslack.OptionAPIURL(srv.URL+"/"))
	return New(client, func(m model.Message) string { return teamshtml.ToMrkdwn(m.Text) }, "Microsoft Teams")
}

func testMessage() model.Message {
	return model.Message{
		Platform: model.PlatformTeams,
		ID:       "teams-1",
		PairID:   "acme",
		Author: model.Author{
			DisplayName: "Ivan Petrov",
			AvatarURL:   "https://example.com/ivan.png",
		},
		Text: "<p>hello <strong>world</strong></p>",
		Kind: model.EventCreated,
	}
}

func TestDeliverReturnsTimestamp(t *testing.T) {
	var form url.Values
	srv := captureServer(t, &form)
	sink := newTestSink(t, srv)

	ts, err := sink.Deliver(context.Background(), testMessage(), "C01ABCDEF", "")
	if err != nil {
		t.Fatalf("Deliver(): %v", err)
	}
	if ts != "1700000000.000100" {
		t.Fatalf("Deliver() = %q, want 1700000000.000100", ts)
	}
}

func TestDeliverConvertsHTMLToMrkdwn(t *testing.T) {
	var form url.Values
	srv := captureServer(t, &form)
	sink := newTestSink(t, srv)

	if _, err := sink.Deliver(context.Background(), testMessage(), "C01ABCDEF", ""); err != nil {
		t.Fatalf("Deliver(): %v", err)
	}
	if got := form.Get("text"); got != "hello *world*" {
		t.Fatalf("text = %q, want %q", got, "hello *world*")
	}
}

func TestDeliverSetsAuthorIdentity(t *testing.T) {
	var form url.Values
	srv := captureServer(t, &form)
	sink := newTestSink(t, srv)

	if _, err := sink.Deliver(context.Background(), testMessage(), "C01ABCDEF", ""); err != nil {
		t.Fatalf("Deliver(): %v", err)
	}
	if got := form.Get("username"); got != "Ivan Petrov" {
		t.Fatalf("username = %q, want %q", got, "Ivan Petrov")
	}
	if got := form.Get("icon_url"); got != "https://example.com/ivan.png" {
		t.Fatalf("icon_url = %q, want the author avatar", got)
	}
}

func TestDeliverSetsThreadTimestamp(t *testing.T) {
	var form url.Values
	srv := captureServer(t, &form)
	sink := newTestSink(t, srv)

	if _, err := sink.Deliver(context.Background(), testMessage(), "C01ABCDEF", "1700000000.000001"); err != nil {
		t.Fatalf("Deliver(): %v", err)
	}
	if got := form.Get("thread_ts"); got != "1700000000.000001" {
		t.Fatalf("thread_ts = %q, want 1700000000.000001", got)
	}
}

func TestDeliverOmitsThreadTimestampForRoot(t *testing.T) {
	var form url.Values
	srv := captureServer(t, &form)
	sink := newTestSink(t, srv)

	if _, err := sink.Deliver(context.Background(), testMessage(), "C01ABCDEF", ""); err != nil {
		t.Fatalf("Deliver(): %v", err)
	}
	// Has, not Get: url.Values.Get returns "" both for an absent key and for
	// a key present with an empty value, so Get would not catch a regression
	// that sent thread_ts= on the wire — which Slack treats differently from
	// omitting it.
	if form.Has("thread_ts") {
		t.Fatalf("thread_ts = %q, want the key absent entirely for a thread root", form.Get("thread_ts"))
	}
}

func TestDeliverFallsBackWhenAuthorHasNoName(t *testing.T) {
	var form url.Values
	srv := captureServer(t, &form)
	sink := newTestSink(t, srv)

	msg := testMessage()
	msg.Author.DisplayName = ""
	if _, err := sink.Deliver(context.Background(), msg, "C01ABCDEF", ""); err != nil {
		t.Fatalf("Deliver(): %v", err)
	}
	if got := form.Get("username"); got != "Microsoft Teams" {
		t.Fatalf("username = %q, want the fallback %q", got, "Microsoft Teams")
	}
}

func TestDeliverSurfacesAPIErrors(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/chat.postMessage", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":false,"error":"channel_not_found"}`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	sink := newTestSink(t, srv)

	if _, err := sink.Deliver(context.Background(), testMessage(), "C-missing", ""); err == nil {
		t.Fatal("Deliver() error = nil, want the Slack API error to surface")
	}
}

// A Slack API error means Slack answered and refused, so nothing was posted
// and the bridge may retry. A lost response means the post may have landed,
// and the sink must not claim otherwise.
func TestDeliverClassifiesFailures(t *testing.T) {
	t.Run("an api error is provably unsent", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.HandleFunc("/chat.postMessage", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ok":false,"error":"channel_not_found"}`))
		})
		srv := httptest.NewServer(mux)
		t.Cleanup(srv.Close)
		sink := newTestSink(t, srv)

		_, err := sink.Deliver(context.Background(), testMessage(), "C01ABCDEF", "")
		if err == nil {
			t.Fatal("Deliver: expected an error")
		}
		if !errors.Is(err, bridge.ErrNotSent) {
			t.Fatalf("error = %v, want it to wrap bridge.ErrNotSent", err)
		}
	})

	t.Run("a lost response is not claimed as unsent", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.HandleFunc("/chat.postMessage", func(w http.ResponseWriter, _ *http.Request) {
			if hj, ok := w.(http.Hijacker); ok {
				conn, _, err := hj.Hijack()
				if err == nil {
					_ = conn.Close()
				}
			}
		})
		srv := httptest.NewServer(mux)
		t.Cleanup(srv.Close)
		sink := newTestSink(t, srv)

		_, err := sink.Deliver(context.Background(), testMessage(), "C01ABCDEF", "")
		if err == nil {
			t.Fatal("Deliver: expected an error")
		}
		if errors.Is(err, bridge.ErrNotSent) {
			t.Fatalf("error = %v, must NOT wrap bridge.ErrNotSent — "+
				"the post may have landed", err)
		}
	})
}

// chat.postMessage refuses an empty body, and an empty body is not a
// hypothetical: any Teams content this converter has no rendering for produces
// one. Failing forever on it is worse than posting a marker, because the
// message is retried until the recovery cursor passes it and it is lost.
func TestDeliverNeverPostsAnEmptyBody(t *testing.T) {
	cases := []struct{ name, text string }{
		{name: "empty text", text: ""},
		{name: "whitespace only", text: "   \n  "},
		{name: "html that renders to nothing", text: "<style>.a{color:red}</style>"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var form url.Values
			srv := captureServer(t, &form)
			sink := newTestSink(t, srv)

			msg := testMessage()
			msg.Text = tc.text
			if _, err := sink.Deliver(context.Background(), msg, "C01ABCDEF", ""); err != nil {
				t.Fatalf("Deliver(): %v", err)
			}
			if got := form.Get("text"); strings.TrimSpace(got) == "" {
				t.Fatalf("posted text = %q, want a non-empty placeholder", got)
			}
		})
	}
}

// Slack allows roughly one message per second per channel, so a throttle is
// ordinary under any burst. Retrying it here keeps the message moving instead
// of leaving it to whatever replays failures.
func TestDeliverRetriesWhenThrottled(t *testing.T) {
	t.Run("a throttled post is retried and succeeds", func(t *testing.T) {
		var mu sync.Mutex
		attempts := 0
		mux := http.NewServeMux()
		mux.HandleFunc("/chat.postMessage", func(w http.ResponseWriter, _ *http.Request) {
			mu.Lock()
			attempts++
			first := attempts == 1
			mu.Unlock()
			if first {
				w.Header().Set("Retry-After", "0")
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"ok":true,"channel":"C01ABCDEF","ts":"1700000000.000200"}`)
		})
		srv := httptest.NewServer(mux)
		t.Cleanup(srv.Close)

		sink := newTestSink(t, srv)
		ts, err := sink.Deliver(context.Background(), testMessage(), "C01ABCDEF", "")
		if err != nil {
			t.Fatalf("Deliver(): %v", err)
		}
		if ts != "1700000000.000200" {
			t.Fatalf("ts = %q, want the timestamp from the successful retry", ts)
		}
		mu.Lock()
		defer mu.Unlock()
		if attempts != 2 {
			t.Fatalf("attempts = %d, want 2", attempts)
		}
	})

	t.Run("exhausted retries stay retriable", func(t *testing.T) {
		var mu sync.Mutex
		attempts := 0
		mux := http.NewServeMux()
		mux.HandleFunc("/chat.postMessage", func(w http.ResponseWriter, _ *http.Request) {
			mu.Lock()
			attempts++
			mu.Unlock()
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
		})
		srv := httptest.NewServer(mux)
		t.Cleanup(srv.Close)

		sink := newTestSink(t, srv)
		_, err := sink.Deliver(context.Background(), testMessage(), "C01ABCDEF", "")
		if err == nil {
			t.Fatal("Deliver(): expected an error once the attempts are exhausted")
		}
		if !errors.Is(err, bridge.ErrNotSent) {
			t.Fatalf("error = %v, want it to stay ErrNotSent so the claim is released", err)
		}
		mu.Lock()
		defer mu.Unlock()
		if attempts != maxPostAttempts {
			t.Fatalf("attempts = %d, want %d", attempts, maxPostAttempts)
		}
	})

	t.Run("a permanent refusal is not retried", func(t *testing.T) {
		var mu sync.Mutex
		attempts := 0
		mux := http.NewServeMux()
		mux.HandleFunc("/chat.postMessage", func(w http.ResponseWriter, _ *http.Request) {
			mu.Lock()
			attempts++
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"ok":false,"error":"channel_not_found"}`)
		})
		srv := httptest.NewServer(mux)
		t.Cleanup(srv.Close)

		sink := newTestSink(t, srv)
		if _, err := sink.Deliver(context.Background(), testMessage(), "C01ABCDEF", ""); err == nil {
			t.Fatal("Deliver(): expected an error")
		}
		mu.Lock()
		defer mu.Unlock()
		if attempts != 1 {
			t.Fatalf("attempts = %d, want 1", attempts)
		}
	})
}

// The sink used to call the Teams converter directly. It now takes one,
// because the same sink serves a connector whose bodies are Signal plain text
// — and a sink that silently ran Teams HTML rules over them would eat every
// underscore and asterisk a person typed.
func TestDeliverUsesTheInjectedRenderer(t *testing.T) {
	var form url.Values
	srv := captureServer(t, &form)
	client := goslack.New("xoxb-test", goslack.OptionAPIURL(srv.URL+"/"))
	sink := New(client, func(m model.Message) string { return "rendered:" + m.Text }, "Signal")

	msg := testMessage()
	msg.Text = "plain body"
	if _, err := sink.Deliver(context.Background(), msg, "C01ABCDEF", ""); err != nil {
		t.Fatalf("Deliver(): %v", err)
	}
	if got := form.Get("text"); got != "rendered:plain body" {
		t.Fatalf("text = %q, want the injected renderer's output", got)
	}
}

func TestDeliverFallsBackToTheConfiguredSourceName(t *testing.T) {
	var form url.Values
	srv := captureServer(t, &form)
	client := goslack.New("xoxb-test", goslack.OptionAPIURL(srv.URL+"/"))
	sink := New(client, nil, "Signal")

	msg := testMessage()
	msg.Author.DisplayName = ""
	if _, err := sink.Deliver(context.Background(), msg, "C01ABCDEF", ""); err != nil {
		t.Fatalf("Deliver(): %v", err)
	}
	if got := form.Get("username"); got != "Signal" {
		t.Fatalf("username = %q, want the configured source name", got)
	}
	// A nil renderer passes the body through: the caller said there is nothing
	// to convert, not that there is nothing to post.
	if got := form.Get("text"); got != msg.Text {
		t.Fatalf("text = %q, want the body unchanged", got)
	}
}

// A file sent without a caption is the ordinary case. The generic placeholder
// reads as something having gone wrong, which is not what happened.
func TestDeliverNamesAnUncaptionedFile(t *testing.T) {
	// Every post is collected rather than only the last: a file that cannot
	// be transferred adds a notice afterwards, and the message itself is the
	// one under test.
	var mu sync.Mutex
	var posted []string
	mux := http.NewServeMux()
	mux.HandleFunc("/chat.postMessage", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Errorf("ParseForm(): %v", err)
		}
		mu.Lock()
		posted = append(posted, r.Form.Get("text"))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"channel":"C01ABCDEF","ts":"1700000000.000100"}`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := goslack.New("xoxb-test", goslack.OptionAPIURL(srv.URL+"/"))
	sink := New(client, func(model.Message) string { return "" }, "Signal")

	msg := testMessage()
	msg.Text = ""
	msg.Attachments = []model.Attachment{{Name: "q3.pdf"}, {Name: "notes.md"}}
	if _, err := sink.Deliver(context.Background(), msg, "C01ABCDEF", ""); err != nil {
		t.Fatalf("Deliver(): %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(posted) == 0 {
		t.Fatal("nothing was posted")
	}
	if posted[0] != "📎 q3.pdf, notes.md" {
		t.Fatalf("text = %q, want the files named", posted[0])
	}
}

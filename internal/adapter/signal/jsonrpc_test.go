package signal

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// fakeDaemon is the signal-cli side of the connection: it reads requests line
// by line and answers with whatever the test tells it to.
type fakeDaemon struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader

	mu       sync.Mutex
	requests []request
}

func newFakeDaemon(t *testing.T) (*Client, *fakeDaemon) {
	t.Helper()
	ours, theirs := net.Pipe()
	d := &fakeDaemon{t: t, conn: theirs, r: bufio.NewReader(theirs)}
	c := NewClient(ours, nil, testLogger())
	t.Cleanup(func() {
		_ = c.Close()
		_ = theirs.Close()
	})
	return c, d
}

// nextRequest reads one request the client sent.
func (d *fakeDaemon) nextRequest() request {
	d.t.Helper()
	_ = d.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := d.r.ReadBytes('\n')
	if err != nil {
		d.t.Fatalf("read request: %v", err)
	}
	var req request
	if err := json.Unmarshal(line, &req); err != nil {
		d.t.Fatalf("decode request %q: %v", line, err)
	}
	d.mu.Lock()
	d.requests = append(d.requests, req)
	d.mu.Unlock()
	return req
}

func (d *fakeDaemon) write(raw string) {
	d.t.Helper()
	_ = d.conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.WriteString(d.conn, raw+"\n"); err != nil {
		d.t.Fatalf("write frame: %v", err)
	}
}

func serve(t *testing.T, c *Client) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Serve(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("Serve did not return after cancellation")
		}
	})
	return cancel
}

func TestCallSendsARequestAndReturnsItsResult(t *testing.T) {
	c, daemon := newFakeDaemon(t)
	serve(t, c)

	type sendResult struct {
		Timestamp int64 `json:"timestamp"`
	}
	var got sendResult
	errCh := make(chan error, 1)
	go func() {
		errCh <- c.Call(context.Background(), "send",
			map[string]any{"groupId": "Ci8K", "message": "hi"}, &got)
	}()

	req := daemon.nextRequest()
	if req.Method != "send" {
		t.Fatalf("method = %q, want send", req.Method)
	}
	if req.JSONRPC != "2.0" {
		t.Fatalf("jsonrpc = %q, want 2.0", req.JSONRPC)
	}
	if req.ID == "" {
		t.Fatal("request carried no id; responses could not be matched")
	}
	daemon.write(`{"jsonrpc":"2.0","id":"` + req.ID + `","result":{"timestamp":1754400000000}}`)

	if err := <-errCh; err != nil {
		t.Fatalf("Call(): %v", err)
	}
	if got.Timestamp != 1754400000000 {
		t.Fatalf("timestamp = %d, want 1754400000000", got.Timestamp)
	}
}

func TestCallReturnsTheDaemonError(t *testing.T) {
	cases := []struct {
		name           string
		code           int
		wantNotSent    bool
		whyNotRetryabe string
	}{
		{
			name:        "user error is a refusal",
			code:        CodeUserError,
			wantNotSent: true,
		},
		{
			name:        "untrusted key is a refusal",
			code:        CodeUntrustedKey,
			wantNotSent: true,
		},
		{
			// An IO error can be raised after the message was handed over.
			// Treating it as "not sent" would let a retry post it twice.
			name:        "io error is ambiguous",
			code:        CodeIOError,
			wantNotSent: false,
		},
		{
			// signal-cli raises this only when no recipient succeeded and
			// every failure was a rate limit, so nothing was delivered and a
			// retry cannot duplicate. A partial rate limit inside a group
			// comes back as a success with per-recipient results instead.
			name:        "an outright rate limit is a refusal",
			code:        CodeRateLimit,
			wantNotSent: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, daemon := newFakeDaemon(t)
			serve(t, c)

			errCh := make(chan error, 1)
			go func() { errCh <- c.Call(context.Background(), "send", nil, nil) }()

			req := daemon.nextRequest()
			daemon.write(`{"jsonrpc":"2.0","id":"` + req.ID +
				`","error":{"code":` + strconv.Itoa(tc.code) + `,"message":"nope"}}`)

			err := <-errCh
			if err == nil {
				t.Fatal("Call() error = nil, want the daemon error")
			}
			var rpcErr *Error
			if !errors.As(err, &rpcErr) {
				t.Fatalf("error = %v, want a *signal.Error", err)
			}
			if rpcErr.Code != tc.code {
				t.Fatalf("code = %d, want %d", rpcErr.Code, tc.code)
			}
			if got := rpcErr.ProvablyNotSent(); got != tc.wantNotSent {
				t.Fatalf("ProvablyNotSent() = %v, want %v", got, tc.wantNotSent)
			}
		})
	}
}

func TestServeDispatchesNotifications(t *testing.T) {
	ours, theirs := net.Pipe()
	t.Cleanup(func() { _ = theirs.Close() })

	type received struct {
		method string
		params string
	}
	got := make(chan received, 1)
	c := NewClient(ours, func(_ context.Context, method string, params json.RawMessage) {
		got <- received{method: method, params: string(params)}
	}, testLogger())
	t.Cleanup(func() { _ = c.Close() })
	serve(t, c)

	go func() {
		_, _ = io.WriteString(theirs,
			`{"jsonrpc":"2.0","method":"receive","params":{"envelope":{"timestamp":1}}}`+"\n")
	}()

	select {
	case r := <-got:
		if r.method != "receive" {
			t.Fatalf("method = %q, want receive", r.method)
		}
		if r.params == "" {
			t.Fatal("params were empty")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("notification was never dispatched")
	}
}

// This is the delivery guarantee, expressed as a test.
//
// signal-cli acknowledges an envelope to the Signal server and forgets it as
// soon as its handler returns, and nothing replays. Handling notifications on
// the read loop means an occupied bridge stops reading, signal-cli stops
// acknowledging, and messages wait on the server. If someone later dispatches
// notifications to a goroutine, this test fails — which is the point.
func TestServeAppliesBackPressureWhileANotificationIsHandled(t *testing.T) {
	ours, theirs := net.Pipe()
	t.Cleanup(func() { _ = theirs.Close() })

	release := make(chan struct{})
	entered := make(chan struct{}, 2)

	c := NewClient(ours, func(context.Context, string, json.RawMessage) {
		entered <- struct{}{}
		<-release
	}, testLogger())
	t.Cleanup(func() { _ = c.Close() })
	serve(t, c)

	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		// Two notifications, written back to back. The pipe is unbuffered, so
		// the second write only completes once the reader takes it.
		for i := 0; i < 2; i++ {
			_, _ = io.WriteString(theirs,
				`{"jsonrpc":"2.0","method":"receive","params":{"envelope":{"timestamp":1}}}`+"\n")
		}
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the first notification was never handled")
	}

	// While the handler is blocked, no second notification may be handled.
	select {
	case <-entered:
		t.Fatal("a second notification was handled while the first was still in progress; " +
			"notifications must not be dispatched concurrently — signal-cli has already " +
			"acknowledged them to the server and nothing replays what is dropped")
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the second notification was never handled after the first finished")
	}
}

func TestCallFailsWhenTheConnectionCloses(t *testing.T) {
	c, daemon := newFakeDaemon(t)
	serve(t, c)

	errCh := make(chan error, 1)
	go func() { errCh <- c.Call(context.Background(), "send", nil, nil) }()

	daemon.nextRequest()
	_ = daemon.conn.Close()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("Call() error = nil, want a failure once the daemon went away")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Call() hung after the connection closed")
	}
}

func TestCallRespectsContextCancellation(t *testing.T) {
	c, daemon := newFakeDaemon(t)
	serve(t, c)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- c.Call(ctx, "send", nil, nil) }()

	daemon.nextRequest()
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Call() ignored its context")
	}
}

// A response arriving after its caller gave up must not be handed to the next
// one: that would answer a question nobody asked, with a timestamp belonging
// to a different message.
func TestCallDiscardsAStaleResponse(t *testing.T) {
	c, daemon := newFakeDaemon(t)
	serve(t, c)

	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() { first <- c.Call(ctx, "send", nil, nil) }()
	abandoned := daemon.nextRequest()
	cancel()
	<-first

	// The daemon answers the abandoned call late.
	daemon.write(`{"jsonrpc":"2.0","id":"` + abandoned.ID + `","result":{"timestamp":1}}`)

	type sendResult struct {
		Timestamp int64 `json:"timestamp"`
	}
	var got sendResult
	second := make(chan error, 1)
	go func() { second <- c.Call(context.Background(), "send", nil, &got) }()

	req := daemon.nextRequest()
	daemon.write(`{"jsonrpc":"2.0","id":"` + req.ID + `","result":{"timestamp":222}}`)

	if err := <-second; err != nil {
		t.Fatalf("second Call(): %v", err)
	}
	if got.Timestamp != 222 {
		t.Fatalf("timestamp = %d, want 222 — the stale response was delivered to the wrong caller", got.Timestamp)
	}
}

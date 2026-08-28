// Package signal talks to a signal-cli daemon over its JSON-RPC interface.
//
// signal-cli is the only practical way to speak the Signal protocol from a
// server: there is no official API and no bot framework. It runs as a sidecar
// and this package is the whole of our side of that conversation.
package signal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// JSON-RPC error codes signal-cli uses on top of the standard ones. They are
// the difference between "the message was refused" and "we do not know", and
// that distinction decides whether a failed delivery may be retried.
const (
	CodeUserError     = -1
	CodeIOError       = -3
	CodeUntrustedKey  = -4
	CodeRateLimit     = -5
	CodeCaptchaReject = -6
)

// Error is a JSON-RPC error returned by signal-cli.
type Error struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *Error) Error() string {
	return fmt.Sprintf("signal-cli error %d: %s", e.Code, e.Message)
}

// ProvablyNotSent reports whether this error means the request never reached
// the Signal service.
//
// A rate limit counts, which is not obvious: signal-cli raises this code only
// when no recipient succeeded and every failure was a rate limit, so nothing
// was delivered and a retry cannot duplicate. A partial rate limit inside a
// group is reported as a success with per-recipient results instead.
//
// An IO error does not count: it may have been raised after the message was
// handed over, and treating that as "not sent" would let a retry post the
// same message into a customer's conversation twice.
func (e *Error) ProvablyNotSent() bool {
	switch e.Code {
	case CodeUserError, CodeUntrustedKey, CodeCaptchaReject, CodeRateLimit:
		return true
	default:
		return false
	}
}

// RetryAfter reads how long signal-cli asked the caller to wait, or zero when
// it said nothing. The hint travels in the error's data, which carries the
// per-recipient results of the send that failed.
func (e *Error) RetryAfter() time.Duration {
	if len(e.Data) == 0 {
		return 0
	}
	var data struct {
		Results []struct {
			RetryAfterSeconds int64 `json:"retryAfterSeconds"`
		} `json:"results"`
	}
	if err := json.Unmarshal(e.Data, &data); err != nil {
		return 0
	}
	var longest int64
	for _, r := range data.Results {
		if r.RetryAfterSeconds > longest {
			longest = r.RetryAfterSeconds
		}
	}
	return time.Duration(longest) * time.Second
}

// request is one outgoing JSON-RPC call.
type request struct {
	JSONRPC string `json:"jsonrpc"`
	ID      string `json:"id,omitempty"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

// frame is anything arriving on the connection: a response to one of our
// requests, or a notification signal-cli sends on its own.
type frame struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      *string         `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	Result  json.RawMessage `json:"result"`
	Error   *Error          `json:"error"`
}

// NotificationHandler is called for every notification signal-cli sends.
//
// It runs on the connection's read loop, and that is deliberate: see Serve.
type NotificationHandler func(ctx context.Context, method string, params json.RawMessage)

// Client is a JSON-RPC client for one signal-cli daemon.
//
// One connection, one reader goroutine, responses routed by request id.
type Client struct {
	conn net.Conn
	dec  *json.Decoder
	enc  *json.Encoder

	// callMu serialises requests end to end, response included.
	callMu sync.Mutex
	// writeMu guards the encoder alone, so a caller cannot interleave with
	// whatever the read loop is doing.
	writeMu sync.Mutex

	nextID atomic.Uint64

	// pendingMu guards pending, which routes each response to the caller
	// that asked for it.
	pendingMu sync.Mutex
	pending   map[string]chan *frame

	handler   NotificationHandler
	log       *slog.Logger
	closeOnce sync.Once
	closed    chan struct{}
}

// Dial connects to a signal-cli daemon. network and address are what net.Dial
// takes: "unix" and a socket path in the deployment, "tcp" in a test.
func Dial(ctx context.Context, network, address string, handler NotificationHandler, log *slog.Logger) (*Client, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, network, address)
	if err != nil {
		return nil, fmt.Errorf("dial signal-cli at %s://%s: %w", network, address, err)
	}
	return NewClient(conn, handler, log), nil
}

// NewClient wraps an established connection.
func NewClient(conn net.Conn, handler NotificationHandler, log *slog.Logger) *Client {
	if handler == nil {
		handler = func(context.Context, string, json.RawMessage) {}
	}
	return &Client{
		conn:    conn,
		dec:     json.NewDecoder(conn),
		enc:     json.NewEncoder(conn),
		pending: make(map[string]chan *frame),
		handler: handler,
		log:     log,
		closed:  make(chan struct{}),
	}
}

// Serve reads the connection until it ends or ctx is cancelled, dispatching
// responses to callers and notifications to the handler.
//
// Notifications are handled inline, on this goroutine, and nothing else may
// change that. signal-cli acknowledges an envelope to the Signal server and
// drops its local copy as soon as its handler returns; there is no replay and
// no cursor to rewind. Handling a message here means the socket is not read
// again until it has been delivered, so an overwhelmed or stuck bridge stops
// draining the connection, signal-cli stops acknowledging, and the messages
// wait on the server instead of being lost. Dispatching to a goroutine would
// turn every restart into a handful of silently dropped customer messages.
func (c *Client) Serve(ctx context.Context) error {
	defer close(c.closed)

	// The decoder has no deadline of its own; closing the connection is what
	// unblocks it when the caller gives up.
	go func() {
		<-ctx.Done()
		_ = c.conn.Close()
	}()

	for {
		var f frame
		if err := c.dec.Decode(&f); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return fmt.Errorf("signal-cli connection closed: %w", err)
			}
			return fmt.Errorf("decode signal-cli frame: %w", err)
		}

		if f.ID != nil && f.Method == "" {
			c.deliverResponse(*f.ID, &f)
			continue
		}
		if f.Method != "" {
			c.handler(ctx, f.Method, f.Params)
			continue
		}
		c.log.Warn("ignoring a signal-cli frame that is neither a response nor a notification")
	}
}

// deliverResponse hands a response to the caller waiting on its id.
//
// A response whose id nobody is waiting for is dropped, which is what makes
// an abandoned call harmless: when a caller's context expires while the
// answer is in flight, that answer arrives here with no one to give it to
// rather than being handed to whoever calls next — a timestamp belonging to
// another message, recorded against this one.
func (c *Client) deliverResponse(id string, f *frame) {
	c.pendingMu.Lock()
	ch, ok := c.pending[id]
	c.pendingMu.Unlock()
	if !ok {
		c.log.Warn("discarding a signal-cli response nobody is waiting for", "id", id)
		return
	}
	// Buffered by one and written once, so this never blocks the read loop.
	ch <- f
}

// Call sends a request and waits for its response, decoding the result into
// out when out is non-nil.
//
// Calls are serialised. signal-cli has a known defect where a message sent
// before the previous response arrives can be dropped, and the ordering the
// bridge promises a customer is not worth trading for concurrency it does not
// need.
func (c *Client) Call(ctx context.Context, method string, params any, out any) error {
	c.callMu.Lock()
	defer c.callMu.Unlock()

	id := fmt.Sprintf("%d", c.nextID.Add(1))
	response := make(chan *frame, 1)

	c.pendingMu.Lock()
	c.pending[id] = response
	c.pendingMu.Unlock()
	defer func() {
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
	}()

	if err := c.write(request{JSONRPC: "2.0", ID: id, Method: method, Params: params}); err != nil {
		return fmt.Errorf("send %s: %w", method, err)
	}

	select {
	case f := <-response:
		if f.Error != nil {
			return fmt.Errorf("%s: %w", method, f.Error)
		}
		if out == nil || len(f.Result) == 0 {
			return nil
		}
		if err := json.Unmarshal(f.Result, out); err != nil {
			return fmt.Errorf("decode %s result: %w", method, err)
		}
		return nil
	case <-c.closed:
		return fmt.Errorf("%s: signal-cli connection closed before answering", method)
	case <-ctx.Done():
		return fmt.Errorf("%s: %w", method, ctx.Err())
	}
}

func (c *Client) write(req request) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.enc.Encode(req)
}

// Close ends the connection. Serve returns shortly afterwards.
func (c *Client) Close() error {
	var err error
	c.closeOnce.Do(func() { err = c.conn.Close() })
	return err
}

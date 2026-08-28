package signal

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aenix-io/slack-connectors/internal/bridge"
	"github.com/aenix-io/slack-connectors/internal/config"
	"github.com/aenix-io/slack-connectors/internal/format/signaltext"
	"github.com/aenix-io/slack-connectors/internal/metrics"
	"github.com/aenix-io/slack-connectors/internal/model"
)

// fallbackAuthorName is shown when the source message carries no author name.
const fallbackAuthorName = "Slack"

// caller is what a Sink needs from the JSON-RPC connection. It is an
// interface so the send path can be tested without a daemon.
type caller interface {
	Call(ctx context.Context, method string, params, out any) error
}

// Sink posts canonical messages into a Signal conversation.
//
// Signal offers no impersonation: everything is sent by the bridge number, so
// the author is the first line of the body, in bold — the same shape the
// Teams sink uses, for the same reason.
type Sink struct {
	client        caller
	cfg           config.Config
	log           *slog.Logger
	onTransferred func(model.Attachment)
	// clock is how the sink reads the current time when deciding whether a
	// message is still inside the platform's change window.
	clock func() time.Time
	// self is the bridge account's service id: what a quote of one of our
	// own messages names as its author, and therefore half of the canonical
	// id of everything this sink sends.
	self string
}

// NewSink builds a Sink. cfg must contain the pair serving every conversation
// the bridge may deliver to. client may be nil, in which case SetClient must
// be called before the first delivery: the connection's handler is the
// bridge, so the two are built in a cycle that has to be closed somewhere.
func NewSink(client caller, cfg config.Config) *Sink {
	return &Sink{
		client: client, cfg: cfg,
		log: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	}
}

// SetClient hands the sink the connection it sends on.
func (s *Sink) SetClient(client caller) { s.client = client }

// sendResult is what signal-cli answers a send with. The timestamp is the
// message's identity; the per-recipient results are how a partial failure in
// a group becomes visible.
type sendResult struct {
	Timestamp int64              `json:"timestamp"`
	Results   []recipientOutcome `json:"results"`
}

type recipientOutcome struct {
	RecipientAddress struct {
		Number string `json:"number"`
		UUID   string `json:"uuid"`
	} `json:"recipientAddress"`
	Type string `json:"type"`
	// Token is the challenge to solve when Signal's anti-abuse system asks
	// for a CAPTCHA before it will carry more traffic.
	Token             string `json:"token"`
	RetryAfterSeconds int64  `json:"retryAfterSeconds"`
}

// outcomeSuccess is the one per-recipient result that is not a failure.
const outcomeSuccess = "SUCCESS"

// Deliver posts msg into the conversation addressed by conversationID. When
// threadTarget is non-empty the message quotes it, which is the closest thing
// Signal has to a thread.
func (s *Sink) Deliver(ctx context.Context, msg model.Message, conversationID, threadTarget string) (string, error) {
	pair, ok := s.cfg.PairFor(model.PlatformSignal, conversationID)
	if !ok {
		return "", fmt.Errorf("no configured pair for signal conversation %q: %w",
			conversationID, bridge.ErrNotSent)
	}

	if s.client == nil {
		return "", fmt.Errorf("signal connection is not ready: %w", bridge.ErrNotSent)
	}

	params, err := s.sendParams(pair, msg, threadTarget)
	if err != nil {
		return "", err
	}
	// Files ride along with the message rather than following it: signal-cli
	// takes them as data URIs on the same send, and a separate one would
	// arrive as a second, bodyless message in the customer's conversation.
	attachments, skipped := s.encodeAttachments(ctx, msg)
	if len(attachments) > 0 {
		params["attachment"] = attachments
	}
	if len(skipped) > 0 {
		params["message"] = appendTransferNotices(params["message"].(string), msg, skipped)
	}

	result, err := s.send(ctx, params)
	if err != nil {
		var rpcErr *Error
		if errors.As(err, &rpcErr) && rpcErr.ProvablyNotSent() {
			return "", fmt.Errorf("send to %s: %w: %w", conversationID, err, bridge.ErrNotSent)
		}
		// Anything else may have been delivered before the failure was
		// raised. Reporting it as unsent would let the message be posted a
		// second time into a customer's conversation.
		return "", fmt.Errorf("send to %s: %w", conversationID, err)
	}

	s.recordOutcomes(pair.ID, result)

	if result.Timestamp == 0 {
		return "", fmt.Errorf("send to %s: signal-cli returned no timestamp", conversationID)
	}
	// The message exists in the conversation even when some recipients
	// failed: their copies are missing, not the message.
	//
	// signal-cli does not name the sender in its answer, so the id is
	// composed from the account we run as — which is exactly what a quote of
	// this message will carry as its author.
	return MessageID(s.self, "", result.Timestamp), nil
}

// send issues a send and repeats it while Signal is rate limiting.
//
// Only a rate limit is repeated, and only because signal-cli raises it when
// nothing was delivered at all. Every other failure is either permanent or of
// unknown outcome, and repeating one of those is how a customer's
// conversation gets the same message twice.
func (s *Sink) send(ctx context.Context, params map[string]any) (sendResult, error) {
	var result sendResult
	var err error
	for attempt := 0; ; attempt++ {
		result = sendResult{}
		err = s.client.Call(ctx, "send", params, &result)
		if err == nil {
			return result, nil
		}

		var rpcErr *Error
		if !errors.As(err, &rpcErr) || rpcErr.Code != CodeRateLimit || attempt >= maxSendAttempts-1 {
			return sendResult{}, err
		}
		s.log.Warn("signal is rate limiting; retrying the send",
			"attempt", attempt+1, "retry_after", rpcErr.RetryAfter())
		if waitErr := wait(ctx, capRetryAfter(rpcErr.RetryAfter())); waitErr != nil {
			return sendResult{}, err
		}
	}
}

// maxSendAttempts bounds how many times a rate-limited send is repeated. The
// delivery holds its pair's lock throughout, so retrying forever would stall
// the conversation.
const maxSendAttempts = 3

// maxRetryAfter caps a single wait for the same reason.
const maxRetryAfter = 30 * time.Second

func capRetryAfter(d time.Duration) time.Duration {
	switch {
	case d <= 0:
		return time.Second
	case d > maxRetryAfter:
		return maxRetryAfter
	default:
		return d
	}
}

// wait sleeps for d, or returns early if ctx ends first.
func wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SetSelf records the bridge account's service id. Until it is set, messages
// this sink posts are recorded under an id no incoming quote will match, so
// replies to them would not thread.
func (s *Sink) SetSelf(uuid string) { s.self = uuid }

func (s *Sink) sendParams(pair config.Pair, msg model.Message, threadTarget string) (map[string]any, error) {
	name := msg.Author.DisplayName
	if name == "" {
		name = fallbackAuthorName
	}
	body, styles, mentions := signaltext.FromMrkdwn(msg.Text, func(slackUserID string) (string, bool) {
		return s.cfg.SignalUserFor(pair.ID, slackUserID)
	})
	body, styles, mentions = signaltext.AuthorLine(name, body, styles, mentions)
	if strings.TrimSpace(body) == "" {
		return nil, fmt.Errorf("refusing to send an empty message: %w", bridge.ErrNotSent)
	}

	params := map[string]any{"message": body}
	s.addressPair(params, pair)
	if len(styles) > 0 {
		params["textStyle"] = encodeStyles(styles)
	}
	if len(mentions) > 0 {
		params["mention"] = encodeMentions(mentions)
	}

	// A quote names the message by author and send time, both of which the
	// canonical id already carries — Signal assigns nothing else to name it
	// by. The quoted text is deliberately not sent: clients render a quote
	// from their own copy, and the bridge does not keep message bodies.
	if threadTarget != "" {
		author, timestamp, ok := SplitMessageID(threadTarget)
		if !ok {
			return nil, fmt.Errorf("thread target %q is not a signal message id", threadTarget)
		}
		params["quoteTimestamp"] = timestamp
		params["quoteAuthor"] = author
	}
	return params, nil
}

// encodeStyles renders style ranges in the "start:length:STYLE" form the
// JSON-RPC interface takes.
func encodeStyles(styles []model.TextStyle) []string {
	out := make([]string, 0, len(styles))
	for _, s := range styles {
		out = append(out, fmt.Sprintf("%d:%d:%s", s.Start, s.Length, s.Style))
	}
	return out
}

// encodeMentions renders mentions in the "start:length:recipient" form the
// JSON-RPC interface takes.
func encodeMentions(mentions []model.Mention) []string {
	out := make([]string, 0, len(mentions))
	for _, m := range mentions {
		out = append(out, fmt.Sprintf("%d:%d:%s", m.Start, m.Length, m.ExternalID))
	}
	return out
}

// recordOutcomes turns per-recipient results into metrics.
//
// A CAPTCHA challenge is the one that matters operationally: Signal's
// anti-abuse system has decided this account looks like a spammer, and until
// somebody solves it by hand nothing more gets through. It is invisible
// otherwise — the send that triggered it still reports a timestamp.
func (s *Sink) recordOutcomes(pairID string, result sendResult) {
	for _, r := range result.Results {
		if r.Type == outcomeSuccess {
			continue
		}
		metrics.SignalSendFailuresTotal.WithLabelValues(pairID, r.Type).Inc()
		if r.Token != "" {
			metrics.SignalProofRequiredTotal.WithLabelValues(pairID).Inc()
		}
	}
}

// SplitMessageID takes a canonical Signal id apart into the author and the
// send time it is composed of.
//
// The author is everything before the last colon: a service id contains none,
// but a phone number fallback and any future identifier should not be able to
// break this by containing one.
func SplitMessageID(id string) (author string, timestamp int64, ok bool) {
	cut := strings.LastIndex(id, ":")
	if cut <= 0 || cut == len(id)-1 {
		return "", 0, false
	}
	timestamp, err := strconv.ParseInt(id[cut+1:], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return id[:cut], timestamp, true
}

// compile-time interface check against the bridge contract
var _ bridge.Sink = (*Sink)(nil)

// changeWindow is how long Signal lets a message be edited or deleted for
// everyone. Past it the server still accepts the request and the recipients'
// clients ignore it, so a bridge that tried anyway would report a success the
// customer never sees.
const changeWindow = 24 * time.Hour

// maxEdits is the other half of that bound: Signal accepts ten edits of a
// message and ignores the rest.
const maxEdits = 10

// MaxEdits reports the platform's limit to the core, which keeps the count.
func (s *Sink) MaxEdits() int { return maxEdits }

// Update replaces a message this sink delivered.
//
// Signal expresses an edit as a send naming the original's send time, so the
// whole body is rebuilt — author line included — rather than patched.
func (s *Sink) Update(ctx context.Context, msg model.Message, conversationID, targetID string) error {
	pair, ok := s.cfg.PairFor(model.PlatformSignal, conversationID)
	if !ok {
		return fmt.Errorf("no configured pair for signal conversation %q", conversationID)
	}
	_, timestamp, ok := SplitMessageID(targetID)
	if !ok {
		return fmt.Errorf("target %q is not a signal message id", targetID)
	}
	if outsideWindow(timestamp, s.now()) {
		return bridge.ErrOutOfWindow
	}

	params, err := s.sendParams(pair, msg, "")
	if err != nil {
		return err
	}
	params["editTimestamp"] = timestamp

	var result sendResult
	if err := s.client.Call(ctx, "send", params, &result); err != nil {
		return fmt.Errorf("edit %s: %w", targetID, err)
	}
	s.recordOutcomes(pair.ID, result)
	return nil
}

// Delete removes a message this sink delivered, for everyone.
func (s *Sink) Delete(ctx context.Context, conversationID, targetID string) error {
	pair, ok := s.cfg.PairFor(model.PlatformSignal, conversationID)
	if !ok {
		return fmt.Errorf("no configured pair for signal conversation %q", conversationID)
	}
	_, timestamp, ok := SplitMessageID(targetID)
	if !ok {
		return fmt.Errorf("target %q is not a signal message id", targetID)
	}
	if outsideWindow(timestamp, s.now()) {
		return bridge.ErrOutOfWindow
	}

	params := map[string]any{"targetTimestamp": timestamp}
	s.addressPair(params, pair)

	var result sendResult
	if err := s.client.Call(ctx, "remoteDelete", params, &result); err != nil {
		return fmt.Errorf("delete %s: %w", targetID, err)
	}
	s.recordOutcomes(pair.ID, result)
	return nil
}

// React places or withdraws a reaction on a message this sink delivered.
//
// Signal holds one reaction per sender per message, and every reaction the
// bridge mirrors comes from the same account — so what is sent here is
// whichever reaction the core decided should be the visible one.
func (s *Sink) React(ctx context.Context, r model.Reaction, conversationID, targetID string) error {
	pair, ok := s.cfg.PairFor(model.PlatformSignal, conversationID)
	if !ok {
		return fmt.Errorf("no configured pair for signal conversation %q", conversationID)
	}
	author, timestamp, ok := SplitMessageID(targetID)
	if !ok {
		return fmt.Errorf("target %q is not a signal message id", targetID)
	}

	params := map[string]any{
		"emoji":           r.Emoji,
		"targetAuthor":    author,
		"targetTimestamp": timestamp,
	}
	if r.Remove {
		params["remove"] = true
	}
	s.addressPair(params, pair)

	var result sendResult
	if err := s.client.Call(ctx, "sendReaction", params, &result); err != nil {
		return fmt.Errorf("react on %s: %w", targetID, err)
	}
	s.recordOutcomes(pair.ID, result)
	return nil
}

// addressPair names the conversation a call acts on: a group, or the single
// recipient of a 1:1 pair.
func (s *Sink) addressPair(params map[string]any, pair config.Pair) {
	if pair.Signal.GroupID != "" {
		params["groupId"] = pair.Signal.GroupID
		return
	}
	params["recipient"] = []string{pair.Signal.Recipient}
}

// outsideWindow reports whether a message sent at timestamp is too old to
// change.
func outsideWindow(timestamp int64, now time.Time) bool {
	return now.Sub(time.UnixMilli(timestamp)) > changeWindow
}

// now is a field so the window can be tested without waiting a day.
func (s *Sink) now() time.Time {
	if s.clock != nil {
		return s.clock()
	}
	return time.Now()
}

// SetClock replaces the sink's idea of the current time. Tests only.
func (s *Sink) SetClock(f func() time.Time) { s.clock = f }

// compile-time checks that this sink offers the capabilities the core looks
// for
var (
	_ bridge.Editor  = (*Sink)(nil)
	_ bridge.Deleter = (*Sink)(nil)
	_ bridge.Reactor = (*Sink)(nil)
)

// encodeAttachments reads each file and renders it as the RFC 2397 data URI
// signal-cli accepts, returning the ones that could not be carried.
//
// Nothing streams here: the JSON-RPC interface takes a file as one base64
// string, so the whole thing is in memory twice — raw and encoded — for the
// duration of the send. That is what the ceiling is for.
func (s *Sink) encodeAttachments(ctx context.Context, msg model.Message) (encoded []string, skipped []model.Attachment) {
	for _, a := range msg.Attachments {
		uri, err := s.encodeOne(ctx, a)
		if err != nil {
			s.log.Error("attachment was not transferred to Signal",
				"name", a.Name, "bytes", a.SizeBytes, "error", err)
			skipped = append(skipped, a)
			continue
		}
		encoded = append(encoded, uri)
		metrics.AttachmentBytesTotal.WithLabelValues(msg.PairID, "to_signal").Add(float64(a.SizeBytes))
		if s.onTransferred != nil {
			s.onTransferred(a)
		}
	}
	return encoded, skipped
}

func (s *Sink) encodeOne(ctx context.Context, a model.Attachment) (string, error) {
	if a.Open == nil {
		return "", errors.New("attachment carries no way to read it")
	}
	if a.SizeBytes > maxAttachmentBytes {
		return "", fmt.Errorf("file is %d bytes, over the %d byte transfer limit", a.SizeBytes, maxAttachmentBytes)
	}
	body, err := a.Open(ctx)
	if err != nil {
		return "", err
	}
	defer func() { _ = body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(body, maxAttachmentBytes+1))
	if err != nil {
		return "", fmt.Errorf("read attachment: %w", err)
	}
	if len(raw) > maxAttachmentBytes {
		return "", fmt.Errorf("file exceeds the %d byte transfer limit", maxAttachmentBytes)
	}
	if len(raw) == 0 {
		return "", errors.New("attachment was empty")
	}

	mime := a.ContentType
	if mime == "" {
		mime = "application/octet-stream"
	}
	name := a.Name
	if name == "" {
		name = "attachment"
	}
	// The filename is a data URI parameter, and signal-cli runs every
	// parameter value through a URL decoder. Passed raw, a name containing
	// "%" throws before the message is sent at all — text and file both — a
	// "+" silently becomes a space, and a comma ends the parameter section
	// early and takes part of the name into the payload.
	return fmt.Sprintf("data:%s;filename=%s;base64,%s",
		mime, url.QueryEscape(name), base64.StdEncoding.EncodeToString(raw)), nil
}

// appendTransferNotices adds a line per file that could not be carried.
//
// It is appended to the message rather than sent separately because a
// customer reading a bare "a file was not delivered" has no idea which
// conversation it belonged to. Naming the sender is what lets them reply to a
// specific person and agree another way to hand it over; a link is
// deliberately absent, since a Slack url_private is not openable by anyone on
// the Signal side.
func appendTransferNotices(body string, msg model.Message, skipped []model.Attachment) string {
	sender := msg.Author.DisplayName
	if sender == "" {
		sender = fallbackAuthorName
	}
	var sb strings.Builder
	sb.WriteString(body)
	for _, a := range skipped {
		name := a.Name
		if name == "" {
			name = "attachment"
		}
		fmt.Fprintf(&sb, "\n\n📎 %s tried to send a file: %s (%s). It was not delivered.",
			sender, name, humanBytes(a.SizeBytes))
	}
	return sb.String()
}

func humanBytes(n int64) string {
	switch {
	case n <= 0:
		return "unknown size"
	case n < 1<<20:
		return fmt.Sprintf("%d KB", max(n/1024, 1))
	default:
		return fmt.Sprintf("%d MB", n/(1<<20))
	}
}

// OnAttachmentTransferred registers a callback invoked once a file has
// reached Signal.
func (s *Sink) OnAttachmentTransferred(f func(model.Attachment)) { s.onTransferred = f }

// SetLogger replaces the sink's logger.
func (s *Sink) SetLogger(log *slog.Logger) {
	if log != nil {
		s.log = log
	}
}

// Package slack delivers canonical messages into Slack channels.
package slack

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	goslack "github.com/slack-go/slack"

	"github.com/aenix-io/slack-connectors/internal/bridge"
	"github.com/aenix-io/slack-connectors/internal/format/emojicode"
	"github.com/aenix-io/slack-connectors/internal/metrics"
	"github.com/aenix-io/slack-connectors/internal/model"
)

// maxAttachmentBytes bounds a single transfer.
//
// Slack itself allows far more, but the other side of this bridge does not:
// signal-cli carries a file as one base64 string over JSON-RPC, so nothing
// streams and the encoded form is a third larger than the file. Holding both
// directions to the same ceiling keeps the limit something an operator can
// state without asking which way the file was going.
const maxAttachmentBytes = 50 << 20

// defaultFallbackUsername is shown when the source message carries no author
// name and the caller named no source either.
const defaultFallbackUsername = "Bridge"

// Renderer converts a source platform's message into Slack mrkdwn.
//
// The sink takes one rather than calling a converter directly: the body
// arriving here is Teams HTML for one connector and, for another, Signal
// plain text whose formatting lives in ranges beside it — which is why this
// takes the whole message and not just its text.
type Renderer func(msg model.Message) string

// Sink posts canonical messages to Slack.
type Sink struct {
	client   *goslack.Client
	render   Renderer
	fallback string
	log      *slog.Logger
	// onTransferred is called after a file has been handed to Slack, so the
	// source adapter can clean up the copy it was holding.
	onTransferred func(model.Attachment)
}

// OnAttachmentTransferred registers a callback invoked once a file has
// reached Slack. The Signal adapter uses it to delete the sidecar's copy,
// which nothing else would ever remove.
func (s *Sink) OnAttachmentTransferred(f func(model.Attachment)) { s.onTransferred = f }

// SetLogger replaces the sink's logger.
func (s *Sink) SetLogger(log *slog.Logger) {
	if log != nil {
		s.log = log
	}
}

// New builds a Sink around an existing Slack client. render converts the
// source platform's body; a nil render passes the text through unchanged.
// fallbackUsername names the source platform and is shown when a message
// carries no author name — an anonymous post at least says where it came from.
func New(client *goslack.Client, render Renderer, fallbackUsername string) *Sink {
	if render == nil {
		render = func(msg model.Message) string { return msg.Text }
	}
	if fallbackUsername == "" {
		fallbackUsername = defaultFallbackUsername
	}
	return &Sink{
		client: client, render: render, fallback: fallbackUsername,
		log: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	}
}

// Deliver posts msg to channelID. When threadTarget is non-empty the message
// is posted as a reply in that thread.
//
// The message is posted with the author's name and avatar so it reads as a
// person speaking rather than as a bot announcement. This requires the
// chat:write.customize scope.
func (s *Sink) Deliver(ctx context.Context, msg model.Message, channelID, threadTarget string) (string, error) {
	username := msg.Author.DisplayName
	if username == "" {
		username = s.fallback
	}

	opts := []goslack.MsgOption{
		goslack.MsgOptionText(s.bodyOrPlaceholder(msg), false),
		goslack.MsgOptionUsername(username),
		goslack.MsgOptionDisableLinkUnfurl(),
	}
	if msg.Author.AvatarURL != "" {
		opts = append(opts, goslack.MsgOptionIconURL(msg.Author.AvatarURL))
	}
	if threadTarget != "" {
		opts = append(opts, goslack.MsgOptionTS(threadTarget))
	}

	var err error
	for attempt := 0; ; attempt++ {
		var ts string
		_, ts, err = s.client.PostMessageContext(ctx, channelID, opts...)
		if err == nil {
			s.uploadAttachments(ctx, msg, channelID, threadOrSelf(threadTarget, ts))
			return ts, nil
		}

		// Only a throttle is repeated. Slack allows roughly one message per
		// second per channel, so this is ordinary under a burst. Every other
		// refusal is permanent, and an ambiguous failure must never be
		// repeated: the post may have landed, and a retry would duplicate it.
		var throttled *goslack.RateLimitedError
		if !errors.As(err, &throttled) || attempt >= maxPostAttempts-1 {
			break
		}
		if waitErr := wait(ctx, capRetryAfter(throttled.RetryAfter)); waitErr != nil {
			break
		}
	}

	if notSent(err) {
		return "", fmt.Errorf("post message to %s: %w: %w", channelID, err, bridge.ErrNotSent)
	}
	return "", fmt.Errorf("post message to %s: %w", channelID, err)
}

// maxPostAttempts bounds how many times a throttled post is repeated. The
// delivery holds its pair's lock throughout, so retrying forever would stall
// the channel.
const maxPostAttempts = 3

// maxRetryAfter caps a single wait. An outsized value would stall the pair for
// as long as it lasts, and past this point it is better to report the failure
// and let the message be replayed.
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

// notSent reports whether an error from the Slack client proves the message
// was not posted. Only Slack answering counts: an API-level error means it
// read the request and refused, and a rate limit means it never accepted the
// post. A transport error is deliberately excluded — the post may have been
// committed and only the response lost, and treating that as a failure would
// let the message be posted twice.
func notSent(err error) bool {
	var apiErr goslack.SlackErrorResponse
	if errors.As(err, &apiErr) {
		return true
	}
	var rateErr *goslack.RateLimitedError
	if errors.As(err, &rateErr) {
		return true
	}
	var statusErr goslack.StatusCodeError
	if errors.As(err, &statusErr) {
		// Slack answered with a status it chose. 5xx is excluded: a gateway
		// can return one after the message was accepted upstream.
		return statusErr.Code < 500
	}
	return false
}

// compile-time interface check against the bridge contract
var _ bridge.Sink = (*Sink)(nil)

// emptyBodyPlaceholder stands in for a message this converter renders to
// nothing.
const emptyBodyPlaceholder = "[message had no text content]"

// bodyOrPlaceholder converts the message and guarantees a non-empty result.
//
// chat.postMessage refuses an empty body, and an empty body is reachable from
// any source content the converter has no rendering for. Failing on it is
// worse than posting a marker: the message is retried until the recovery
// cursor moves past it, and then it is gone with the reader never knowing
// anything was said.
func (s *Sink) bodyOrPlaceholder(msg model.Message) string {
	converted := s.render(msg)
	if strings.TrimSpace(converted) != "" {
		return converted
	}
	// A file sent without a caption is the ordinary case, not an empty
	// message: naming it reads as what happened, where the generic
	// placeholder reads as something having gone wrong.
	if len(msg.Attachments) > 0 {
		names := make([]string, 0, len(msg.Attachments))
		for _, a := range msg.Attachments {
			names = append(names, attachmentName(a))
		}
		return "📎 " + strings.Join(names, ", ")
	}
	return emptyBodyPlaceholder
}

// Update rewrites a message this sink posted.
//
// Whether the customised username and avatar survive an update is not
// documented by Slack; the first live run answers it. If they do not, this
// has to degrade to an annotation rather than silently re-attributing a
// person's message to the app.
func (s *Sink) Update(ctx context.Context, msg model.Message, channelID, targetID string) error {
	_, _, _, err := s.client.UpdateMessageContext(ctx, channelID, targetID,
		goslack.MsgOptionText(s.bodyOrPlaceholder(msg), false))
	if err == nil {
		return nil
	}
	// Slack refuses an edit outside the workspace's edit window, and refuses
	// to touch a message it does not consider ours. Both are reasons to
	// annotate rather than to fail: the reader still has to learn the message
	// above is no longer what the author meant.
	var apiErr goslack.SlackErrorResponse
	if errors.As(err, &apiErr) {
		switch apiErr.Err {
		case "edit_window_closed", "cant_update_message", "message_not_found":
			return bridge.ErrOutOfWindow
		}
	}
	return fmt.Errorf("update %s in %s: %w", targetID, channelID, err)
}

// Delete removes a message this sink posted.
//
// Slack's own documentation warns that chat.delete "has no accommodations for
// impersonation": a message sent impersonating another user cannot be deleted
// through it. Every message this sink posts carries a username and an avatar
// belonging to the person who wrote it on the far side, which is exactly that
// — so this is expected to refuse, and the refusal degrades into an
// annotation rather than a failure. The live run settles whether the warning
// applies to chat:write.customize as written.
func (s *Sink) Delete(ctx context.Context, channelID, targetID string) error {
	_, _, err := s.client.DeleteMessageContext(ctx, channelID, targetID)
	if err == nil {
		return nil
	}
	var apiErr goslack.SlackErrorResponse
	if errors.As(err, &apiErr) {
		switch apiErr.Err {
		case "cant_delete_message", "message_not_found":
			return bridge.ErrOutOfWindow
		}
	}
	return fmt.Errorf("delete %s in %s: %w", targetID, channelID, err)
}

// React places or withdraws a reaction.
//
// Every reaction the bridge mirrors is placed by the app, so a customer's
// three colleagues reacting the same way show up as one reaction here. That
// is a platform limit: reaction authorship cannot be customised the way
// message authorship can.
func (s *Sink) React(ctx context.Context, r model.Reaction, channelID, targetID string) error {
	name, ok := emojicode.ToSlackName(r.Emoji)
	if !ok {
		return bridge.ErrUnsupported
	}
	ref := goslack.NewRefToMessage(channelID, targetID)

	var err error
	if r.Remove {
		err = s.client.RemoveReactionContext(ctx, name, ref)
	} else {
		err = s.client.AddReactionContext(ctx, name, ref)
	}
	if err == nil {
		return nil
	}
	// Placing one that is already there, or removing one that is not, is the
	// state the caller asked for.
	var apiErr goslack.SlackErrorResponse
	if errors.As(err, &apiErr) {
		switch apiErr.Err {
		case "already_reacted", "no_reaction":
			// The state the caller asked for is the state Slack is in.
			return nil
		case "invalid_name", "message_not_found":
			return bridge.ErrUnsupported
		case "too_many_reactions", "too_many_emoji", "thread_locked":
			// Slack's own limits, not a defect here. A reaction is a signal,
			// not content: losing one is not worth an error an operator has
			// to read.
			s.log.Debug("slack declined a reaction",
				"channel", channelID, "reaction", name, "error", apiErr.Err)
			return bridge.ErrUnsupported
		}
	}
	return fmt.Errorf("react %q on %s: %w", name, targetID, err)
}

// compile-time checks that this sink offers the capabilities the core looks
// for
var (
	_ bridge.Editor  = (*Sink)(nil)
	_ bridge.Deleter = (*Sink)(nil)
	_ bridge.Reactor = (*Sink)(nil)
)

// threadOrSelf is the thread files belong in: the one the message was posted
// into, or the message itself when it started one.
func threadOrSelf(threadTarget, ts string) string {
	if threadTarget != "" {
		return threadTarget
	}
	return ts
}

// uploadAttachments transfers the message's files and posts them in the same
// thread.
//
// Failures here do not fail the delivery. The message is already in the
// channel and re-posting it to retry a file would duplicate the conversation;
// a file that did not make it is reported in its place, naming the sender so
// the reader knows who to ask.
func (s *Sink) uploadAttachments(ctx context.Context, msg model.Message, channelID, threadTS string) {
	for _, a := range msg.Attachments {
		if err := s.uploadOne(ctx, msg, a, channelID, threadTS); err != nil {
			s.log.Error("attachment was not transferred to Slack",
				"channel", channelID, "name", a.Name, "bytes", a.SizeBytes, "error", err)
			s.postTransferNotice(ctx, msg, a, channelID, threadTS)
		}
	}
}

func (s *Sink) uploadOne(ctx context.Context, msg model.Message, a model.Attachment, channelID, threadTS string) error {
	if a.Open == nil {
		return errors.New("attachment carries no way to read it")
	}
	if a.SizeBytes > maxAttachmentBytes {
		return fmt.Errorf("file is %d bytes, over the %d byte transfer limit", a.SizeBytes, maxAttachmentBytes)
	}

	body, err := a.Open(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = body.Close() }()

	// Slack needs the size up front, and a source that lied about it would
	// otherwise be discovered halfway through the upload. Reading through a
	// bounded buffer settles both questions before the transfer starts.
	buf, err := io.ReadAll(io.LimitReader(body, maxAttachmentBytes+1))
	if err != nil {
		return fmt.Errorf("read attachment: %w", err)
	}
	if len(buf) > maxAttachmentBytes {
		return fmt.Errorf("file exceeds the %d byte transfer limit", maxAttachmentBytes)
	}
	if len(buf) == 0 {
		return errors.New("attachment was empty")
	}

	// UploadFileContext is the three-step external upload — getUploadURLExternal,
	// the PUT, then completeUploadExternal. files.upload itself was retired on
	// 2025-11-12 and no longer exists.
	_, err = s.client.UploadFileContext(ctx, goslack.UploadFileParameters{
		Channel:         channelID,
		ThreadTimestamp: threadTS,
		Filename:        attachmentName(a),
		FileSize:        len(buf),
		Reader:          bytes.NewReader(buf),
	})
	if err != nil {
		return fmt.Errorf("upload %q: %w", a.Name, err)
	}
	metrics.AttachmentBytesTotal.WithLabelValues(msg.PairID, "to_slack").Add(float64(len(buf)))
	if s.onTransferred != nil {
		s.onTransferred(a)
	}
	return nil
}

// postTransferNotice stands in for a file that could not be transferred.
//
// Naming the sender is the point: without it the reader knows only that
// something failed and has nobody to ask, and with it they can reply in the
// same thread and agree another way to hand the file over. It carries no
// link, deliberately — a Signal attachment id is not something a Slack user
// can open, and a link that fails on click reads as available.
func (s *Sink) postTransferNotice(ctx context.Context, msg model.Message, a model.Attachment, channelID, threadTS string) {
	sender := msg.Author.DisplayName
	if sender == "" {
		sender = s.fallback
	}
	name := attachmentName(a)
	notice := fmt.Sprintf("📎 %s tried to send a file: `%s` (%s). It was not delivered.",
		sender, name, humanBytes(a.SizeBytes))

	opts := []goslack.MsgOption{
		goslack.MsgOptionText(notice, false),
		goslack.MsgOptionUsername(sender),
		goslack.MsgOptionDisableLinkUnfurl(),
	}
	if threadTS != "" {
		opts = append(opts, goslack.MsgOptionTS(threadTS))
	}
	if _, _, err := s.client.PostMessageContext(ctx, channelID, opts...); err != nil {
		s.log.Error("could not post the failed-transfer notice",
			"channel", channelID, "name", name, "error", err)
	}
}

func attachmentName(a model.Attachment) string {
	if a.Name != "" {
		return a.Name
	}
	return "attachment"
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

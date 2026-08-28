package slack

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aenix-io/slack-connectors/internal/config"
	"github.com/aenix-io/slack-connectors/internal/format/emojicode"
	"github.com/aenix-io/slack-connectors/internal/model"
)

// maxEventBody caps the request body we are willing to read.
const maxEventBody = 4 << 20 // 4 MiB

// maxConcurrentProcessing bounds how many events are dispatched at once, so a
// burst cannot exhaust memory.
const maxConcurrentProcessing = 32

// maxChallenge caps the handshake value we are willing to echo back. Slack's
// challenges are short; without an explicit bound the only limit is an
// incidental server default that this code neither chose nor documents.
const maxChallenge = 2048

// maxTimestampSkew is how far a request timestamp may be from now before the
// request is treated as a replay.
const maxTimestampSkew = 5 * time.Minute

// slackFile is a file attached to a message event.
type slackFile struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Title      string `json:"title"`
	Mimetype   string `json:"mimetype"`
	Size       int64  `json:"size"`
	URLPrivate string `json:"url_private"`
}

// Downloader fetches a Slack-hosted file. It exists as a dependency rather
// than a method because url_private needs the bot token in a header, which is
// the one thing the Events handler does not otherwise hold.
type Downloader func(ctx context.Context, url string) (io.ReadCloser, error)

// Handler serves the Slack Events API endpoint.
type Handler struct {
	signingSecret    string
	cfg              config.Config
	users            UserDirectory
	dispatch         func(context.Context, model.Message) error
	dispatchReaction func(context.Context, model.Reaction) error
	download         Downloader
	log              *slog.Logger

	// inFlight tracks asynchronous processing so shutdown can wait for it.
	inFlight sync.WaitGroup
	// sem bounds concurrent processing.
	sem chan struct{}
}

// NewHandler builds the events endpoint. users resolves author names, which
// Slack's event payload carries only as an opaque id. dispatch receives every
// accepted message and must not block the caller for long.
func NewHandler(
	signingSecret string,
	cfg config.Config,
	users UserDirectory,
	dispatch func(context.Context, model.Message) error,
	log *slog.Logger,
) *Handler {
	return &Handler{
		signingSecret: signingSecret,
		cfg:           cfg,
		users:         users,
		dispatch:      dispatch,
		log:           log,
		sem:           make(chan struct{}, maxConcurrentProcessing),
	}
}

// OnDownload registers how attached files are fetched. Without one, files are
// described but not carried — which is what a deployment without file
// transfer gets.
func (h *Handler) OnDownload(d Downloader) { h.download = d }

// OnReaction registers the dispatcher for reactions. Without one, reaction
// events are ignored — which is how a deployment turns them off.
func (h *Handler) OnReaction(d func(context.Context, model.Reaction) error) {
	h.dispatchReaction = d
}

// Drain waits for in-flight event processing to finish, or until ctx expires.
// Without it, shutdown discards events that were acknowledged with 200 but not
// yet delivered — the bridge would have told Slack it took responsibility for
// a message and then dropped it.
func (h *Handler) Drain(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		h.inFlight.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// eventPayload is the Events API envelope, with the one event kind we consume
// inlined.
type eventPayload struct {
	Type      string `json:"type"`
	Challenge string `json:"challenge"`
	Event     struct {
		Type     string `json:"type"`
		Subtype  string `json:"subtype"`
		Channel  string `json:"channel"`
		User     string `json:"user"`
		BotID    string `json:"bot_id"`
		Text     string `json:"text"`
		TS       string `json:"ts"`
		ThreadTS string `json:"thread_ts"`

		Files []slackFile `json:"files"`

		// An edit arrives as the new message nested inside the event, and a
		// delete names only the timestamp it removed.
		Message *struct {
			Type     string `json:"type"`
			User     string `json:"user"`
			BotID    string `json:"bot_id"`
			Text     string `json:"text"`
			TS       string `json:"ts"`
			ThreadTS string `json:"thread_ts"`
			Edited   *struct {
				User string `json:"user"`
				TS   string `json:"ts"`
			} `json:"edited"`
		} `json:"message"`
		DeletedTS string `json:"deleted_ts"`

		// A reaction names the message it sits on rather than carrying one.
		Reaction string `json:"reaction"`
		ItemUser string `json:"item_user"`
		Item     *struct {
			Type    string `json:"type"`
			Channel string `json:"channel"`
			TS      string `json:"ts"`
		} `json:"item"`
	} `json:"event"`
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Content-Type-Options", "nosniff")

	// Claim a processing slot before reading the body, so a burst cannot
	// leave every accepted request pinning up to maxEventBody bytes while it
	// waits. Under saturation a 503 tells Slack to retry, so nothing is lost;
	// answering 200 and queueing without bound would promise delivery we
	// cannot keep.
	select {
	case h.sem <- struct{}{}:
	default:
		h.log.Warn("slack event processing saturated; asking Slack to retry")
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	release := func() { <-h.sem }

	body, err := io.ReadAll(io.LimitReader(r.Body, maxEventBody))
	if err != nil {
		release()
		h.log.Error("read slack event body", "error", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Verified over the bytes exactly as received: decoding and re-encoding
	// first would change them and the signature would never match.
	if err := h.verify(r.Header, body); err != nil {
		release()
		h.log.Warn("rejecting slack request", "error", err)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	var payload eventPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		release()
		h.log.Error("decode slack event payload", "error", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// The URL verification handshake is the one response Slack expects to
	// carry a body.
	if payload.Type == "url_verification" {
		release()
		h.writeChallenge(w, payload.Challenge)
		return
	}

	// Slack retries anything not acknowledged within three seconds, so
	// acknowledge before doing any work.
	w.WriteHeader(http.StatusOK)

	// Processing continues after the response, so it must survive the request
	// context but still be waited on at shutdown.
	ctx := context.WithoutCancel(r.Context())
	h.inFlight.Add(1)
	go func() {
		defer h.inFlight.Done()
		defer release()
		h.process(ctx, payload)
	}()
}

func (h *Handler) writeChallenge(w http.ResponseWriter, challenge string) {
	if len(challenge) > maxChallenge {
		h.log.Warn("rejecting oversized challenge", "length", len(challenge))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	//nolint:gosec // Echoing the challenge verbatim is the documented Slack
	// handshake. Content-Type is text/plain and nosniff is set above.
	if _, err := io.WriteString(w, challenge); err != nil {
		h.log.Error("write challenge response", "error", err)
	}
}

// verify checks Slack's request signature and rejects replays.
func (h *Handler) verify(header http.Header, body []byte) error {
	stamp := header.Get("X-Slack-Request-Timestamp")
	seconds, err := strconv.ParseInt(stamp, 10, 64)
	if err != nil {
		return errors.New("malformed request timestamp")
	}
	if skew := time.Since(time.Unix(seconds, 0)); skew > maxTimestampSkew || skew < -maxTimestampSkew {
		return errors.New("request timestamp outside the accepted window")
	}

	mac := hmac.New(sha256.New, []byte(h.signingSecret))
	mac.Write([]byte("v0:" + stamp + ":"))
	mac.Write(body)
	expected := "v0=" + hex.EncodeToString(mac.Sum(nil))

	if !hmac.Equal([]byte(expected), []byte(header.Get("X-Slack-Signature"))) {
		return errors.New("signature mismatch")
	}
	return nil
}

func (h *Handler) process(ctx context.Context, payload eventPayload) {
	if payload.Type != "event_callback" {
		return
	}
	switch payload.Event.Type {
	case "message":
		h.processMessage(ctx, payload)
	case "reaction_added", "reaction_removed":
		h.processReaction(ctx, payload)
	}
}

// processReaction mirrors a reaction placed on a message in a paired channel.
func (h *Handler) processReaction(ctx context.Context, payload eventPayload) {
	if h.dispatchReaction == nil {
		return
	}
	ev := payload.Event
	if ev.Item == nil || ev.Item.Type != "message" {
		// Reactions on files and file comments name something this bridge
		// never mirrored.
		return
	}
	pair, ok := h.cfg.PairFor(model.PlatformSlack, ev.Item.Channel)
	if !ok {
		return
	}
	// Slack names a reaction; the canonical form is the character itself.
	// Translating at the boundary is what keeps the core free of one
	// platform's spelling — and it is also what makes loop prevention work,
	// since the record of a mirrored reaction is keyed on the emoji and a
	// shortcode would never match the character it stands for.
	//
	// A custom emoji is uploaded to one workspace and has no character at
	// all. It stops here rather than travelling as a name Signal would
	// refuse.
	emoji, ok := emojicode.ToUnicode(ev.Reaction)
	if !ok {
		h.log.Debug("dropping a reaction with no unicode form",
			"channel", ev.Item.Channel, "reaction", ev.Reaction)
		return
	}

	// A reaction placed by this app is one it mirrored a moment ago. Slack
	// reports the app's own user id here rather than a bot id, so the
	// delivery record is what actually catches the echo; this is layer one.
	var displayName string
	if ev.User != "" {
		displayName = h.users.DisplayName(ctx, ev.User)
	}
	_ = h.dispatchReaction(ctx, model.Reaction{
		Platform: model.PlatformSlack,
		PairID:   pair.ID,
		Author:   model.Author{ExternalID: ev.User, DisplayName: displayName},
		Emoji:    emoji,
		TargetID: ev.Item.TS,
		Remove:   ev.Type == "reaction_removed",
	})
}

func (h *Handler) processMessage(ctx context.Context, payload eventPayload) {
	ev := payload.Event

	switch ev.Subtype {
	case "message_changed":
		h.processEdit(ctx, ev.Channel, payload)
		return
	case "message_deleted":
		h.processDelete(ctx, ev.Channel, ev.DeletedTS)
		return
	}

	// Loop prevention layer one: a post carrying a bot identity is ours
	// coming back. It is still dispatched so the bridge can record the drop.
	fromBridge := ev.BotID != "" || ev.Subtype == "bot_message"

	// Every other subtype is a join, a leave, a topic change, an edit or a
	// delete. Phase 2 mirrors new messages only, matching the Teams side,
	// which subscribes to created events alone.
	//
	// file_share is the exception: it is a person typing a sentence and
	// attaching something to it. The attachment is Phase 3, but dropping the
	// sentence with it would silently lose conversation. A file shared with
	// no text has nothing left to mirror once the file is set aside.
	if ev.Subtype != "" && !fromBridge && !worthMirroring(ev.Subtype, ev.Text, len(ev.Files)) {
		h.log.Debug("skipping subtyped message event", "subtype", ev.Subtype)
		return
	}

	pair, ok := h.cfg.PairFor(model.PlatformSlack, ev.Channel)
	if !ok {
		h.log.Debug("ignoring event for an unconfigured channel", "channel_id", ev.Channel)
		return
	}

	// Slack's payload names the author only by id, and Teams cannot
	// impersonate, so the name has to be resolved here or the message arrives
	// attributed to nobody. This runs after the acknowledgement, on the
	// detached goroutine, so the three-second budget is untouched. It is
	// skipped for a post the bridge is about to drop anyway.
	var displayName string
	if !fromBridge && ev.User != "" {
		displayName = h.users.DisplayName(ctx, ev.User)
	}

	// The outcome is the caller's to record; this endpoint already
	// acknowledged the event and has nothing left to report to Slack either
	// way.
	attachments := h.attachments(ev.Files)
	_ = h.dispatch(ctx, model.Message{
		Platform: model.PlatformSlack,
		ID:       ev.TS,
		// Slack sets thread_ts equal to ts on a thread root; treating that
		// as a parent would make every root a reply to itself.
		ParentID:     threadParent(ev.TS, ev.ThreadTS),
		ThreadRootID: threadParent(ev.TS, ev.ThreadTS),
		PairID:       pair.ID,
		Author: model.Author{
			ExternalID:  ev.User,
			DisplayName: displayName,
		},
		Text:        ev.Text,
		Kind:        model.EventCreated,
		Timestamp:   parseSlackTS(ev.TS),
		Attachments: attachments,
		FromBridge:  fromBridge,
	})
}

// worthMirroring reports whether a subtyped event carries something a reader
// on the far side would want.
//
// file_share is the one subtype that does: a person attaching a file, with or
// without a sentence to go with it. Everything else — joins, leaves, topic
// changes — is channel furniture. A file_share with neither text nor files is
// nothing at all, and mirroring it would post a body consisting of the
// author's name.
func worthMirroring(subtype, text string, files int) bool {
	if subtype != "file_share" {
		return false
	}
	return strings.TrimSpace(text) != "" || files > 0
}

func threadParent(ts, threadTS string) string {
	if threadTS == ts {
		return ""
	}
	return threadTS
}

// parseSlackTS converts a Slack timestamp such as 1700000000.000100 — seconds
// and microseconds — into a time. An unparseable value yields the zero time
// rather than an error: the timestamp is metadata, not something worth
// dropping a message over.
func parseSlackTS(ts string) time.Time {
	secondsPart, fraction, _ := strings.Cut(ts, ".")
	seconds, err := strconv.ParseInt(secondsPart, 10, 64)
	if err != nil {
		return time.Time{}
	}
	var micros int64
	if fraction != "" {
		if micros, err = strconv.ParseInt(fraction, 10, 64); err != nil {
			micros = 0
		}
	}
	return time.Unix(seconds, micros*int64(time.Microsecond))
}

// processEdit mirrors a correction to a message already delivered.
//
// The event carries the whole new message rather than a patch, and its own
// edit timestamp — which becomes the event's id, so that two corrections to
// one message are two events instead of one the idempotency gate mistakes for
// a repeat.
func (h *Handler) processEdit(ctx context.Context, channelID string, payload eventPayload) {
	ev := payload.Event
	if ev.Message == nil || ev.Message.TS == "" {
		return
	}
	pair, ok := h.cfg.PairFor(model.PlatformSlack, channelID)
	if !ok {
		return
	}
	if ev.Message.BotID != "" {
		// Our own mirror being corrected, which happens when the far side
		// edits and we apply it here.
		return
	}

	editID := ev.Message.TS
	if ev.Message.Edited != nil && ev.Message.Edited.TS != "" {
		editID = ev.Message.Edited.TS
	}
	var displayName string
	if ev.Message.User != "" {
		displayName = h.users.DisplayName(ctx, ev.Message.User)
	}
	_ = h.dispatch(ctx, model.Message{
		Platform:  model.PlatformSlack,
		ID:        editID,
		EditsID:   ev.Message.TS,
		PairID:    pair.ID,
		Author:    model.Author{ExternalID: ev.Message.User, DisplayName: displayName},
		Text:      ev.Message.Text,
		Kind:      model.EventEdited,
		Timestamp: parseSlackTS(editID),
	})
}

// processDelete mirrors a deletion. A delete has no version of its own, so
// the deleted message's timestamp is also the event's id.
func (h *Handler) processDelete(ctx context.Context, channelID, deletedTS string) {
	if deletedTS == "" {
		return
	}
	pair, ok := h.cfg.PairFor(model.PlatformSlack, channelID)
	if !ok {
		return
	}
	_ = h.dispatch(ctx, model.Message{
		Platform:  model.PlatformSlack,
		ID:        deletedTS,
		PairID:    pair.ID,
		Kind:      model.EventDeleted,
		Timestamp: parseSlackTS(deletedTS),
	})
}

// attachments describes the files on an event and gives each a way to be
// read.
//
// url_private is not a public link: it answers with a login page unless the
// request carries the bot token, which is why the download lives here rather
// than being handed to the far side as a URL.
func (h *Handler) attachments(files []slackFile) []model.Attachment {
	if len(files) == 0 || h.download == nil {
		return nil
	}
	out := make([]model.Attachment, 0, len(files))
	for _, f := range files {
		if f.URLPrivate == "" {
			continue
		}
		url := f.URLPrivate
		out = append(out, model.Attachment{
			Name:        firstNonEmpty(f.Name, f.Title),
			ContentType: f.Mimetype,
			SizeBytes:   f.Size,
			SourceURL:   url,
			Open: func(ctx context.Context) (io.ReadCloser, error) {
				return h.download(ctx, url)
			},
		})
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

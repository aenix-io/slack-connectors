package signal

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/aenix-io/slack-connectors/internal/config"
	"github.com/aenix-io/slack-connectors/internal/metrics"
	"github.com/aenix-io/slack-connectors/internal/model"
)

// receiveMethod is the notification signal-cli sends for every incoming
// envelope.
const receiveMethod = "receive"

// receiveParams covers both shapes signal-cli uses. A daemon bound to a
// single account sends the envelope directly; a subscription wraps it in a
// result alongside the account it belongs to.
type receiveParams struct {
	Envelope *envelope `json:"envelope"`
	Account  string    `json:"account"`
	Result   *struct {
		Envelope *envelope `json:"envelope"`
		Account  string    `json:"account"`
	} `json:"result"`
}

func (p receiveParams) envelope() *envelope {
	if p.Result != nil {
		return p.Result.Envelope
	}
	return p.Envelope
}

type envelope struct {
	SourceNumber string       `json:"sourceNumber"`
	SourceUUID   string       `json:"sourceUuid"`
	SourceName   string       `json:"sourceName"`
	SourceDevice int          `json:"sourceDevice"`
	Timestamp    int64        `json:"timestamp"`
	DataMessage  *dataMessage `json:"dataMessage"`
	EditMessage  *editMessage `json:"editMessage"`
	SyncMessage  *syncMessage `json:"syncMessage"`
	// Receipts, typing indicators, calls and stories arrive on this same
	// stream and are named here only so their absence from the switch below
	// reads as a decision rather than an oversight.
	ReceiptMessage json.RawMessage `json:"receiptMessage"`
	TypingMessage  json.RawMessage `json:"typingMessage"`
	CallMessage    json.RawMessage `json:"callMessage"`
	StoryMessage   json.RawMessage `json:"storyMessage"`
}

type dataMessage struct {
	Timestamp        int64        `json:"timestamp"`
	Message          string       `json:"message"`
	ExpiresInSeconds int          `json:"expiresInSeconds"`
	ViewOnce         bool         `json:"viewOnce"`
	Quote            *quote       `json:"quote"`
	Reaction         *reaction    `json:"reaction"`
	RemoteDelete     *remoteDel   `json:"remoteDelete"`
	Mentions         []mention    `json:"mentions"`
	TextStyles       []textStyle  `json:"textStyles"`
	Attachments      []attachment `json:"attachments"`
	GroupInfo        *groupInfo   `json:"groupInfo"`
}

type editMessage struct {
	TargetSentTimestamp int64        `json:"targetSentTimestamp"`
	DataMessage         *dataMessage `json:"dataMessage"`
}

// syncMessage carries what another device on our own account did. It matters
// because the bridge number may also be linked to a phone: a message sent
// from there is a real message in the conversation, and the only way we hear
// about it is here.
type syncMessage struct {
	SentMessage *struct {
		DestinationNumber string `json:"destinationNumber"`
		DestinationUUID   string `json:"destinationUuid"`
		// The data message is unwrapped into this object by signal-cli, so
		// the fields sit alongside the destination rather than under a key.
		dataMessage
	} `json:"sentMessage"`
}

type quote struct {
	ID          int64  `json:"id"`
	AuthorUUID  string `json:"authorUuid"`
	AuthorNumb  string `json:"authorNumber"`
	Text        string `json:"text"`
	Attachments []struct {
		ContentType string `json:"contentType"`
		Filename    string `json:"filename"`
	} `json:"attachments"`
}

type reaction struct {
	Emoji               string `json:"emoji"`
	TargetAuthorUUID    string `json:"targetAuthorUuid"`
	TargetAuthorNumber  string `json:"targetAuthorNumber"`
	TargetSentTimestamp int64  `json:"targetSentTimestamp"`
	IsRemove            bool   `json:"isRemove"`
}

type remoteDel struct {
	Timestamp int64 `json:"timestamp"`
}

type mention struct {
	UUID   string `json:"uuid"`
	Number string `json:"number"`
	Name   string `json:"name"`
	Start  int    `json:"start"`
	Length int    `json:"length"`
}

type textStyle struct {
	Style  string `json:"style"`
	Start  int    `json:"start"`
	Length int    `json:"length"`
}

type attachment struct {
	ContentType string `json:"contentType"`
	Filename    string `json:"filename"`
	ID          string `json:"id"`
	Size        int64  `json:"size"`
}

type groupInfo struct {
	GroupID   string `json:"groupId"`
	GroupName string `json:"groupName"`
	Type      string `json:"type"`
}

// Dispatcher receives every canonical message the stream produces.
type Dispatcher func(ctx context.Context, msg model.Message) error

// ReactionDispatcher receives every canonical reaction the stream produces.
type ReactionDispatcher func(ctx context.Context, r model.Reaction) error

// Stream turns signal-cli notifications into canonical messages.
//
// It is the NotificationHandler the Client runs on its read loop, so
// everything here happens before the next envelope is read — the property the
// delivery guarantee rests on.
type Stream struct {
	cfg              config.Config
	dispatch         Dispatcher
	dispatchReaction ReactionDispatcher
	log              *slog.Logger
	// selfUUID and selfNumber identify the bridge account, so a message it
	// sent from another linked device is attributed rather than mistaken for
	// a stranger.
	selfUUID   string
	selfNumber string
	// client fetches attachment contents; attachmentsDir is where the sidecar
	// wrote them, so they can be removed once transferred.
	client         *Client
	attachmentsDir string
}

// NewStream builds a Stream. selfUUID and selfNumber name the bridge account
// and may be empty when they are not known yet; they only affect attribution.
func NewStream(cfg config.Config, dispatch Dispatcher, selfUUID, selfNumber string, log *slog.Logger) *Stream {
	return &Stream{
		cfg:        cfg,
		dispatch:   dispatch,
		log:        log,
		selfUUID:   selfUUID,
		selfNumber: selfNumber,
	}
}

// OnReaction registers the dispatcher for reactions. Without one they are
// dropped, which is how a deployment turns the Slack-to-Signal direction off.
func (s *Stream) OnReaction(d ReactionDispatcher) { s.dispatchReaction = d }

// SetSelf records the identity of the bridge account.
//
// It is separate from the constructor because the service id is not known
// until signal-cli is asked, and asking requires the connection the stream is
// already the handler for. Attribution of sync transcripts depends on it;
// nothing else does.
func (s *Stream) SetSelf(uuid, number string) {
	s.selfUUID = uuid
	s.selfNumber = number
}

// Subscribe asks signal-cli to start delivering messages.
//
// The daemon runs with --receive-mode=manual, so until this call it does not
// drain the Signal server's queue at all. That is what makes a restart safe:
// messages sent while the bridge was down are still waiting on the server
// rather than acknowledged into a socket nobody was reading.
func (s *Stream) Subscribe(ctx context.Context, c *Client) error {
	if err := c.Call(ctx, "subscribeReceive", nil, nil); err != nil {
		return fmt.Errorf("subscribe to signal messages: %w", err)
	}
	s.log.Info("subscribed to signal messages")
	return nil
}

// Handle implements NotificationHandler.
func (s *Stream) Handle(ctx context.Context, method string, params json.RawMessage) {
	if method != receiveMethod {
		s.log.Debug("ignoring signal notification", "method", method)
		return
	}
	var p receiveParams
	if err := json.Unmarshal(params, &p); err != nil {
		s.log.Error("decode signal receive notification", "error", err)
		return
	}
	env := p.envelope()
	if env == nil {
		s.log.Debug("signal notification carried no envelope")
		return
	}
	s.handleEnvelope(ctx, env)
}

func (s *Stream) handleEnvelope(ctx context.Context, env *envelope) {
	switch {
	case env.DataMessage != nil:
		s.handleData(ctx, env, env.DataMessage, env.SourceUUID, env.SourceNumber, env.SourceName)
	case env.SyncMessage != nil && env.SyncMessage.SentMessage != nil:
		// Sent from another device on the bridge account. It is attributed to
		// us, and loop prevention still rejects our own mirrored posts by
		// their delivery record rather than by author.
		sent := env.SyncMessage.SentMessage
		data := sent.dataMessage
		s.handleData(ctx, env, &data, s.selfUUID, s.selfNumber, env.SourceName)
	case env.EditMessage != nil && env.EditMessage.DataMessage != nil:
		s.handleEdit(ctx, env, env.EditMessage)
	default:
		s.log.Debug("ignoring a signal envelope with nothing to mirror",
			"source_device", env.SourceDevice)
	}
}

func (s *Stream) handleData(ctx context.Context, env *envelope, data *dataMessage, authorUUID, authorNumber, authorName string) {
	pair, ok := s.pairFor(data, authorUUID, authorNumber)
	if !ok {
		// The bridge number is reachable by anyone who knows it, so an
		// unconfigured conversation is expected traffic, not an error.
		s.log.Debug("ignoring a signal message for an unconfigured conversation")
		return
	}

	switch {
	case data.Reaction != nil:
		s.handleReaction(ctx, pair, data.Reaction, authorUUID, authorNumber, authorName, data.Timestamp)
		return
	case data.RemoteDelete != nil:
		s.handleDelete(ctx, pair, data.RemoteDelete, authorUUID, authorNumber, data.Timestamp)
		return
	}

	if data.ExpiresInSeconds > 0 {
		// The Slack copy does not disappear. Nobody should learn that from
		// the customer.
		metrics.ExpiringMessagesObservedTotal.WithLabelValues(pair.ID).Inc()
		s.log.Warn("signal conversation has a disappearing-message timer; the mirrored copy will not expire",
			"pair", pair.ID, "expires_in_seconds", data.ExpiresInSeconds)
	}

	text := data.Message
	if strings.TrimSpace(text) == "" && len(data.Attachments) == 0 {
		s.log.Debug("ignoring a signal message with no body and no attachment", "pair", pair.ID)
		return
	}

	msg := model.Message{
		Platform:  model.PlatformSignal,
		ID:        MessageID(authorUUID, authorNumber, data.Timestamp),
		ParentID:  quoteParent(data.Quote),
		PairID:    pair.ID,
		Author:    model.Author{ExternalID: firstNonEmpty(authorUUID, authorNumber), DisplayName: authorName},
		Text:      text,
		Kind:      model.EventCreated,
		Timestamp: time.UnixMilli(data.Timestamp),
	}
	msg.Mentions = toMentions(data.Mentions)
	msg.TextStyles = toTextStyles(data.TextStyles)
	msg.Attachments = s.toAttachments(data, firstNonEmpty(authorUUID, authorNumber))

	if err := s.dispatch(ctx, msg); err != nil {
		// The bridge has already logged and counted this. Returning here is
		// what applies back-pressure: the read loop does not continue until
		// this call is done either way.
		s.log.Debug("signal message was not delivered", "pair", pair.ID, "id", msg.ID)

		// A failure while the process is stopping is the one gap in the
		// delivery guarantee: signal-cli acknowledged this envelope to the
		// Signal server before handing it over, and nothing replays it. It is
		// counted rather than left to a log line, because the alert on it is
		// how anyone finds out a message was lost.
		if ctx.Err() != nil {
			metrics.SignalInflightLostTotal.WithLabelValues(pair.ID).Inc()
			s.log.Error("a received message was not delivered before shutdown; it will not be retried",
				"pair", pair.ID, "id", msg.ID)
		}
	}
}

// pairFor resolves the conversation a message belongs to: its group when it
// has one, otherwise the 1:1 conversation with its author.
func (s *Stream) pairFor(data *dataMessage, authorUUID, authorNumber string) (config.Pair, bool) {
	if data.GroupInfo != nil && data.GroupInfo.GroupID != "" {
		return s.cfg.PairFor(model.PlatformSignal, data.GroupInfo.GroupID)
	}
	// A 1:1 pair may be configured by either identifier, and which one an
	// operator had to hand is not something to make them think about.
	for _, id := range []string{authorUUID, authorNumber} {
		if id == "" {
			continue
		}
		if pair, ok := s.cfg.PairFor(model.PlatformSignal, id); ok {
			return pair, true
		}
	}
	return config.Pair{}, false
}

// MessageID is the canonical id of a Signal message.
//
// Signal assigns no message ids: a message is identified by its author and
// the millisecond it was sent, and every reference to it — quotes, edits,
// deletes, reactions — names both. The pair is therefore what the store keys
// on, and it is composed here so that exactly one function decides the shape.
func MessageID(authorUUID, authorNumber string, timestamp int64) string {
	return fmt.Sprintf("%s:%d", firstNonEmpty(authorUUID, authorNumber), timestamp)
}

func quoteParent(q *quote) string {
	if q == nil || q.ID == 0 {
		return ""
	}
	return MessageID(q.AuthorUUID, q.AuthorNumb, q.ID)
}

func toMentions(in []mention) []model.Mention {
	if len(in) == 0 {
		return nil
	}
	out := make([]model.Mention, 0, len(in))
	for _, m := range in {
		out = append(out, model.Mention{
			ExternalID:  firstNonEmpty(m.UUID, m.Number),
			DisplayName: m.Name,
			Start:       m.Start,
			Length:      m.Length,
		})
	}
	return out
}

func toTextStyles(in []textStyle) []model.TextStyle {
	if len(in) == 0 {
		return nil
	}
	out := make([]model.TextStyle, 0, len(in))
	for _, s := range in {
		out = append(out, model.TextStyle{
			Style:  model.TextStyleKind(s.Style),
			Start:  s.Start,
			Length: s.Length,
		})
	}
	return out
}

// toAttachments describes each file and gives it a way to be read. The
// conversation it arrived in is captured because getAttachment needs to know
// which conversation to look in, not just the id.
func (s *Stream) toAttachments(data *dataMessage, sender string) []model.Attachment {
	if len(data.Attachments) == 0 {
		return nil
	}
	conversation, isGroup := conversationOf(data, sender)
	out := make([]model.Attachment, 0, len(data.Attachments))
	for _, a := range data.Attachments {
		out = append(out, model.Attachment{
			Name:        a.Filename,
			ContentType: a.ContentType,
			SizeBytes:   a.Size,
			// Not a URL: the file is fetched from the sidecar by this id.
			SourceURL: a.ID,
			Open:      s.attachmentReader(a.ID, conversation, isGroup),
		})
	}
	return out
}

// conversationOf names where a message arrived, in the form getAttachment
// expects: a group id, or the sender of a 1:1 conversation. signal-cli looks
// the file up per conversation, so an id alone is not enough to find it.
func conversationOf(data *dataMessage, sender string) (string, bool) {
	if data.GroupInfo != nil && data.GroupInfo.GroupID != "" {
		return data.GroupInfo.GroupID, true
	}
	return sender, false
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// handleEdit turns an edited message into a change to the one it replaces.
//
// The edit carries its own send time, which becomes its id: two corrections
// to the same message are two events, and an id shared with the original
// would make the second look like a repeat of the first.
func (s *Stream) handleEdit(ctx context.Context, env *envelope, edit *editMessage) {
	data := edit.DataMessage
	pair, ok := s.pairFor(data, env.SourceUUID, env.SourceNumber)
	if !ok {
		s.log.Debug("ignoring a signal edit for an unconfigured conversation")
		return
	}
	msg := model.Message{
		Platform:  model.PlatformSignal,
		ID:        MessageID(env.SourceUUID, env.SourceNumber, data.Timestamp),
		EditsID:   MessageID(env.SourceUUID, env.SourceNumber, edit.TargetSentTimestamp),
		PairID:    pair.ID,
		Author:    model.Author{ExternalID: firstNonEmpty(env.SourceUUID, env.SourceNumber), DisplayName: env.SourceName},
		Text:      data.Message,
		Kind:      model.EventEdited,
		Timestamp: time.UnixMilli(data.Timestamp),
	}
	msg.Mentions = toMentions(data.Mentions)
	msg.TextStyles = toTextStyles(data.TextStyles)

	if err := s.dispatch(ctx, msg); err != nil {
		s.log.Debug("signal edit was not applied", "pair", pair.ID, "id", msg.ID)
	}
}

// handleDelete turns a remote delete into a change to the message it names.
// A delete has no version of its own, so the original's id is also the
// event's: deleting the same message twice is the same event.
func (s *Stream) handleDelete(
	ctx context.Context, pair config.Pair, del *remoteDel, authorUUID, authorNumber string, timestamp int64,
) {
	msg := model.Message{
		Platform:  model.PlatformSignal,
		ID:        MessageID(authorUUID, authorNumber, del.Timestamp),
		PairID:    pair.ID,
		Author:    model.Author{ExternalID: firstNonEmpty(authorUUID, authorNumber)},
		Kind:      model.EventDeleted,
		Timestamp: time.UnixMilli(timestamp),
	}
	if err := s.dispatch(ctx, msg); err != nil {
		s.log.Debug("signal delete was not applied", "pair", pair.ID, "id", msg.ID)
	}
}

// handleReaction turns a Signal reaction into the canonical form. The message
// reacted to is named by its author and send time, which is exactly what our
// composite id is.
func (s *Stream) handleReaction(
	ctx context.Context,
	pair config.Pair,
	r *reaction,
	authorUUID, authorNumber, authorName string,
	timestamp int64,
) {
	if s.dispatchReaction == nil {
		s.log.Debug("no reaction dispatcher registered; dropping", "pair", pair.ID)
		return
	}
	canonical := model.Reaction{
		Platform: model.PlatformSignal,
		PairID:   pair.ID,
		Author: model.Author{
			ExternalID:  firstNonEmpty(authorUUID, authorNumber),
			DisplayName: authorName,
		},
		Emoji:     r.Emoji,
		TargetID:  MessageID(r.TargetAuthorUUID, r.TargetAuthorNumber, r.TargetSentTimestamp),
		Remove:    r.IsRemove,
		Timestamp: time.UnixMilli(timestamp),
	}
	if err := s.dispatchReaction(ctx, canonical); err != nil {
		s.log.Debug("signal reaction was not mirrored", "pair", pair.ID, "target", canonical.TargetID)
	}
}

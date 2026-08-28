package signal

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/aenix-io/slack-connectors/internal/config"
	"github.com/aenix-io/slack-connectors/internal/model"
)

const (
	adaUUID   = "8f5ac0de-0000-4000-8000-000000000001"
	selfUUID  = "8f5ac0de-0000-4000-8000-0000000000ff"
	selfPhone = "+491700000000"
	groupID   = "Ci8KIExhbXBz"
)

func streamConfig() config.Config {
	return config.Config{Pairs: []config.Pair{
		{
			ID:     "acme-group",
			Signal: &config.SignalEndpoint{GroupID: groupID},
			Slack:  &config.SlackEndpoint{ChannelID: "C0GROUP"},
		},
		{
			ID:     "ada-direct",
			Signal: &config.SignalEndpoint{Recipient: adaUUID},
			Slack:  &config.SlackEndpoint{ChannelID: "C0DIRECT"},
		},
		{
			ID:     "grace-direct",
			Signal: &config.SignalEndpoint{Recipient: "+491701111111"},
			Slack:  &config.SlackEndpoint{ChannelID: "C0PHONE"},
		},
	}}
}

// collector records what the stream dispatched.
type collector struct {
	mu       sync.Mutex
	messages []model.Message
	err      error
}

func (c *collector) dispatch(_ context.Context, msg model.Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.messages = append(c.messages, msg)
	return c.err
}

func (c *collector) only(t *testing.T) model.Message {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.messages) != 1 {
		t.Fatalf("dispatched %d messages, want 1", len(c.messages))
	}
	return c.messages[0]
}

func (c *collector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.messages)
}

func newTestStream(t *testing.T) (*Stream, *collector) {
	t.Helper()
	got := &collector{}
	return NewStream(streamConfig(), got.dispatch, selfUUID, selfPhone, testLogger()), got
}

func handle(t *testing.T, s *Stream, params string) {
	t.Helper()
	s.Handle(context.Background(), receiveMethod, json.RawMessage(params))
}

func TestHandleGroupMessage(t *testing.T) {
	s, got := newTestStream(t)
	handle(t, s, `{"envelope":{
		"sourceUuid":"`+adaUUID+`",
		"sourceNumber":"+491701234567",
		"sourceName":"Ada Lovelace",
		"timestamp":1754400000000,
		"dataMessage":{
			"timestamp":1754400000000,
			"message":"hello team",
			"groupInfo":{"groupId":"`+groupID+`","groupName":"Bridge Pilot","type":"DELIVER"}
		}}}`)

	msg := got.only(t)
	if msg.PairID != "acme-group" {
		t.Fatalf("PairID = %q, want acme-group", msg.PairID)
	}
	if msg.Platform != model.PlatformSignal {
		t.Fatalf("Platform = %q, want signal", msg.Platform)
	}
	// The id is the author and the send time: Signal assigns nothing else,
	// and a bare timestamp would collide across senders in a group.
	if want := adaUUID + ":1754400000000"; msg.ID != want {
		t.Fatalf("ID = %q, want %q", msg.ID, want)
	}
	if msg.Author.DisplayName != "Ada Lovelace" {
		t.Fatalf("Author.DisplayName = %q", msg.Author.DisplayName)
	}
	if msg.Text != "hello team" {
		t.Fatalf("Text = %q", msg.Text)
	}
	if msg.Kind != model.EventCreated {
		t.Fatalf("Kind = %q, want created", msg.Kind)
	}
	if msg.IsReply() {
		t.Fatalf("ParentID = %q, want empty", msg.ParentID)
	}
	if msg.Timestamp.UnixMilli() != 1754400000000 {
		t.Fatalf("Timestamp = %v", msg.Timestamp)
	}
}

func TestHandleDirectMessage(t *testing.T) {
	cases := []struct {
		name     string
		envelope string
		wantPair string
	}{
		{
			name: "pair configured by service id",
			envelope: `{"envelope":{"sourceUuid":"` + adaUUID + `","sourceNumber":"+491701234567",
				"sourceName":"Ada","timestamp":1,
				"dataMessage":{"timestamp":1,"message":"hi"}}}`,
			wantPair: "ada-direct",
		},
		{
			// An operator may only have the phone number to hand, and which
			// identifier they used is not something to make them think about.
			name: "pair configured by phone number",
			envelope: `{"envelope":{"sourceUuid":"8f5ac0de-0000-4000-8000-000000000002",
				"sourceNumber":"+491701111111","sourceName":"Grace","timestamp":1,
				"dataMessage":{"timestamp":1,"message":"hi"}}}`,
			wantPair: "grace-direct",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, got := newTestStream(t)
			handle(t, s, tc.envelope)
			if msg := got.only(t); msg.PairID != tc.wantPair {
				t.Fatalf("PairID = %q, want %q", msg.PairID, tc.wantPair)
			}
		})
	}
}

// A quote is the only thing Signal has that resembles a thread, so it is what
// the thread mapping is built on.
func TestHandleQuoteBecomesAParent(t *testing.T) {
	s, got := newTestStream(t)
	handle(t, s, `{"envelope":{
		"sourceUuid":"`+adaUUID+`","sourceName":"Ada","timestamp":2,
		"dataMessage":{"timestamp":2,"message":"answering",
			"quote":{"id":1754400000000,"authorUuid":"`+selfUUID+`","text":"question"},
			"groupInfo":{"groupId":"`+groupID+`","type":"DELIVER"}}}}`)

	msg := got.only(t)
	if want := selfUUID + ":1754400000000"; msg.ParentID != want {
		t.Fatalf("ParentID = %q, want %q", msg.ParentID, want)
	}
}

func TestHandleCarriesStylesAndMentions(t *testing.T) {
	s, got := newTestStream(t)
	handle(t, s, `{"envelope":{
		"sourceUuid":"`+adaUUID+`","sourceName":"Ada","timestamp":3,
		"dataMessage":{"timestamp":3,"message":"￼ look at this",
			"mentions":[{"uuid":"`+selfUUID+`","name":"Bridge","start":0,"length":1}],
			"textStyles":[{"style":"BOLD","start":2,"length":4}],
			"groupInfo":{"groupId":"`+groupID+`","type":"DELIVER"}}}}`)

	msg := got.only(t)
	if len(msg.Mentions) != 1 {
		t.Fatalf("got %d mentions, want 1", len(msg.Mentions))
	}
	if msg.Mentions[0].ExternalID != selfUUID || msg.Mentions[0].Start != 0 || msg.Mentions[0].Length != 1 {
		t.Fatalf("mention = %+v", msg.Mentions[0])
	}
	if len(msg.TextStyles) != 1 {
		t.Fatalf("got %d styles, want 1", len(msg.TextStyles))
	}
	if msg.TextStyles[0].Style != model.StyleBold || msg.TextStyles[0].Start != 2 {
		t.Fatalf("style = %+v", msg.TextStyles[0])
	}
}

func TestHandleAttachmentsCarryTheirSignalID(t *testing.T) {
	s, got := newTestStream(t)
	handle(t, s, `{"envelope":{
		"sourceUuid":"`+adaUUID+`","sourceName":"Ada","timestamp":4,
		"dataMessage":{"timestamp":4,"message":"",
			"attachments":[{"contentType":"image/png","filename":"diagram.png","id":"att-1","size":2048}],
			"groupInfo":{"groupId":"`+groupID+`","type":"DELIVER"}}}}`)

	msg := got.only(t)
	if len(msg.Attachments) != 1 {
		t.Fatalf("got %d attachments, want 1", len(msg.Attachments))
	}
	a := msg.Attachments[0]
	if a.Name != "diagram.png" || a.ContentType != "image/png" || a.SizeBytes != 2048 {
		t.Fatalf("attachment = %+v", a)
	}
	// Not a URL: the file is fetched with getAttachment by this id.
	if a.SourceURL != "att-1" {
		t.Fatalf("SourceURL = %q, want the attachment id", a.SourceURL)
	}
}

// The subscription form wraps the envelope in a result. Both shapes are real
// and which one arrives depends on how the daemon was started, so neither may
// be the one we forgot.
func TestHandleAcceptsTheSubscriptionEnvelopeShape(t *testing.T) {
	s, got := newTestStream(t)
	handle(t, s, `{"subscription":0,"result":{"account":"`+selfPhone+`","envelope":{
		"sourceUuid":"`+adaUUID+`","sourceName":"Ada","timestamp":5,
		"dataMessage":{"timestamp":5,"message":"wrapped",
			"groupInfo":{"groupId":"`+groupID+`","type":"DELIVER"}}}}}`)

	if msg := got.only(t); msg.Text != "wrapped" {
		t.Fatalf("Text = %q, want wrapped", msg.Text)
	}
}

// A message the operator sent from a phone linked to the bridge number is a
// real message in the conversation, and a sync transcript is the only way we
// hear about it. It is attributed to the bridge account; loop prevention
// rejects our own mirrored posts by their delivery record, not by author.
func TestHandleSyncSentMessage(t *testing.T) {
	s, got := newTestStream(t)
	handle(t, s, `{"envelope":{
		"sourceUuid":"`+selfUUID+`","sourceName":"Bridge","timestamp":6,
		"syncMessage":{"sentMessage":{
			"destinationUuid":"`+adaUUID+`",
			"timestamp":6,"message":"sent from the phone",
			"groupInfo":{"groupId":"`+groupID+`","type":"DELIVER"}}}}}`)

	msg := got.only(t)
	if msg.PairID != "acme-group" {
		t.Fatalf("PairID = %q, want acme-group", msg.PairID)
	}
	if want := selfUUID + ":6"; msg.ID != want {
		t.Fatalf("ID = %q, want %q", msg.ID, want)
	}
	if msg.Text != "sent from the phone" {
		t.Fatalf("Text = %q", msg.Text)
	}
}

func TestHandleIgnores(t *testing.T) {
	cases := []struct {
		name   string
		params string
		why    string
	}{
		{
			name: "unconfigured group",
			params: `{"envelope":{"sourceUuid":"` + adaUUID + `","timestamp":1,
				"dataMessage":{"timestamp":1,"message":"hi",
					"groupInfo":{"groupId":"SomeOtherGroup","type":"DELIVER"}}}}`,
			why: "the bridge number is reachable by anyone who knows it",
		},
		{
			name: "unconfigured sender",
			params: `{"envelope":{"sourceUuid":"8f5ac0de-0000-4000-8000-0000000000aa",
				"sourceNumber":"+491709999999","timestamp":1,
				"dataMessage":{"timestamp":1,"message":"hello stranger"}}}`,
			why: "an unsolicited direct message is spam, not an error",
		},
		{
			name: "empty message with no attachment",
			params: `{"envelope":{"sourceUuid":"` + adaUUID + `","timestamp":1,
				"dataMessage":{"timestamp":1,"message":"   ",
					"groupInfo":{"groupId":"` + groupID + `","type":"DELIVER"}}}}`,
			why: "there is nothing to mirror",
		},
		{
			name: "receipt",
			params: `{"envelope":{"sourceUuid":"` + adaUUID + `","timestamp":1,
				"receiptMessage":{"when":1,"isDelivery":true,"isRead":false,"timestamps":[1]}}}`,
			why: "receipts are not mirrored",
		},
		{
			name: "typing indicator",
			params: `{"envelope":{"sourceUuid":"` + adaUUID + `","timestamp":1,
				"typingMessage":{"action":"STARTED","timestamp":1}}}`,
			why: "typing indicators are not mirrored",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, got := newTestStream(t)
			handle(t, s, tc.params)
			if n := got.count(); n != 0 {
				t.Fatalf("dispatched %d messages, want 0 — %s", n, tc.why)
			}
		})
	}
}

func TestHandleIgnoresOtherNotificationsAndMalformedPayloads(t *testing.T) {
	s, got := newTestStream(t)

	s.Handle(context.Background(), "callEvent", json.RawMessage(`{"whatever":true}`))
	handle(t, s, `{"envelope":null}`)
	handle(t, s, `not json at all`)

	if n := got.count(); n != 0 {
		t.Fatalf("dispatched %d messages, want 0", n)
	}
}

// A failed delivery must not take the process down or skip the next message:
// the bridge has already recorded the failure, and the read loop carries on.
func TestHandleSurvivesADispatchFailure(t *testing.T) {
	got := &collector{err: context.DeadlineExceeded}
	s := NewStream(streamConfig(), got.dispatch, selfUUID, selfPhone, testLogger())

	handle(t, s, `{"envelope":{"sourceUuid":"`+adaUUID+`","sourceName":"Ada","timestamp":7,
		"dataMessage":{"timestamp":7,"message":"one",
			"groupInfo":{"groupId":"`+groupID+`","type":"DELIVER"}}}}`)
	handle(t, s, `{"envelope":{"sourceUuid":"`+adaUUID+`","sourceName":"Ada","timestamp":8,
		"dataMessage":{"timestamp":8,"message":"two",
			"groupInfo":{"groupId":"`+groupID+`","type":"DELIVER"}}}}`)

	if n := got.count(); n != 2 {
		t.Fatalf("dispatched %d messages, want 2", n)
	}
}

func TestSubscribeCallsTheDaemon(t *testing.T) {
	c, daemon := newFakeDaemon(t)
	serve(t, c)
	s, _ := newTestStream(t)

	errCh := make(chan error, 1)
	go func() { errCh <- s.Subscribe(context.Background(), c) }()

	req := daemon.nextRequest()
	if req.Method != "subscribeReceive" {
		t.Fatalf("method = %q, want subscribeReceive", req.Method)
	}
	daemon.write(`{"jsonrpc":"2.0","id":"` + req.ID + `","result":0}`)

	if err := <-errCh; err != nil {
		t.Fatalf("Subscribe(): %v", err)
	}
}

func TestHandleEdit(t *testing.T) {
	s, got := newTestStream(t)
	handle(t, s, `{"envelope":{
		"sourceUuid":"`+adaUUID+`","sourceName":"Ada","timestamp":20,
		"editMessage":{"targetSentTimestamp":1754400000000,
			"dataMessage":{"timestamp":20,"message":"corrected text",
				"groupInfo":{"groupId":"`+groupID+`","type":"DELIVER"}}}}}`)

	msg := got.only(t)
	if msg.Kind != model.EventEdited {
		t.Fatalf("Kind = %q, want edited", msg.Kind)
	}
	// Two ids: its own, so a second correction is a second event, and the
	// original's, so the far side knows what to change.
	if want := adaUUID + ":20"; msg.ID != want {
		t.Fatalf("ID = %q, want %q", msg.ID, want)
	}
	if want := adaUUID + ":1754400000000"; msg.EditsID != want {
		t.Fatalf("EditsID = %q, want %q", msg.EditsID, want)
	}
	if msg.Text != "corrected text" {
		t.Fatalf("Text = %q", msg.Text)
	}
}

func TestHandleRemoteDelete(t *testing.T) {
	s, got := newTestStream(t)
	handle(t, s, `{"envelope":{
		"sourceUuid":"`+adaUUID+`","sourceName":"Ada","timestamp":21,
		"dataMessage":{"timestamp":21,"message":"",
			"remoteDelete":{"timestamp":1754400000000},
			"groupInfo":{"groupId":"`+groupID+`","type":"DELIVER"}}}}`)

	msg := got.only(t)
	if msg.Kind != model.EventDeleted {
		t.Fatalf("Kind = %q, want deleted", msg.Kind)
	}
	// A delete has no version of its own: the original's id is the event's.
	if want := adaUUID + ":1754400000000"; msg.ID != want {
		t.Fatalf("ID = %q, want %q", msg.ID, want)
	}
}

func TestHandleReaction(t *testing.T) {
	s, got := newTestStream(t)
	var reactions []model.Reaction
	s.OnReaction(func(_ context.Context, r model.Reaction) error {
		reactions = append(reactions, r)
		return nil
	})

	handle(t, s, `{"envelope":{
		"sourceUuid":"`+adaUUID+`","sourceName":"Ada","timestamp":22,
		"dataMessage":{"timestamp":22,"message":"",
			"reaction":{"emoji":"👍","targetAuthorUuid":"`+selfUUID+`",
				"targetSentTimestamp":1754400000000,"isRemove":false},
			"groupInfo":{"groupId":"`+groupID+`","type":"DELIVER"}}}}`)

	if got.count() != 0 {
		t.Fatalf("a reaction was dispatched as a message")
	}
	if len(reactions) != 1 {
		t.Fatalf("dispatched %d reactions, want 1", len(reactions))
	}
	r := reactions[0]
	if r.Emoji != "👍" || r.Remove {
		t.Fatalf("reaction = %+v", r)
	}
	// The message reacted to is named by author and send time, which is what
	// the canonical id is made of.
	if want := selfUUID + ":1754400000000"; r.TargetID != want {
		t.Fatalf("TargetID = %q, want %q", r.TargetID, want)
	}
	if r.Author.ExternalID != adaUUID {
		t.Fatalf("Author.ExternalID = %q, want %q", r.Author.ExternalID, adaUUID)
	}
}

// Without a reaction dispatcher registered, reactions are dropped rather than
// crashing the read loop: that is how a deployment turns the feature off.
func TestHandleReactionWithoutADispatcher(t *testing.T) {
	s, got := newTestStream(t)
	handle(t, s, `{"envelope":{
		"sourceUuid":"`+adaUUID+`","timestamp":23,
		"dataMessage":{"timestamp":23,"message":"",
			"reaction":{"emoji":"👍","targetAuthorUuid":"`+selfUUID+`",
				"targetSentTimestamp":1,"isRemove":false},
			"groupInfo":{"groupId":"`+groupID+`","type":"DELIVER"}}}}`)

	if got.count() != 0 {
		t.Fatalf("dispatched %d messages, want 0", got.count())
	}
}

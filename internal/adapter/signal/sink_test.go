package signal

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aenix-io/slack-connectors/internal/bridge"
	"github.com/aenix-io/slack-connectors/internal/model"
)

// fakeCaller records the JSON-RPC calls a sink makes and answers with
// whatever the test set up.
type fakeCaller struct {
	method string
	params map[string]any
	result string
	err    error
}

func (f *fakeCaller) Call(_ context.Context, method string, params, out any) error {
	f.method = method
	if p, ok := params.(map[string]any); ok {
		f.params = p
	}
	if f.err != nil {
		return f.err
	}
	if f.result == "" || out == nil {
		return nil
	}
	return json.Unmarshal([]byte(f.result), out)
}

func newTestSink(t *testing.T, result string) (*Sink, *fakeCaller) {
	t.Helper()
	c := &fakeCaller{result: result}
	s := NewSink(c, streamConfig())
	s.SetSelf(selfUUID)
	return s, c
}

func slackMessage() model.Message {
	return model.Message{
		Platform: model.PlatformSlack,
		ID:       "1700000000.000100",
		PairID:   "acme-group",
		Author:   model.Author{DisplayName: "Timur Tukaev"},
		Text:     "hello there",
		Kind:     model.EventCreated,
	}
}

const okResult = `{"timestamp":1754400000000,"results":[{"type":"SUCCESS"}]}`

func TestDeliverSendsToAGroup(t *testing.T) {
	s, caller := newTestSink(t, okResult)

	id, err := s.Deliver(context.Background(), slackMessage(), groupID, "")
	if err != nil {
		t.Fatalf("Deliver(): %v", err)
	}
	if caller.method != "send" {
		t.Fatalf("method = %q, want send", caller.method)
	}
	if got := caller.params["groupId"]; got != groupID {
		t.Fatalf("groupId = %v, want %q", got, groupID)
	}
	if _, ok := caller.params["recipient"]; ok {
		t.Fatal("a group send must not also name a recipient")
	}
	// The id is composed from the bridge account and the timestamp signal-cli
	// used, because that is what an incoming quote of this message will name.
	if want := selfUUID + ":1754400000000"; id != want {
		t.Fatalf("id = %q, want %q", id, want)
	}
}

func TestDeliverSendsToADirectRecipient(t *testing.T) {
	s, caller := newTestSink(t, okResult)

	msg := slackMessage()
	msg.PairID = "ada-direct"
	if _, err := s.Deliver(context.Background(), msg, adaUUID, ""); err != nil {
		t.Fatalf("Deliver(): %v", err)
	}
	got, ok := caller.params["recipient"].([]string)
	if !ok || len(got) != 1 || got[0] != adaUUID {
		t.Fatalf("recipient = %v, want [%s]", caller.params["recipient"], adaUUID)
	}
	if _, ok := caller.params["groupId"]; ok {
		t.Fatal("a direct send must not name a group")
	}
}

// Signal has no impersonation: everything arrives from the bridge number. The
// author line is the only thing telling a customer who is speaking.
func TestDeliverPrefixesTheAuthorInBold(t *testing.T) {
	s, caller := newTestSink(t, okResult)

	if _, err := s.Deliver(context.Background(), slackMessage(), groupID, ""); err != nil {
		t.Fatalf("Deliver(): %v", err)
	}
	if got := caller.params["message"]; got != "Timur Tukaev\nhello there" {
		t.Fatalf("message = %q", got)
	}
	styles, ok := caller.params["textStyle"].([]string)
	if !ok || len(styles) == 0 {
		t.Fatalf("textStyle = %v, want the author line styled", caller.params["textStyle"])
	}
	// "Timur Tukaev" is 12 UTF-16 units from offset 0.
	if styles[0] != "0:12:BOLD" {
		t.Fatalf("textStyle[0] = %q, want 0:12:BOLD", styles[0])
	}
}

func TestDeliverFallsBackToTheSourceName(t *testing.T) {
	s, caller := newTestSink(t, okResult)

	msg := slackMessage()
	msg.Author.DisplayName = ""
	if _, err := s.Deliver(context.Background(), msg, groupID, ""); err != nil {
		t.Fatalf("Deliver(): %v", err)
	}
	if got := caller.params["message"]; got != "Slack\nhello there" {
		t.Fatalf("message = %q, want the source named", got)
	}
}

// A quote is how a Slack thread reply reaches Signal, and it names the quoted
// message by author and send time — both of which the canonical id carries.
func TestDeliverQuotesTheThreadTarget(t *testing.T) {
	s, caller := newTestSink(t, okResult)

	target := adaUUID + ":1754399999999"
	if _, err := s.Deliver(context.Background(), slackMessage(), groupID, target); err != nil {
		t.Fatalf("Deliver(): %v", err)
	}
	if got := caller.params["quoteTimestamp"]; got != int64(1754399999999) {
		t.Fatalf("quoteTimestamp = %v, want 1754399999999", got)
	}
	if got := caller.params["quoteAuthor"]; got != adaUUID {
		t.Fatalf("quoteAuthor = %v, want %q", got, adaUUID)
	}
	// The quoted text is deliberately absent: clients render a quote from
	// their own copy, and this bridge does not keep message bodies.
	if _, ok := caller.params["quoteMessage"]; ok {
		t.Fatal("the quoted body must not be sent; the bridge stores no message text")
	}
}

func TestDeliverStylesTheBody(t *testing.T) {
	s, caller := newTestSink(t, okResult)

	msg := slackMessage()
	msg.Author.DisplayName = "A"
	msg.Text = "see *this* now"
	if _, err := s.Deliver(context.Background(), msg, groupID, ""); err != nil {
		t.Fatalf("Deliver(): %v", err)
	}
	if got := caller.params["message"]; got != "A\nsee this now" {
		t.Fatalf("message = %q, want the markers gone from the body", got)
	}
	styles, _ := caller.params["textStyle"].([]string)
	// "A\n" is 2 units, "see " is 4, so "this" starts at 6 and runs 4.
	if len(styles) != 2 || styles[1] != "6:4:BOLD" {
		t.Fatalf("textStyle = %v, want the bold span at 6:4", styles)
	}
}

func TestDeliverClassifiesFailures(t *testing.T) {
	cases := []struct {
		name        string
		err         error
		wantNotSent bool
		why         string
	}{
		{
			name:        "a refusal is provably not sent",
			err:         &Error{Code: CodeUserError, Message: "invalid recipient"},
			wantNotSent: true,
			why:         "signal-cli read the request and refused it",
		},
		{
			name:        "an io error is ambiguous",
			err:         &Error{Code: CodeIOError, Message: "connection reset"},
			wantNotSent: false,
			why:         "the message may have been delivered before the failure surfaced",
		},
		{
			name:        "a transport failure is ambiguous",
			err:         errors.New("socket closed"),
			wantNotSent: false,
			why:         "nothing here proves the daemon did not send it",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &fakeCaller{err: tc.err}
			s := NewSink(c, streamConfig())

			_, err := s.Deliver(context.Background(), slackMessage(), groupID, "")
			if err == nil {
				t.Fatal("Deliver() error = nil, want a failure")
			}
			if got := errors.Is(err, bridge.ErrNotSent); got != tc.wantNotSent {
				t.Fatalf("wraps ErrNotSent = %v, want %v — %s", got, tc.wantNotSent, tc.why)
			}
		})
	}
}

// A group send where some members failed still put the message in the
// conversation. Reporting it as a failure would have the bridge post it again.
func TestDeliverSucceedsOnAPartialGroupFailure(t *testing.T) {
	s, _ := newTestSink(t, `{"timestamp":1754400000000,"results":[
		{"type":"SUCCESS"},
		{"type":"UNREGISTERED_FAILURE","recipientAddress":{"number":"+49170"}}]}`)

	id, err := s.Deliver(context.Background(), slackMessage(), groupID, "")
	if err != nil {
		t.Fatalf("Deliver(): %v", err)
	}
	if id == "" {
		t.Fatal("id was empty; the message exists in the conversation")
	}
}

func TestDeliverRejectsAnUnconfiguredConversation(t *testing.T) {
	s, _ := newTestSink(t, okResult)

	_, err := s.Deliver(context.Background(), slackMessage(), "SomeOtherGroup", "")
	if err == nil {
		t.Fatal("Deliver() error = nil, want a failure")
	}
	if !errors.Is(err, bridge.ErrNotSent) {
		t.Fatalf("error = %v, want it to wrap ErrNotSent — nothing was attempted", err)
	}
}

func TestDeliverRejectsAThreadTargetThatIsNotASignalID(t *testing.T) {
	s, _ := newTestSink(t, okResult)

	if _, err := s.Deliver(context.Background(), slackMessage(), groupID, "1700000000.000100"); err == nil {
		t.Fatal("Deliver() error = nil, want a failure for a Slack timestamp used as a Signal id")
	}
}

func TestSplitMessageID(t *testing.T) {
	cases := []struct {
		name      string
		id        string
		author    string
		timestamp int64
		ok        bool
	}{
		{name: "service id", id: adaUUID + ":1754400000000", author: adaUUID, timestamp: 1754400000000, ok: true},
		{name: "phone number", id: "+491701234567:17", author: "+491701234567", timestamp: 17, ok: true},
		{name: "slack timestamp", id: "1700000000.000100", ok: false},
		{name: "no timestamp", id: adaUUID + ":", ok: false},
		{name: "not a number", id: adaUUID + ":soon", ok: false},
		{name: "empty", id: "", ok: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			author, timestamp, ok := SplitMessageID(tc.id)
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v", ok, tc.ok)
			}
			if !ok {
				return
			}
			if author != tc.author || timestamp != tc.timestamp {
				t.Fatalf("= %q, %d; want %q, %d", author, timestamp, tc.author, tc.timestamp)
			}
		})
	}
}

func TestUpdateSendsAnEdit(t *testing.T) {
	s, caller := newTestSink(t, okResult)
	s.SetClock(func() time.Time { return time.UnixMilli(1754400000000).Add(time.Hour) })

	msg := slackMessage()
	msg.Text = "corrected"
	target := selfUUID + ":1754400000000"
	if err := s.Update(context.Background(), msg, groupID, target); err != nil {
		t.Fatalf("Update(): %v", err)
	}
	if caller.method != "send" {
		t.Fatalf("method = %q, want send", caller.method)
	}
	if got := caller.params["editTimestamp"]; got != int64(1754400000000) {
		t.Fatalf("editTimestamp = %v, want the original's send time", got)
	}
	// Signal rebuilds the whole message, so the author line is part of it.
	if got := caller.params["message"]; got != "Timur Tukaev\ncorrected" {
		t.Fatalf("message = %q", got)
	}
}

// Signal accepts an edit or a delete past 24 hours and every client ignores
// it. Reporting success there would leave a customer reading a number the
// author has retracted, with the bridge's metrics claiming otherwise.
func TestChangesOutsideTheWindowAreReported(t *testing.T) {
	sent := time.UnixMilli(1754400000000)
	old := func() time.Time { return sent.Add(25 * time.Hour) }

	t.Run("edit", func(t *testing.T) {
		s, caller := newTestSink(t, okResult)
		s.SetClock(old)
		err := s.Update(context.Background(), slackMessage(), groupID, selfUUID+":1754400000000")
		if !errors.Is(err, bridge.ErrOutOfWindow) {
			t.Fatalf("error = %v, want ErrOutOfWindow", err)
		}
		if caller.method != "" {
			t.Fatalf("called %q; nothing should be sent past the window", caller.method)
		}
	})

	t.Run("delete", func(t *testing.T) {
		s, caller := newTestSink(t, okResult)
		s.SetClock(old)
		err := s.Delete(context.Background(), groupID, selfUUID+":1754400000000")
		if !errors.Is(err, bridge.ErrOutOfWindow) {
			t.Fatalf("error = %v, want ErrOutOfWindow", err)
		}
		if caller.method != "" {
			t.Fatalf("called %q; nothing should be sent past the window", caller.method)
		}
	})
}

func TestDeleteSendsARemoteDelete(t *testing.T) {
	s, caller := newTestSink(t, okResult)
	s.SetClock(func() time.Time { return time.UnixMilli(1754400000000).Add(time.Minute) })

	if err := s.Delete(context.Background(), groupID, selfUUID+":1754400000000"); err != nil {
		t.Fatalf("Delete(): %v", err)
	}
	if caller.method != "remoteDelete" {
		t.Fatalf("method = %q, want remoteDelete", caller.method)
	}
	if got := caller.params["targetTimestamp"]; got != int64(1754400000000) {
		t.Fatalf("targetTimestamp = %v", got)
	}
	if got := caller.params["groupId"]; got != groupID {
		t.Fatalf("groupId = %v, want the pair's group", got)
	}
}

func TestReactNamesTheTargetByAuthorAndTime(t *testing.T) {
	s, caller := newTestSink(t, okResult)

	err := s.React(context.Background(), model.Reaction{Emoji: "👍"}, groupID, adaUUID+":1754400000000")
	if err != nil {
		t.Fatalf("React(): %v", err)
	}
	if caller.method != "sendReaction" {
		t.Fatalf("method = %q, want sendReaction", caller.method)
	}
	if got := caller.params["emoji"]; got != "👍" {
		t.Fatalf("emoji = %v", got)
	}
	if got := caller.params["targetAuthor"]; got != adaUUID {
		t.Fatalf("targetAuthor = %v, want the message's author", got)
	}
	if got := caller.params["targetTimestamp"]; got != int64(1754400000000) {
		t.Fatalf("targetTimestamp = %v", got)
	}
	if _, ok := caller.params["remove"]; ok {
		t.Fatal("an added reaction must not carry remove")
	}
}

func TestReactWithdraws(t *testing.T) {
	s, caller := newTestSink(t, okResult)

	err := s.React(context.Background(),
		model.Reaction{Emoji: "👍", Remove: true}, groupID, adaUUID+":1754400000000")
	if err != nil {
		t.Fatalf("React(): %v", err)
	}
	if got := caller.params["remove"]; got != true {
		t.Fatalf("remove = %v, want true", got)
	}
}

// A file rides along with the message rather than following it: a separate
// send would arrive as a second, bodyless message in the conversation.
func TestDeliverCarriesAnAttachment(t *testing.T) {
	s, caller := newTestSink(t, okResult)

	msg := slackMessage()
	msg.Attachments = []model.Attachment{{
		Name:        "q3.pdf",
		ContentType: "application/pdf",
		SizeBytes:   9,
		Open: func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader("pdf bytes")), nil
		},
	}}

	if _, err := s.Deliver(context.Background(), msg, groupID, ""); err != nil {
		t.Fatalf("Deliver(): %v", err)
	}
	got, ok := caller.params["attachment"].([]string)
	if !ok || len(got) != 1 {
		t.Fatalf("attachment = %v, want one data URI", caller.params["attachment"])
	}
	want := "data:application/pdf;filename=q3.pdf;base64," +
		base64.StdEncoding.EncodeToString([]byte("pdf bytes"))
	if got[0] != want {
		t.Fatalf("attachment = %q, want %q", got[0], want)
	}
	// The message keeps its own body: the file is an addition, not a
	// replacement.
	if body := caller.params["message"]; body != "Timur Tukaev\nhello there" {
		t.Fatalf("message = %q", body)
	}
}

// Naming the sender is the point of the notice: without it a reader knows
// only that something failed and has nobody to ask.
func TestDeliverNotesAFileItCouldNotCarry(t *testing.T) {
	cases := []struct {
		name       string
		attachment model.Attachment
		wantIn     string
	}{
		{
			name: "over the size ceiling",
			attachment: model.Attachment{
				Name:      "huge.zip",
				SizeBytes: 200 << 20,
				Open: func(context.Context) (io.ReadCloser, error) {
					return io.NopCloser(strings.NewReader("x")), nil
				},
			},
			wantIn: "huge.zip",
		},
		{
			name: "the source could not be read",
			attachment: model.Attachment{
				Name:      "gone.pdf",
				SizeBytes: 10,
				Open: func(context.Context) (io.ReadCloser, error) {
					return nil, errors.New("404")
				},
			},
			wantIn: "gone.pdf",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, caller := newTestSink(t, okResult)
			msg := slackMessage()
			msg.Attachments = []model.Attachment{tc.attachment}

			if _, err := s.Deliver(context.Background(), msg, groupID, ""); err != nil {
				t.Fatalf("Deliver(): %v", err)
			}
			if _, ok := caller.params["attachment"]; ok {
				t.Fatal("a file that could not be read must not be sent")
			}
			body, _ := caller.params["message"].(string)
			if !strings.Contains(body, tc.wantIn) {
				t.Fatalf("message = %q, want it to name %q", body, tc.wantIn)
			}
			if !strings.Contains(body, "Timur Tukaev") {
				t.Fatalf("message = %q, want it to name the sender", body)
			}
			// The message the person actually wrote is still there.
			if !strings.Contains(body, "hello there") {
				t.Fatalf("message = %q, want the original text kept", body)
			}
		})
	}
}

// A transferred file is one the sidecar no longer needs to keep.
func TestDeliverReportsTransferredAttachments(t *testing.T) {
	s, _ := newTestSink(t, okResult)
	var transferred []string
	s.OnAttachmentTransferred(func(a model.Attachment) { transferred = append(transferred, a.SourceURL) })

	msg := slackMessage()
	msg.Attachments = []model.Attachment{{
		Name:      "a.txt",
		SizeBytes: 3,
		SourceURL: "att-1",
		Open: func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader("abc")), nil
		},
	}}
	if _, err := s.Deliver(context.Background(), msg, groupID, ""); err != nil {
		t.Fatalf("Deliver(): %v", err)
	}
	if len(transferred) != 1 || transferred[0] != "att-1" {
		t.Fatalf("transferred = %v, want [att-1]", transferred)
	}
}

// signal-cli runs every data URI parameter through a URL decoder. Passed raw,
// a name containing "%" throws before the message is sent at all — text and
// file both — a "+" silently becomes a space, and a comma ends the parameter
// section early and takes part of the name into the payload.
func TestDeliverEncodesTheAttachmentFilename(t *testing.T) {
	cases := []struct {
		name     string
		filename string
		wantIn   string
	}{
		{name: "percent", filename: "100% done.pdf", wantIn: "filename=100%25+done.pdf;"},
		{name: "plus", filename: "C++ notes.md", wantIn: "filename=C%2B%2B+notes.md;"},
		{name: "comma", filename: "report,final.pdf", wantIn: "filename=report%2Cfinal.pdf;"},
		{name: "cyrillic", filename: "отчёт.pdf", wantIn: "filename=%D0%BE%D1%82"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, caller := newTestSink(t, okResult)
			msg := slackMessage()
			msg.Attachments = []model.Attachment{{
				Name:        tc.filename,
				ContentType: "application/pdf",
				SizeBytes:   3,
				Open: func(context.Context) (io.ReadCloser, error) {
					return io.NopCloser(strings.NewReader("abc")), nil
				},
			}}
			if _, err := s.Deliver(context.Background(), msg, groupID, ""); err != nil {
				t.Fatalf("Deliver(): %v", err)
			}
			got, _ := caller.params["attachment"].([]string)
			if len(got) != 1 {
				t.Fatalf("attachment = %v", caller.params["attachment"])
			}
			if !strings.Contains(got[0], tc.wantIn) {
				t.Fatalf("data URI = %q, want it to contain %q", got[0], tc.wantIn)
			}
		})
	}
}

// A rate limit means nothing was delivered, so the send is worth repeating —
// and signal-cli says how long to wait in the error's data.
func TestDeliverRetriesARateLimitedSend(t *testing.T) {
	c := &countingCaller{
		failures: 1,
		err: &Error{
			Code:    CodeRateLimit,
			Message: "rate limited",
			Data:    json.RawMessage(`{"results":[{"type":"RATE_LIMIT_FAILURE","retryAfterSeconds":0}]}`),
		},
		result: okResult,
	}
	s := NewSink(c, streamConfig())
	s.SetSelf(selfUUID)

	id, err := s.Deliver(context.Background(), slackMessage(), groupID, "")
	if err != nil {
		t.Fatalf("Deliver(): %v", err)
	}
	if id == "" {
		t.Fatal("id was empty")
	}
	if c.calls != 2 {
		t.Fatalf("called signal-cli %d times, want 2 — the first was rate limited", c.calls)
	}
}

func TestDeliverGivesUpAfterRepeatedRateLimits(t *testing.T) {
	c := &countingCaller{
		failures: 99,
		err:      &Error{Code: CodeRateLimit, Message: "rate limited"},
	}
	s := NewSink(c, streamConfig())

	_, err := s.Deliver(context.Background(), slackMessage(), groupID, "")
	if err == nil {
		t.Fatal("Deliver() error = nil, want a failure")
	}
	// Nothing was delivered, so the claim must be released and the message
	// left replayable rather than stuck.
	if !errors.Is(err, bridge.ErrNotSent) {
		t.Fatalf("error = %v, want it to wrap ErrNotSent", err)
	}
	if c.calls != 3 {
		t.Fatalf("called signal-cli %d times, want 3 — the retry budget", c.calls)
	}
}

// countingCaller fails a given number of times before answering.
type countingCaller struct {
	calls    int
	failures int
	err      error
	result   string
}

func (c *countingCaller) Call(_ context.Context, _ string, _, out any) error {
	c.calls++
	if c.calls <= c.failures {
		return c.err
	}
	if c.result == "" || out == nil {
		return nil
	}
	return json.Unmarshal([]byte(c.result), out)
}

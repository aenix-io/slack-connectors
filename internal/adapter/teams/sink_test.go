package teams

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aenix-io/slack-connectors/internal/bridge"
	"github.com/aenix-io/slack-connectors/internal/model"
)

type postCall struct {
	teamID, channelID, replyToID, content string
}

// fakePoster records calls and returns queued results, so the sink's own
// behaviour is tested without a transport underneath.
type fakePoster struct {
	calls   []postCall
	results []error
	id      string
}

func (f *fakePoster) PostChannelMessage(
	_ context.Context, teamID, channelID, replyToID, content string,
) (string, error) {
	f.calls = append(f.calls, postCall{teamID, channelID, replyToID, content})
	if len(f.results) > 0 {
		err := f.results[0]
		f.results = f.results[1:]
		if err != nil {
			return "", err
		}
	}
	return f.id, nil
}

func msg(author, text string) model.Message {
	return model.Message{
		Author: model.Author{DisplayName: author},
		Text:   text,
	}
}

func TestSinkDeliverNewMessage(t *testing.T) {
	poster := &fakePoster{id: "175"}
	s := NewSink(poster, recoveryConfig())

	id, err := s.Deliver(context.Background(), msg("Ivan", "hi"), "c1", "")
	if err != nil {
		t.Fatalf("Deliver(): %v", err)
	}
	if id != "175" {
		t.Fatalf("id = %q", id)
	}
	if len(poster.calls) != 1 {
		t.Fatalf("made %d posts, want 1", len(poster.calls))
	}
	call := poster.calls[0]
	if call.teamID != "t1" || call.channelID != "c1" || call.replyToID != "" {
		t.Fatalf("call = %+v", call)
	}
	if !strings.HasPrefix(call.content, "<strong>Ivan</strong><br>") {
		t.Fatalf("content = %q, want the author as the first line", call.content)
	}
}

func TestSinkDeliverReplyPassesTheThreadRoot(t *testing.T) {
	poster := &fakePoster{id: "x"}
	s := NewSink(poster, recoveryConfig())

	if _, err := s.Deliver(context.Background(), msg("Ivan", "hi"), "c1", "root-9"); err != nil {
		t.Fatalf("Deliver(): %v", err)
	}
	if poster.calls[0].replyToID != "root-9" {
		t.Fatalf("replyToID = %q, want root-9", poster.calls[0].replyToID)
	}
}

func TestSinkDeliverFallbackAuthorName(t *testing.T) {
	poster := &fakePoster{id: "x"}
	s := NewSink(poster, recoveryConfig())

	if _, err := s.Deliver(context.Background(), msg("  ", "hi"), "c1", ""); err != nil {
		t.Fatalf("Deliver(): %v", err)
	}
	if !strings.HasPrefix(poster.calls[0].content, "<strong>Slack</strong><br>") {
		t.Fatalf("content = %q, want the fallback author", poster.calls[0].content)
	}
}

func TestSinkDeliverEscapesTheAuthorName(t *testing.T) {
	poster := &fakePoster{id: "x"}
	s := NewSink(poster, recoveryConfig())

	if _, err := s.Deliver(context.Background(), msg("<i>x</i>", "hi"), "c1", ""); err != nil {
		t.Fatalf("Deliver(): %v", err)
	}
	if strings.Contains(poster.calls[0].content, "<i>") {
		t.Fatalf("content = %q — a remote profile name injected markup", poster.calls[0].content)
	}
}

func TestSinkDeliverConvertsMrkdwn(t *testing.T) {
	poster := &fakePoster{id: "x"}
	s := NewSink(poster, recoveryConfig())

	if _, err := s.Deliver(context.Background(), msg("Ivan", "*bold*"), "c1", ""); err != nil {
		t.Fatalf("Deliver(): %v", err)
	}
	if !strings.Contains(poster.calls[0].content, "<strong>bold</strong>") {
		t.Fatalf("content = %q, want converted mrkdwn", poster.calls[0].content)
	}
}

func TestSinkDeliverUnknownChannelIsAClearError(t *testing.T) {
	poster := &fakePoster{id: "x"}
	s := NewSink(poster, recoveryConfig())

	_, err := s.Deliver(context.Background(), msg("Ivan", "hi"), "nope", "")
	if !errors.Is(err, bridge.ErrNotSent) {
		t.Fatalf("error = %v, want ErrNotSent", err)
	}
	if len(poster.calls) != 0 {
		t.Fatal("posted despite an unconfigured channel")
	}
}

func TestSinkDeliverRetriesOnlyOnThrottle(t *testing.T) {
	poster := &fakePoster{
		id:      "x",
		results: []error{&ThrottledError{RetryAfter: time.Millisecond}, nil},
	}
	s := NewSink(poster, recoveryConfig())

	if _, err := s.Deliver(context.Background(), msg("Ivan", "hi"), "c1", ""); err != nil {
		t.Fatalf("Deliver() after a throttle: %v", err)
	}
	if len(poster.calls) != 2 {
		t.Fatalf("made %d posts, want a single retry after the throttle", len(poster.calls))
	}
}

func TestSinkDeliverDoesNotRetryPermanentErrors(t *testing.T) {
	poster := &fakePoster{results: []error{errors.New("connector 403")}}
	s := NewSink(poster, recoveryConfig())

	if _, err := s.Deliver(context.Background(), msg("Ivan", "hi"), "c1", ""); err == nil {
		t.Fatal("Deliver() = nil, want the permanent error surfaced")
	}
	// An ambiguous failure must never be repeated: the post may have landed,
	// and a second attempt would duplicate it.
	if len(poster.calls) != 1 {
		t.Fatalf("made %d posts, want exactly 1", len(poster.calls))
	}
}

func TestSinkDeliverGivesUpAfterMaxThrottledAttempts(t *testing.T) {
	throttle := func() error { return &ThrottledError{RetryAfter: time.Millisecond} }
	poster := &fakePoster{results: []error{throttle(), throttle(), throttle()}}
	s := NewSink(poster, recoveryConfig())

	if _, err := s.Deliver(context.Background(), msg("Ivan", "hi"), "c1", ""); err == nil {
		t.Fatal("Deliver() = nil, want an error after exhausting attempts")
	}
	if len(poster.calls) != maxPostAttempts {
		t.Fatalf("made %d posts, want %d", len(poster.calls), maxPostAttempts)
	}
}

package bridge

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aenix-io/slack-connectors/internal/config"
	"github.com/aenix-io/slack-connectors/internal/model"
	"github.com/aenix-io/slack-connectors/internal/store"
)

// fakeSink records what the bridge asked it to deliver.
type fakeSink struct {
	mu         sync.Mutex
	calls      []fakeCall
	nextID     string
	deliverErr error
}

type fakeCall struct {
	msg          model.Message
	channelID    string
	threadTarget string
}

func (f *fakeSink) Deliver(_ context.Context, msg model.Message, channelID, threadTarget string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fakeCall{msg: msg, channelID: channelID, threadTarget: threadTarget})
	if f.deliverErr != nil {
		return "", f.deliverErr
	}
	if f.nextID == "" {
		return "delivered-1", nil
	}
	return f.nextID, nil
}

// fakeStore is an in-memory store.Store.
type fakeStore struct {
	mu          sync.Mutex
	processed   map[string]bool
	deliveries  map[string]store.Delivery
	reactions   []store.ReactionDelivery
	edits       map[string]int
	unmarkErr   error
	lookupErr   error
	recordErr   error
	reactionErr error
	// unmarkCtx records the context UnmarkProcessed was actually called with,
	// so a test can assert release() did not hand it the caller's own
	// (possibly cancelled) context.
	unmarkCtx context.Context
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		processed:  make(map[string]bool),
		deliveries: make(map[string]store.Delivery),
	}
}

func (f *fakeStore) MarkProcessed(_ context.Context, src model.Platform, srcID string, kind model.EventKind) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := string(src) + "|" + srcID + "|" + string(kind)
	if f.processed[key] {
		return false, nil
	}
	f.processed[key] = true
	return true, nil
}

func (f *fakeStore) UnmarkProcessed(ctx context.Context, src model.Platform, srcID string, kind model.EventKind) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unmarkCtx = ctx
	if f.unmarkErr != nil {
		return f.unmarkErr
	}
	delete(f.processed, string(src)+"|"+srcID+"|"+string(kind))
	return nil
}

func (f *fakeStore) RecordDelivery(_ context.Context, d store.Delivery) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.recordErr != nil {
		return f.recordErr
	}
	f.deliveries[d.PairID+"|"+string(d.SourcePlatform)+"|"+d.SourceID] = d
	return nil
}

func (f *fakeStore) LookupTarget(_ context.Context, pairID string, src model.Platform, srcID string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lookupErr != nil {
		return "", f.lookupErr
	}
	d, ok := f.deliveries[pairID+"|"+string(src)+"|"+srcID]
	if !ok {
		return "", store.ErrNotFound
	}
	return d.TargetID, nil
}

// LookupCounterpart mirrors the SQL: match either end of the mapping, scoped
// to the pair.
func (f *fakeStore) LookupCounterpart(_ context.Context, pairID string, p model.Platform, id string) (store.Counterpart, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lookupErr != nil {
		return store.Counterpart{}, f.lookupErr
	}
	for _, d := range f.deliveries {
		if d.PairID != pairID {
			continue
		}
		if d.SourcePlatform == p && d.SourceID == id {
			return store.Counterpart{ID: d.TargetID, ThreadRoot: d.TargetThreadRoot}, nil
		}
		if d.TargetPlatform == p && d.TargetID == id {
			return store.Counterpart{ID: d.SourceID, ThreadRoot: d.SourceThreadRoot}, nil
		}
	}
	return store.Counterpart{}, store.ErrNotFound
}

// Reactions are stored in insertion order so the newest survivor can be
// found the way the SQL finds it: by recency.
// wasProcessed reports whether an event is still claimed.
func (f *fakeStore) wasProcessed(p model.Platform, id string, kind model.EventKind) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.processed[string(p)+"|"+id+"|"+string(kind)]
}

func (f *fakeStore) CountEdit(
	_ context.Context, pairID string, target model.Platform, targetID string,
) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.edits == nil {
		f.edits = make(map[string]int)
	}
	key := pairID + "|" + string(target) + "|" + targetID
	f.edits[key]++
	return f.edits[key], nil
}

func (f *fakeStore) RecordReaction(_ context.Context, r store.ReactionDelivery) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.reactionErr != nil {
		return f.reactionErr
	}
	for i, existing := range f.reactions {
		if existing.PairID == r.PairID && existing.SourcePlatform == r.SourcePlatform &&
			existing.SourceAuthor == r.SourceAuthor &&
			existing.SourceMessageID == r.SourceMessageID && existing.Emoji == r.Emoji {
			f.reactions[i] = r
			return nil
		}
	}
	f.reactions = append(f.reactions, r)
	return nil
}

func (f *fakeStore) ForgetReaction(
	_ context.Context, pairID string, src model.Platform, author, sourceMessageID, emoji string,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	kept := f.reactions[:0]
	for _, r := range f.reactions {
		if r.PairID == pairID && r.SourcePlatform == src && r.SourceAuthor == author &&
			r.SourceMessageID == sourceMessageID && r.Emoji == emoji {
			continue
		}
		kept = append(kept, r)
	}
	f.reactions = kept
	return nil
}

func (f *fakeStore) LatestReaction(
	_ context.Context,
	pairID string,
	target model.Platform,
	targetMessageID string,
	except store.ReactionKey,
) (store.ReactionDelivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := len(f.reactions) - 1; i >= 0; i-- {
		r := f.reactions[i]
		if r.PairID != pairID || r.TargetPlatform != target || r.TargetMessageID != targetMessageID {
			continue
		}
		if r.SourceAuthor == except.SourceAuthor &&
			r.SourceMessageID == except.SourceMessageID && r.Emoji == except.Emoji {
			continue
		}
		return r, nil
	}
	return store.ReactionDelivery{}, store.ErrNotFound
}

func (f *fakeStore) IsReactionResult(
	_ context.Context, pairID string, target model.Platform, targetMessageID, emoji string,
) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range f.reactions {
		if r.PairID == pairID && r.TargetPlatform == target &&
			r.TargetMessageID == targetMessageID && r.Emoji == emoji {
			return true, nil
		}
	}
	return false, nil
}

func (f *fakeStore) IsDeliveryResult(_ context.Context, p model.Platform, id string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, d := range f.deliveries {
		if d.TargetPlatform == p && d.TargetID == id {
			return true, nil
		}
	}
	return false, nil
}

func (f *fakeStore) SaveSubscription(_ context.Context, _ string, _ store.Subscription) error {
	return nil
}

func (f *fakeStore) LoadSubscription(_ context.Context, _ string) (store.Subscription, error) {
	return store.Subscription{}, store.ErrNotFound
}

func (f *fakeStore) DeleteSubscription(_ context.Context, _ string) error { return nil }

func (f *fakeStore) Close() error { return nil }

func testConfig() config.Config {
	return config.Config{Pairs: []config.Pair{{
		ID:    "acme",
		Teams: &config.TeamsEndpoint{TeamID: "team-1", ChannelID: "channel-1"},
		Slack: &config.SlackEndpoint{ChannelID: "C01ABCDEF"},
	}}}
}

func newTestBridge(t *testing.T, sink *fakeSink, st store.Store) *Bridge {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(testConfig(), st, map[model.Platform]Sink{model.PlatformSlack: sink}, log)
}

func teamsMessage() model.Message {
	return model.Message{
		Platform: model.PlatformTeams,
		ID:       "teams-1",
		PairID:   "acme",
		Author:   model.Author{DisplayName: "Ivan Petrov"},
		Text:     "hello",
		Kind:     model.EventCreated,
	}
}

func TestHandleDeliversToOppositePlatform(t *testing.T) {
	sink := &fakeSink{nextID: "1700000000.000100"}
	st := newFakeStore()
	b := newTestBridge(t, sink, st)

	delivered, _, err := b.Handle(context.Background(), teamsMessage())
	if err != nil {
		t.Fatalf("Handle(): %v", err)
	}
	if !delivered {
		t.Fatal("delivered = false, want true")
	}
	if len(sink.calls) != 1 {
		t.Fatalf("sink received %d calls, want 1", len(sink.calls))
	}
	if sink.calls[0].channelID != "C01ABCDEF" {
		t.Fatalf("channelID = %q, want C01ABCDEF", sink.calls[0].channelID)
	}
	if sink.calls[0].threadTarget != "" {
		t.Fatalf("threadTarget = %q, want empty for a thread root", sink.calls[0].threadTarget)
	}
}

func TestHandleRecordsDelivery(t *testing.T) {
	sink := &fakeSink{nextID: "1700000000.000100"}
	st := newFakeStore()
	b := newTestBridge(t, sink, st)

	if _, _, err := b.Handle(context.Background(), teamsMessage()); err != nil {
		t.Fatalf("Handle(): %v", err)
	}
	got, err := st.LookupTarget(context.Background(), "acme", model.PlatformTeams, "teams-1")
	if err != nil {
		t.Fatalf("LookupTarget(): %v", err)
	}
	if got != "1700000000.000100" {
		t.Fatalf("recorded target = %q, want 1700000000.000100", got)
	}
}

func TestHandleDropsOwnMessages(t *testing.T) {
	sink := &fakeSink{}
	st := newFakeStore()
	b := newTestBridge(t, sink, st)

	msg := teamsMessage()
	msg.FromBridge = true
	delivered, reason, err := b.Handle(context.Background(), msg)
	if err != nil {
		t.Fatalf("Handle(): %v", err)
	}
	if delivered {
		t.Fatal("delivered = true, want false for a bridge-authored message")
	}
	if reason != ReasonFromBridge {
		t.Fatalf("reason = %q, want %q", reason, ReasonFromBridge)
	}
	if len(sink.calls) != 0 {
		t.Fatalf("sink received %d calls, want 0 for a bridge-authored message", len(sink.calls))
	}
}

func TestHandleDropsPreviouslyDeliveredMessages(t *testing.T) {
	sink := &fakeSink{}
	st := newFakeStore()
	// Pretend the bridge already posted teams-echo into Teams.
	if err := st.RecordDelivery(context.Background(), store.Delivery{
		PairID:         "acme",
		SourcePlatform: model.PlatformSlack,
		SourceID:       "slack-1",
		TargetPlatform: model.PlatformTeams,
		TargetID:       "teams-echo",
	}); err != nil {
		t.Fatalf("RecordDelivery(): %v", err)
	}
	b := newTestBridge(t, sink, st)

	msg := teamsMessage()
	msg.ID = "teams-echo"
	delivered, reason, err := b.Handle(context.Background(), msg)
	if err != nil {
		t.Fatalf("Handle(): %v", err)
	}
	if delivered {
		t.Fatal("delivered = true, want false for an echo of our own delivery")
	}
	if reason != ReasonEcho {
		t.Fatalf("reason = %q, want %q", reason, ReasonEcho)
	}
	if len(sink.calls) != 0 {
		t.Fatalf("sink received %d calls, want 0 — this is the echo loop", len(sink.calls))
	}
}

func TestHandleIsIdempotent(t *testing.T) {
	sink := &fakeSink{nextID: "1700000000.000100"}
	st := newFakeStore()
	b := newTestBridge(t, sink, st)

	for i := 0; i < 3; i++ {
		delivered, reason, err := b.Handle(context.Background(), teamsMessage())
		if err != nil {
			t.Fatalf("Handle() call %d: %v", i, err)
		}
		if i == 0 {
			if !delivered {
				t.Fatalf("call %d: delivered = false, want true for the first delivery", i)
			}
			continue
		}
		if delivered {
			t.Fatalf("call %d: delivered = true, want false for a repeat of an already-processed event", i)
		}
		if reason != ReasonDuplicate {
			t.Fatalf("call %d: reason = %q, want %q", i, reason, ReasonDuplicate)
		}
	}
	if len(sink.calls) != 1 {
		t.Fatalf("sink received %d calls, want 1 for three identical events", len(sink.calls))
	}
}

func TestHandleResolvesThreadTarget(t *testing.T) {
	sink := &fakeSink{nextID: "1700000000.000200"}
	st := newFakeStore()
	if err := st.RecordDelivery(context.Background(), store.Delivery{
		PairID:         "acme",
		SourcePlatform: model.PlatformTeams,
		SourceID:       "teams-parent",
		TargetPlatform: model.PlatformSlack,
		TargetID:       "1700000000.000100",
	}); err != nil {
		t.Fatalf("RecordDelivery(): %v", err)
	}
	b := newTestBridge(t, sink, st)

	msg := teamsMessage()
	msg.ID = "teams-reply"
	msg.ParentID = "teams-parent"
	if _, _, err := b.Handle(context.Background(), msg); err != nil {
		t.Fatalf("Handle(): %v", err)
	}
	if sink.calls[0].threadTarget != "1700000000.000100" {
		t.Fatalf("threadTarget = %q, want the parent's Slack ts", sink.calls[0].threadTarget)
	}
}

func TestHandleDeliversReplyToChannelWhenParentUnknown(t *testing.T) {
	sink := &fakeSink{nextID: "1700000000.000200"}
	st := newFakeStore()
	b := newTestBridge(t, sink, st)

	msg := teamsMessage()
	msg.ID = "teams-reply"
	msg.ParentID = "never-mirrored"
	if _, _, err := b.Handle(context.Background(), msg); err != nil {
		t.Fatalf("Handle(): %v", err)
	}
	if len(sink.calls) != 1 {
		t.Fatalf("sink received %d calls, want 1 — an orphan reply must still be delivered", len(sink.calls))
	}
	if sink.calls[0].threadTarget != "" {
		t.Fatalf("threadTarget = %q, want empty when the parent was never mirrored", sink.calls[0].threadTarget)
	}
}

func TestHandleRejectsUnknownPair(t *testing.T) {
	sink := &fakeSink{}
	st := newFakeStore()
	b := newTestBridge(t, sink, st)

	msg := teamsMessage()
	msg.PairID = "does-not-exist"
	delivered, _, err := b.Handle(context.Background(), msg)
	if err == nil {
		t.Fatal("Handle() error = nil, want an error for an unknown pair")
	}
	if delivered {
		t.Fatal("delivered = true, want false when Handle returns an error")
	}
	if !strings.Contains(err.Error(), "unknown pair") {
		t.Fatalf("error = %q, want it to mention the unknown pair", err)
	}
}

// A delivery the sink can prove did not happen must stay retriable. The error
// wraps ErrNotSent, which is what a platform answering and refusing looks
// like; an error without it means the outcome is unknown and the claim is kept
// instead, covered by TestHandleKeepsClaimWhenDeliveryOutcomeIsUnknown.
func TestHandleReleasesClaimWhenDeliveryFails(t *testing.T) {
	sink := &fakeSink{deliverErr: fmt.Errorf("slack is down: %w", ErrNotSent)}
	st := newFakeStore()
	b := newTestBridge(t, sink, st)

	if _, _, err := b.Handle(context.Background(), teamsMessage()); err == nil {
		t.Fatal("Handle() error = nil, want the sink error to surface")
	}

	// The retry must actually reach the sink rather than hitting a claim
	// left behind by the failed attempt.
	sink.deliverErr = nil
	sink.nextID = "1700000000.000100"
	if _, _, err := b.Handle(context.Background(), teamsMessage()); err != nil {
		t.Fatalf("retry Handle(): %v", err)
	}
	if len(sink.calls) != 2 {
		t.Fatalf("sink received %d calls, want 2 — a failed delivery must be retryable", len(sink.calls))
	}
	got, err := st.LookupTarget(context.Background(), "acme", model.PlatformTeams, "teams-1")
	if err != nil {
		t.Fatalf("LookupTarget(): %v", err)
	}
	if got != "1700000000.000100" {
		t.Fatalf("recorded target = %q, want the retry result", got)
	}
}

func TestHandleReleaseSurvivesCallerContextCancellation(t *testing.T) {
	sink := &fakeSink{deliverErr: fmt.Errorf("slack is down: %w", ErrNotSent)}
	st := newFakeStore()
	b := newTestBridge(t, sink, st)

	// The recovery sweep dispatches on the long-lived process context, not on
	// context.WithoutCancel like the webhook path. A SIGTERM mid-sweep
	// cancels this context before release() runs.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, _, err := b.Handle(ctx, teamsMessage()); err == nil {
		t.Fatal("Handle() error = nil, want the sink error to surface")
	}

	if st.unmarkCtx == nil {
		t.Fatal("UnmarkProcessed was never called")
	}
	if err := st.unmarkCtx.Err(); err != nil {
		t.Fatalf("UnmarkProcessed context.Err() = %v, want nil — "+
			"release() must run on a context that outlives the caller's cancellation", err)
	}

	// Prove the claim was actually released: a retry on a fresh context must
	// reach the sink instead of hitting a claim left behind by the cancelled
	// attempt.
	sink.deliverErr = nil
	sink.nextID = "1700000000.000100"
	if _, _, err := b.Handle(context.Background(), teamsMessage()); err != nil {
		t.Fatalf("retry Handle(): %v", err)
	}
	if len(sink.calls) != 2 {
		t.Fatalf("sink received %d calls, want 2 — the claim must be released despite the cancelled context", len(sink.calls))
	}
}

func TestHandleReleasesClaimWhenThreadLookupFails(t *testing.T) {
	sink := &fakeSink{nextID: "1700000000.000100"}
	st := newFakeStore()
	st.lookupErr = errors.New("database is down")
	b := newTestBridge(t, sink, st)

	msg := teamsMessage()
	msg.ID = "teams-reply"
	msg.ParentID = "teams-parent"

	if _, _, err := b.Handle(context.Background(), msg); err == nil {
		t.Fatal("Handle() error = nil, want the store error to surface")
	}
	if len(sink.calls) != 0 {
		t.Fatalf("sink received %d calls, want 0 — nothing was delivered", len(sink.calls))
	}

	// A store blip while resolving the thread parent must not consume the
	// event: the retry has to reach the sink.
	st.lookupErr = nil
	if err := st.RecordDelivery(context.Background(), store.Delivery{
		PairID:         "acme",
		SourcePlatform: model.PlatformTeams,
		SourceID:       "teams-parent",
		TargetPlatform: model.PlatformSlack,
		TargetID:       "1700000000.000001",
	}); err != nil {
		t.Fatalf("RecordDelivery(): %v", err)
	}
	if _, _, err := b.Handle(context.Background(), msg); err != nil {
		t.Fatalf("retry Handle(): %v", err)
	}
	if len(sink.calls) != 1 {
		t.Fatalf("sink received %d calls after retry, want 1", len(sink.calls))
	}
	if sink.calls[0].threadTarget != "1700000000.000001" {
		t.Fatalf("threadTarget = %q, want the parent's ts", sink.calls[0].threadTarget)
	}
}

func TestHandleSurfacesDeliveryErrorWhenClaimReleaseFails(t *testing.T) {
	sink := &fakeSink{deliverErr: errors.New("slack is down")}
	st := newFakeStore()
	st.unmarkErr = errors.New("database is down")
	b := newTestBridge(t, sink, st)

	_, _, err := b.Handle(context.Background(), teamsMessage())
	if err == nil {
		t.Fatal("Handle() error = nil, want an error")
	}
	// The delivery failure is the actionable one; a failed claim release is
	// logged but must not mask it.
	if !strings.Contains(err.Error(), "slack is down") {
		t.Fatalf("error = %q, want the delivery failure to surface", err)
	}
}

func TestHandleRejectsUnknownPlatform(t *testing.T) {
	cases := []struct {
		name     string
		platform model.Platform
	}{
		{name: "zero value", platform: model.Platform("")},
		{name: "unknown platform", platform: model.Platform("discord")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sink := &fakeSink{}
			st := newFakeStore()
			b := newTestBridge(t, sink, st)

			msg := teamsMessage()
			msg.Platform = tc.platform
			_, _, err := b.Handle(context.Background(), msg)
			if err == nil {
				t.Fatal("Handle() error = nil, want an error for an unroutable platform")
			}
			if !strings.Contains(err.Error(), "unknown source platform") {
				t.Fatalf("error = %q, want it to name the unknown platform", err)
			}
			if len(sink.calls) != 0 {
				t.Fatalf("sink received %d calls, want 0 — an unroutable message must never be delivered", len(sink.calls))
			}
		})
	}
}

// A change to a message the bridge never mirrored has nothing to act on: the
// original failed delivery, or it predates the pair. Dropping it is right;
// what matters is that the reason says so rather than blaming the event kind.
func TestHandleDropsChangesToUnmirroredMessages(t *testing.T) {
	cases := []struct {
		name string
		kind model.EventKind
	}{
		{name: "edited", kind: model.EventEdited},
		{name: "deleted", kind: model.EventDeleted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sink := &fakeSink{}
			st := newFakeStore()
			b := newTestBridge(t, sink, st)

			msg := teamsMessage()
			msg.Kind = tc.kind
			delivered, reason, err := b.Handle(context.Background(), msg)
			if err != nil {
				t.Fatalf("Handle(): %v", err)
			}
			if delivered {
				t.Fatalf("delivered = true, want false for a %s event", tc.kind)
			}
			if reason != ReasonUnmapped {
				t.Fatalf("reason = %q, want %q", reason, ReasonUnmapped)
			}
			if len(sink.calls) != 0 {
				t.Fatalf("sink received %d calls, want 0 for a %s event", len(sink.calls), tc.kind)
			}
			if len(st.deliveries) != 0 {
				t.Fatalf("recorded %d deliveries, want 0 for a %s event", len(st.deliveries), tc.kind)
			}
		})
	}
}

func TestHandleKeepsClaimWhenRecordDeliveryFails(t *testing.T) {
	sink := &fakeSink{nextID: "1700000000.000100"}
	st := newFakeStore()
	// Mirrors the real postgres.Store.RecordDelivery, which rejects a
	// conflicting target rather than silently keeping the first mapping.
	st.recordErr = errors.New("record delivery: teams message \"teams-1\" already maps to a different target")
	b := newTestBridge(t, sink, st)

	if _, _, err := b.Handle(context.Background(), teamsMessage()); err == nil {
		t.Fatal("Handle() error = nil, want the RecordDelivery failure to surface")
	}
	if len(sink.calls) != 1 {
		t.Fatalf("sink received %d calls, want 1 for the first attempt", len(sink.calls))
	}

	// The claim must be kept, not released: releasing it here would let a
	// retry post the message a second time into the customer's channel, which
	// is worse than the message merely not threading.
	st.recordErr = nil
	if _, _, err := b.Handle(context.Background(), teamsMessage()); err != nil {
		t.Fatalf("retry Handle(): %v", err)
	}
	if len(sink.calls) != 1 {
		t.Fatalf("sink received %d calls after retry, want 1 — "+
			"a RecordDelivery failure must not release the claim and cause a duplicate post", len(sink.calls))
	}
}

func TestHandleDoesNotRecordDeliveryWhenSinkFails(t *testing.T) {
	sink := &fakeSink{deliverErr: errors.New("slack is down")}
	st := newFakeStore()
	b := newTestBridge(t, sink, st)

	if _, _, err := b.Handle(context.Background(), teamsMessage()); err == nil {
		t.Fatal("Handle() error = nil, want the sink error to surface")
	}
	if _, err := st.LookupTarget(context.Background(), "acme", model.PlatformTeams, "teams-1"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("a failed delivery must not be recorded, got err = %v", err)
	}
}

// newBidirectionalBridge registers a sink for both platforms, the arrangement
// the service runs with once the Slack-to-Teams direction exists.
func newBidirectionalBridge(t *testing.T, slackSink, teamsSink *fakeSink, st store.Store) *Bridge {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(testConfig(), st, map[model.Platform]Sink{
		model.PlatformSlack: slackSink,
		model.PlatformTeams: teamsSink,
	}, log)
}

func slackMessage() model.Message {
	return model.Message{
		Platform: model.PlatformSlack,
		ID:       "1700000000.000100",
		PairID:   "acme",
		Author:   model.Author{DisplayName: "Ada Lovelace"},
		Text:     "hello",
		Kind:     model.EventCreated,
	}
}

// TestHandleDeliversSlackToTeams pins the direction Phase 2 adds: a Slack
// message must reach the Teams sink, addressed by the pair's Teams channel.
func TestHandleDeliversSlackToTeams(t *testing.T) {
	slackSink := &fakeSink{nextID: "slack-posted-1"}
	teamsSink := &fakeSink{nextID: "teams-posted-1"}
	b := newBidirectionalBridge(t, slackSink, teamsSink, newFakeStore())

	delivered, _, err := b.Handle(context.Background(), slackMessage())
	if err != nil {
		t.Fatalf("Handle(): %v", err)
	}
	if !delivered {
		t.Fatal("delivered = false, want true")
	}
	if len(teamsSink.calls) != 1 {
		t.Fatalf("teams sink received %d calls, want 1", len(teamsSink.calls))
	}
	if got := teamsSink.calls[0].channelID; got != "channel-1" {
		t.Fatalf("channelID = %q, want channel-1", got)
	}
	if len(slackSink.calls) != 0 {
		t.Fatal("a Slack message must not be delivered back into Slack")
	}
}

// TestLoopPreventionHoldsInBothDirections runs a message through each
// direction and then feeds the resulting post back in as the platform would
// report it. Neither may cross the bridge a second time: that is the loop the
// whole service exists to avoid, and with both sinks registered it can now
// actually close.
func TestLoopPreventionHoldsInBothDirections(t *testing.T) {
	slackSink := &fakeSink{nextID: "slack-posted-1"}
	teamsSink := &fakeSink{nextID: "teams-posted-1"}
	st := newFakeStore()
	b := newBidirectionalBridge(t, slackSink, teamsSink, st)

	// Teams to Slack, then the Slack post comes back on the Events API.
	if _, _, err := b.Handle(context.Background(), teamsMessage()); err != nil {
		t.Fatalf("Handle(teams): %v", err)
	}
	echoFromSlack := slackMessage()
	echoFromSlack.ID = "slack-posted-1"
	delivered, reason, err := b.Handle(context.Background(), echoFromSlack)
	if err != nil {
		t.Fatalf("Handle(slack echo): %v", err)
	}
	if delivered {
		t.Fatal("a message the bridge posted into Slack was mirrored back into Teams")
	}
	if reason != ReasonEcho {
		t.Fatalf("reason = %q, want %q", reason, ReasonEcho)
	}

	// Slack to Teams, then the Teams post comes back as a notification.
	if _, _, err := b.Handle(context.Background(), slackMessage()); err != nil {
		t.Fatalf("Handle(slack): %v", err)
	}
	echoFromTeams := teamsMessage()
	echoFromTeams.ID = "teams-posted-1"
	delivered, reason, err = b.Handle(context.Background(), echoFromTeams)
	if err != nil {
		t.Fatalf("Handle(teams echo): %v", err)
	}
	if delivered {
		t.Fatal("a message the bridge posted into Teams was mirrored back into Slack")
	}
	if reason != ReasonEcho {
		t.Fatalf("reason = %q, want %q", reason, ReasonEcho)
	}

	// Two deliveries in total, one per direction. A third would mean an echo
	// crossed.
	if len(slackSink.calls) != 1 {
		t.Fatalf("slack sink received %d calls, want 1", len(slackSink.calls))
	}
	if len(teamsSink.calls) != 1 {
		t.Fatalf("teams sink received %d calls, want 1", len(teamsSink.calls))
	}
}

// TestHandleThreadsReplyIntoAThreadTheFarSideStarted covers the dominant
// conversation shape once both directions are live: someone starts a thread on
// one platform and someone on the other platform answers in it. The root is
// then the *target* of the mapping, not its source, and resolving it forward
// only would deliver every such reply as a new top-level message.
func TestHandleThreadsReplyIntoAThreadTheFarSideStarted(t *testing.T) {
	t.Run("teams starts the thread, slack replies", func(t *testing.T) {
		slackSink := &fakeSink{nextID: "slack-root"}
		teamsSink := &fakeSink{nextID: "teams-reply"}
		b := newBidirectionalBridge(t, slackSink, teamsSink, newFakeStore())

		root := teamsMessage()
		root.ID = "teams-root"
		if _, _, err := b.Handle(context.Background(), root); err != nil {
			t.Fatalf("Handle(root): %v", err)
		}

		reply := slackMessage()
		reply.ID = "1700000000.000200"
		reply.ParentID = "slack-root"
		if _, _, err := b.Handle(context.Background(), reply); err != nil {
			t.Fatalf("Handle(reply): %v", err)
		}

		if len(teamsSink.calls) != 1 {
			t.Fatalf("teams sink received %d calls, want 1", len(teamsSink.calls))
		}
		if got := teamsSink.calls[0].threadTarget; got != "teams-root" {
			t.Fatalf("threadTarget = %q, want teams-root — the reply was not threaded", got)
		}
	})

	t.Run("slack starts the thread, teams replies", func(t *testing.T) {
		slackSink := &fakeSink{nextID: "slack-reply"}
		teamsSink := &fakeSink{nextID: "teams-root"}
		b := newBidirectionalBridge(t, slackSink, teamsSink, newFakeStore())

		root := slackMessage()
		root.ID = "1700000000.000300"
		if _, _, err := b.Handle(context.Background(), root); err != nil {
			t.Fatalf("Handle(root): %v", err)
		}

		reply := teamsMessage()
		reply.ID = "teams-reply-1"
		reply.ParentID = "teams-root"
		if _, _, err := b.Handle(context.Background(), reply); err != nil {
			t.Fatalf("Handle(reply): %v", err)
		}

		if len(slackSink.calls) != 1 {
			t.Fatalf("slack sink received %d calls, want 1", len(slackSink.calls))
		}
		if got := slackSink.calls[0].threadTarget; got != "1700000000.000300" {
			t.Fatalf("threadTarget = %q, want the Slack root ts — the reply was not threaded", got)
		}
	})
}

// TestHandleSerializesPerPair pins the ordering guarantee the design requires:
// "The delivery queue is serialized per channel." Without it a root and its
// reply arriving as two concurrent webhook POSTs race — the reply resolves its
// thread parent before the root has recorded its mapping — and the reply is
// delivered as a new top-level message, permanently detached from its thread.
//
// Both webhooks dispatch each request on its own goroutine, so concurrent
// Handle calls for one pair are the normal case, not an edge case.
func TestHandleSerializesPerPair(t *testing.T) {
	t.Run("a reply arriving mid-delivery waits for its root", func(t *testing.T) {
		// The real sequence: the root's post to the far platform is a network
		// call still in flight when the reply's notification arrives. Before
		// serialization the reply proceeded straight past it, found no
		// recorded parent, and was delivered detached.
		//
		// Serialization guarantees non-overlap, not send order — a mutex is
		// not FIFO, so two calls starting at the same instant may still be
		// handled in either order. What it does guarantee, and what matters in
		// production, is that a message arriving while another is being
		// delivered waits behind it.
		slackSink := &blockingSink{entered: make(chan struct{}), release: make(chan struct{}), id: "slack-root"}
		teamsSink := &fakeSink{nextID: "teams-reply"}
		b := newBidirectionalBridge(t, nil, teamsSink, newFakeStore())
		b.sinks[model.PlatformSlack] = slackSink

		root := teamsMessage()
		root.ID = "teams-root"
		reply := slackMessage()
		reply.ID = "1700000000.000900"
		reply.ParentID = "slack-root"

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, err := b.Handle(context.Background(), root); err != nil {
				t.Errorf("Handle(root): %v", err)
			}
		}()

		// The root is now inside its sink call, mid-delivery.
		<-slackSink.entered

		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, err := b.Handle(context.Background(), reply); err != nil {
				t.Errorf("Handle(reply): %v", err)
			}
		}()

		// Give the reply every chance to overtake. It must not reach its sink
		// while the root's delivery is still open.
		time.Sleep(100 * time.Millisecond)
		if n := len(teamsSink.calls); n != 0 {
			t.Fatalf("teams sink received %d calls while the root was still being delivered — "+
				"the reply overtook its own root", n)
		}

		close(slackSink.release)
		wg.Wait()

		if len(teamsSink.calls) != 1 {
			t.Fatalf("teams sink received %d calls, want 1", len(teamsSink.calls))
		}
		if got := teamsSink.calls[0].threadTarget; got != "teams-root" {
			t.Fatalf("threadTarget = %q, want teams-root", got)
		}
	})

	t.Run("different pairs are not serialized against each other", func(t *testing.T) {
		// One slow pair must not stall another. Two pairs deliver
		// concurrently; if the lock were global the second would block behind
		// the first and this would deadlock on the unbuffered channel.
		cfg := config.Config{Pairs: []config.Pair{
			{ID: "acme", Teams: &config.TeamsEndpoint{TeamID: "t1", ChannelID: "c1"}, Slack: &config.SlackEndpoint{ChannelID: "C1"}},
			{ID: "globex", Teams: &config.TeamsEndpoint{TeamID: "t2", ChannelID: "c2"}, Slack: &config.SlackEndpoint{ChannelID: "C2"}},
		}}
		both := make(chan struct{})
		sink := &gatedSink{arrived: make(chan string, 2), release: both}
		log := slog.New(slog.NewTextHandler(io.Discard, nil))
		b := New(cfg, newFakeStore(), map[model.Platform]Sink{model.PlatformSlack: sink}, log)

		for _, pairID := range []string{"acme", "globex"} {
			msg := teamsMessage()
			msg.PairID = pairID
			msg.ID = "teams-" + pairID
			go func() { _, _, _ = b.Handle(context.Background(), msg) }()
		}

		// Both deliveries must be in flight at once.
		for i := 0; i < 2; i++ {
			select {
			case <-sink.arrived:
			case <-time.After(3 * time.Second):
				t.Fatal("only one pair reached the sink; pairs are serialized against each other")
			}
		}
		close(both)
	})
}

// gatedSink announces its arrival and then waits, so a test can observe how
// many deliveries are in flight simultaneously.
type gatedSink struct {
	arrived chan string
	release chan struct{}
}

func (g *gatedSink) Deliver(_ context.Context, msg model.Message, _, _ string) (string, error) {
	g.arrived <- msg.PairID
	<-g.release
	return "delivered-" + msg.PairID, nil
}

// blockingSink parks inside Deliver until released, so a test can hold one
// delivery open and observe what another concurrent call does meanwhile.
type blockingSink struct {
	once    sync.Once
	entered chan struct{}
	release chan struct{}
	id      string
}

func (s *blockingSink) Deliver(_ context.Context, _ model.Message, _, _ string) (string, error) {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return s.id, nil
}

// TestHandleKeepsClaimWhenDeliveryOutcomeIsUnknown pins the distinction that
// keeps a message from being posted twice into a customer channel.
//
// A sink error does not mean the post failed. A read timeout, a connection
// reset or a 502 from an intermediary all arrive after the platform has
// already committed the message. Releasing the claim on those lets the
// recovery sweep post the same message a second time.
func TestHandleKeepsClaimWhenDeliveryOutcomeIsUnknown(t *testing.T) {
	t.Run("an ambiguous failure keeps the claim, so a retry cannot duplicate", func(t *testing.T) {
		// The sink records every post and then reports a lost response, which
		// is exactly what a post that landed but whose reply never arrived
		// looks like from here.
		sink := &fakeSink{deliverErr: errors.New("read tcp 10.0.0.1:443: i/o timeout")}
		st := newFakeStore()
		b := newTestBridge(t, sink, st)

		if _, _, err := b.Handle(context.Background(), teamsMessage()); err == nil {
			t.Fatal("Handle() error = nil, want the sink error to surface")
		}
		if len(sink.calls) != 1 {
			t.Fatalf("sink received %d calls, want 1", len(sink.calls))
		}

		// The recovery sweep re-dispatches the same message. It must not post
		// again: the first post may well have landed.
		if _, reason, err := b.Handle(context.Background(), teamsMessage()); err != nil {
			t.Fatalf("retry Handle(): %v", err)
		} else if reason != ReasonDuplicate {
			t.Fatalf("retry reason = %q, want %q", reason, ReasonDuplicate)
		}
		if len(sink.calls) != 1 {
			t.Fatalf("sink received %d calls after retry, want 1 — "+
				"an ambiguous failure released its claim and duplicated the message", len(sink.calls))
		}
	})

	t.Run("a provably unsent failure releases the claim, so the message is retried", func(t *testing.T) {
		// The platform answered and refused: nothing was posted, and the
		// message must stay retriable or it is lost for good.
		sink := &fakeSink{deliverErr: fmt.Errorf("slack rejected the post: %w", ErrNotSent)}
		st := newFakeStore()
		b := newTestBridge(t, sink, st)

		if _, _, err := b.Handle(context.Background(), teamsMessage()); err == nil {
			t.Fatal("Handle() error = nil, want the sink error to surface")
		}

		// Second attempt must reach the sink again.
		sink.deliverErr = nil
		sink.nextID = "1700000000.000100"
		delivered, _, err := b.Handle(context.Background(), teamsMessage())
		if err != nil {
			t.Fatalf("retry Handle(): %v", err)
		}
		if !delivered {
			t.Fatal("delivered = false on retry, want true — the claim was never released")
		}
		if len(sink.calls) != 2 {
			t.Fatalf("sink received %d calls, want 2", len(sink.calls))
		}
	})
}

// A third platform is the reason routing moved onto the pair. These tests pin
// what Platform.Opposite could not express: the target comes from the pair a
// message belongs to, and a pair that does not carry the source platform is an
// error rather than a delivery into whatever platform the old helper defaulted
// to.
func signalPairConfig() config.Config {
	return config.Config{Pairs: []config.Pair{
		{
			ID:    "acme",
			Teams: &config.TeamsEndpoint{TeamID: "team-1", ChannelID: "channel-1"},
			Slack: &config.SlackEndpoint{ChannelID: "C01ABCDEF"},
		},
		{
			ID:     "acme-signal",
			Signal: &config.SignalEndpoint{GroupID: "Ci8KIExhbXBz"},
			Slack:  &config.SlackEndpoint{ChannelID: "C02SIGNAL"},
		},
	}}
}

func signalMessage() model.Message {
	return model.Message{
		Platform: model.PlatformSignal,
		ID:       "8f5ac0de-0000-4000-8000-000000000001:1754400000000",
		PairID:   "acme-signal",
		Author:   model.Author{DisplayName: "Ada Lovelace"},
		Text:     "hello from signal",
		Kind:     model.EventCreated,
	}
}

func TestHandleRoutesSignalToItsOwnSlackChannel(t *testing.T) {
	slackSink := &fakeSink{nextID: "1700000000.000200"}
	signalSink := &fakeSink{nextID: "signal-posted-1"}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := New(signalPairConfig(), newFakeStore(), map[model.Platform]Sink{
		model.PlatformSlack:  slackSink,
		model.PlatformSignal: signalSink,
	}, log)

	delivered, _, err := b.Handle(context.Background(), signalMessage())
	if err != nil {
		t.Fatalf("Handle(): %v", err)
	}
	if !delivered {
		t.Fatal("delivered = false, want true")
	}
	if len(slackSink.calls) != 1 {
		t.Fatalf("slack sink received %d calls, want 1", len(slackSink.calls))
	}
	// C02SIGNAL, not the Teams pair's C01ABCDEF: the channel comes from the
	// message's own pair.
	if got := slackSink.calls[0].channelID; got != "C02SIGNAL" {
		t.Fatalf("channelID = %q, want C02SIGNAL", got)
	}
	if len(signalSink.calls) != 0 {
		t.Fatalf("signal sink received %d calls, want 0", len(signalSink.calls))
	}
}

func TestHandleRejectsAPlatformThePairDoesNotCarry(t *testing.T) {
	slackSink := &fakeSink{nextID: "1700000000.000300"}
	teamsSink := &fakeSink{nextID: "teams-posted-1"}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := New(signalPairConfig(), newFakeStore(), map[model.Platform]Sink{
		model.PlatformSlack: slackSink,
		model.PlatformTeams: teamsSink,
	}, log)

	// A Teams message tagged with the Signal pair. Opposite() would have
	// answered "slack" without ever consulting the pair, and the message would
	// have landed in a channel that has nothing to do with it.
	msg := teamsMessage()
	msg.PairID = "acme-signal"

	delivered, _, err := b.Handle(context.Background(), msg)
	if err == nil {
		t.Fatal("Handle() error = nil, want an error naming the pair and platform")
	}
	if delivered {
		t.Fatal("delivered = true, want false")
	}
	if !strings.Contains(err.Error(), "acme-signal") || !strings.Contains(err.Error(), "teams") {
		t.Fatalf("error = %q, want it to name the pair and the platform", err)
	}
	if len(slackSink.calls) != 0 || len(teamsSink.calls) != 0 {
		t.Fatalf("sinks received %d/%d calls, want none",
			len(slackSink.calls), len(teamsSink.calls))
	}
}

// Signal has no threads: a person quotes whatever message they tapped, which
// is regularly one from the middle of a thread this bridge created. Slack
// addresses a thread by its root, so a reply to a reply has to resolve back to
// that root — otherwise it opens a second thread beside the conversation it
// answers.
func TestHandleThreadsAReplyToAReplyOntoTheThreadRoot(t *testing.T) {
	sink := &fakeSink{}
	st := newFakeStore()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := New(signalPairConfig(), st, map[model.Platform]Sink{model.PlatformSlack: sink}, log)

	root := signalMessage()
	root.ID = "uuid-a:1754400000000"
	sink.nextID = "1700000000.000100"
	if _, _, err := b.Handle(context.Background(), root); err != nil {
		t.Fatalf("Handle(root): %v", err)
	}

	reply := signalMessage()
	reply.ID = "uuid-b:1754400000001"
	reply.ParentID = root.ID
	sink.nextID = "1700000000.000200"
	if _, _, err := b.Handle(context.Background(), reply); err != nil {
		t.Fatalf("Handle(reply): %v", err)
	}
	if got := sink.calls[1].threadTarget; got != "1700000000.000100" {
		t.Fatalf("reply threadTarget = %q, want the root's ts", got)
	}

	// A quote of the reply, not of the root.
	replyToReply := signalMessage()
	replyToReply.ID = "uuid-c:1754400000002"
	replyToReply.ParentID = reply.ID
	sink.nextID = "1700000000.000300"
	if _, _, err := b.Handle(context.Background(), replyToReply); err != nil {
		t.Fatalf("Handle(reply to reply): %v", err)
	}
	if got := sink.calls[2].threadTarget; got != "1700000000.000100" {
		t.Fatalf("threadTarget = %q, want the thread root 1700000000.000100 — "+
			"Slack threads are one level deep and are addressed by their root", got)
	}
}

// The antiloop test, at the level the core can express it: a message crosses
// once and stops. Layer one is the platform's own marker on posts we made;
// layer two is this store, which is what actually holds when a platform gives
// us nothing to recognise ourselves by.
func TestHandleStopsAMessageAfterOneCrossing(t *testing.T) {
	// Each sink is keyed by the platform it delivers *to*.
	signalSink := &fakeSink{nextID: "signal-uuid:1754400000000"}
	slackSink := &fakeSink{nextID: "1700000000.000100"}
	st := newFakeStore()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := New(signalPairConfig(), st, map[model.Platform]Sink{
		model.PlatformSignal: signalSink,
		model.PlatformSlack:  slackSink,
	}, log)

	// A Slack message crosses into Signal.
	fromSlack := model.Message{
		Platform: model.PlatformSlack,
		ID:       "1700000000.000900",
		PairID:   "acme-signal",
		Author:   model.Author{DisplayName: "Timur"},
		Text:     "hello",
		Kind:     model.EventCreated,
	}
	delivered, _, err := b.Handle(context.Background(), fromSlack)
	if err != nil || !delivered {
		t.Fatalf("Handle(slack): delivered = %v, err = %v", delivered, err)
	}

	// Signal echoes it back: a sync transcript of our own send arrives on the
	// same conversation, carrying the id we just recorded as a delivery.
	echo := model.Message{
		Platform: model.PlatformSignal,
		ID:       "signal-uuid:1754400000000",
		PairID:   "acme-signal",
		Author:   model.Author{DisplayName: "Bridge"},
		Text:     "Timur\nhello",
		Kind:     model.EventCreated,
	}
	delivered, reason, err := b.Handle(context.Background(), echo)
	if err != nil {
		t.Fatalf("Handle(echo): %v", err)
	}
	if delivered {
		t.Fatal("the echo was mirrored back into Slack; the bridge would flood both sides")
	}
	if reason != ReasonEcho {
		t.Fatalf("reason = %q, want %q", reason, ReasonEcho)
	}
	if len(slackSink.calls) != 0 {
		t.Fatalf("slack sink received %d calls, want 0", len(slackSink.calls))
	}
}

// capableSink implements every optional capability and records what it was
// asked to do.
type capableSink struct {
	fakeSink
	updates   []model.Message
	deletes   []string
	reactions []model.Reaction
	updateErr error
	reactErr  error
}

func (c *capableSink) Update(_ context.Context, msg model.Message, _, targetID string) error {
	c.updates = append(c.updates, msg)
	_ = targetID
	return c.updateErr
}

func (c *capableSink) Delete(_ context.Context, _, targetID string) error {
	c.deletes = append(c.deletes, targetID)
	return nil
}

func (c *capableSink) React(_ context.Context, r model.Reaction, _, _ string) error {
	c.reactions = append(c.reactions, r)
	return c.reactErr
}

// mirror delivers a message so the tests below have something to change.
func mirror(t *testing.T, b *Bridge, sink *capableSink, id, targetID string) model.Message {
	t.Helper()
	msg := signalMessage()
	msg.ID = id
	sink.nextID = targetID
	if _, _, err := b.Handle(context.Background(), msg); err != nil {
		t.Fatalf("Handle(original): %v", err)
	}
	return msg
}

func newCapableBridge(t *testing.T) (*Bridge, *capableSink, *fakeStore) {
	t.Helper()
	sink := &capableSink{}
	st := newFakeStore()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := New(signalPairConfig(), st, map[model.Platform]Sink{model.PlatformSlack: sink}, log)
	return b, sink, st
}

func TestHandleAppliesAnEditInPlace(t *testing.T) {
	b, sink, _ := newCapableBridge(t)
	original := mirror(t, b, sink, "uuid-a:1", "1700000000.000100")

	edit := original
	edit.ID = "uuid-a:2"
	edit.EditsID = original.ID
	edit.Kind = model.EventEdited
	edit.Text = "corrected"

	delivered, _, err := b.Handle(context.Background(), edit)
	if err != nil {
		t.Fatalf("Handle(edit): %v", err)
	}
	if !delivered {
		t.Fatal("delivered = false, want true")
	}
	if len(sink.updates) != 1 || sink.updates[0].Text != "corrected" {
		t.Fatalf("updates = %+v, want one carrying the new text", sink.updates)
	}
	// An in-place edit posts nothing: only the original delivery is a message.
	if len(sink.calls) != 1 {
		t.Fatalf("sink posted %d messages, want 1", len(sink.calls))
	}
}

func TestHandleAppliesADelete(t *testing.T) {
	b, sink, _ := newCapableBridge(t)
	original := mirror(t, b, sink, "uuid-a:1", "1700000000.000100")

	del := original
	del.Kind = model.EventDeleted
	delivered, _, err := b.Handle(context.Background(), del)
	if err != nil {
		t.Fatalf("Handle(delete): %v", err)
	}
	if !delivered {
		t.Fatal("delivered = false, want true")
	}
	if len(sink.deletes) != 1 || sink.deletes[0] != "1700000000.000100" {
		t.Fatalf("deletes = %v, want the mirrored message", sink.deletes)
	}
}

// Past a platform's change window the edit is accepted and ignored by every
// client, so the reader would keep seeing a number the author has retracted.
// An annotation is worse than an in-place fix and far better than silence.
func TestHandleAnnotatesAChangeItCannotApply(t *testing.T) {
	cases := []struct {
		name     string
		kind     model.EventKind
		sinkErr  error
		wantText string
	}{
		{
			name:     "edit outside the window",
			kind:     model.EventEdited,
			sinkErr:  ErrOutOfWindow,
			wantText: "✅ Corrected: corrected",
		},
		{
			name:     "sink cannot edit at all",
			kind:     model.EventEdited,
			sinkErr:  ErrUnsupported,
			wantText: "✅ Corrected: corrected",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, sink, _ := newCapableBridge(t)
			original := mirror(t, b, sink, "uuid-a:1", "1700000000.000100")
			sink.updateErr = tc.sinkErr

			edit := original
			edit.ID = "uuid-a:2"
			edit.EditsID = original.ID
			edit.Kind = tc.kind
			edit.Text = "corrected"

			delivered, _, err := b.Handle(context.Background(), edit)
			if err != nil {
				t.Fatalf("Handle(): %v", err)
			}
			if !delivered {
				t.Fatal("delivered = false, want true — the annotation is the delivery")
			}
			if len(sink.calls) != 2 {
				t.Fatalf("sink posted %d messages, want 2 (original plus annotation)", len(sink.calls))
			}
			annotation := sink.calls[1]
			if annotation.msg.Text != tc.wantText {
				t.Fatalf("annotation = %q, want %q", annotation.msg.Text, tc.wantText)
			}
			// It belongs in the thread of the message it corrects, not at the
			// bottom of the channel where nobody connects the two.
			if annotation.threadTarget != "1700000000.000100" {
				t.Fatalf("threadTarget = %q, want the corrected message", annotation.threadTarget)
			}
		})
	}
}

func TestHandleReactionMirrorsAndRecords(t *testing.T) {
	b, sink, st := newCapableBridge(t)
	original := mirror(t, b, sink, "uuid-a:1", "1700000000.000100")

	delivered, _, err := b.HandleReaction(context.Background(), model.Reaction{
		Platform: model.PlatformSignal,
		PairID:   original.PairID,
		Author:   model.Author{ExternalID: "uuid-b"},
		Emoji:    "👍",
		TargetID: original.ID,
	})
	if err != nil {
		t.Fatalf("HandleReaction(): %v", err)
	}
	if !delivered {
		t.Fatal("delivered = false, want true")
	}
	if len(sink.reactions) != 1 || sink.reactions[0].Emoji != "👍" {
		t.Fatalf("reactions = %+v", sink.reactions)
	}
	if len(st.reactions) != 1 {
		t.Fatalf("recorded %d reactions, want 1", len(st.reactions))
	}
}

// This is the rule that keeps a customer's view honest on a platform holding
// one reaction per sender: when one person withdraws theirs and others remain,
// the newest survivor is re-asserted instead of everything disappearing.
func TestHandleReactionReassertsTheSurvivorOnWithdrawal(t *testing.T) {
	b, sink, _ := newCapableBridge(t)
	original := mirror(t, b, sink, "uuid-a:1", "1700000000.000100")

	react := func(author, emoji string, remove bool) {
		t.Helper()
		if _, _, err := b.HandleReaction(context.Background(), model.Reaction{
			Platform: model.PlatformSignal,
			PairID:   original.PairID,
			Author:   model.Author{ExternalID: author},
			Emoji:    emoji,
			TargetID: original.ID,
			Remove:   remove,
		}); err != nil {
			t.Fatalf("HandleReaction(%s %s): %v", author, emoji, err)
		}
	}

	react("uuid-b", "👍", false)
	react("uuid-c", "🎉", false)
	react("uuid-c", "🎉", true)

	if len(sink.reactions) != 3 {
		t.Fatalf("sink saw %d reactions, want 3", len(sink.reactions))
	}
	last := sink.reactions[2]
	if last.Remove {
		t.Fatal("the bridge withdrew its reaction; the other person's 👍 vanished from the customer's view")
	}
	if last.Emoji != "👍" {
		t.Fatalf("re-asserted %q, want the surviving 👍", last.Emoji)
	}
}

func TestHandleReactionWithdrawsWhenNothingSurvives(t *testing.T) {
	b, sink, _ := newCapableBridge(t)
	original := mirror(t, b, sink, "uuid-a:1", "1700000000.000100")

	add := model.Reaction{
		Platform: model.PlatformSignal,
		PairID:   original.PairID,
		Author:   model.Author{ExternalID: "uuid-b"},
		Emoji:    "👍",
		TargetID: original.ID,
	}
	if _, _, err := b.HandleReaction(context.Background(), add); err != nil {
		t.Fatalf("HandleReaction(add): %v", err)
	}
	remove := add
	remove.Remove = true
	if _, _, err := b.HandleReaction(context.Background(), remove); err != nil {
		t.Fatalf("HandleReaction(remove): %v", err)
	}

	if len(sink.reactions) != 2 || !sink.reactions[1].Remove {
		t.Fatalf("reactions = %+v, want the second to be a withdrawal", sink.reactions)
	}
}

func TestHandleReactionCountsOverwrites(t *testing.T) {
	b, sink, _ := newCapableBridge(t)
	original := mirror(t, b, sink, "uuid-a:1", "1700000000.000100")

	var overwritten []string
	b.OnReactionOverwritten(func(pairID string) { overwritten = append(overwritten, pairID) })

	for _, e := range []struct{ author, emoji string }{{"uuid-b", "👍"}, {"uuid-c", "🎉"}} {
		if _, _, err := b.HandleReaction(context.Background(), model.Reaction{
			Platform: model.PlatformSignal,
			PairID:   original.PairID,
			Author:   model.Author{ExternalID: e.author},
			Emoji:    e.emoji,
			TargetID: original.ID,
		}); err != nil {
			t.Fatalf("HandleReaction(): %v", err)
		}
	}

	if len(overwritten) != 1 {
		t.Fatalf("counted %d overwrites, want 1 — the 🎉 displaced the 👍", len(overwritten))
	}
	_ = sink
}

func TestHandleReactionDrops(t *testing.T) {
	cases := []struct {
		name     string
		mutate   func(*model.Reaction)
		sinkErr  error
		wantWhy  string
		wantSent bool
	}{
		{
			name:    "a reaction to a message we never mirrored",
			mutate:  func(r *model.Reaction) { r.TargetID = "uuid-z:999" },
			wantWhy: ReasonUnmapped,
		},
		{
			name:    "a reaction the bridge placed itself",
			mutate:  func(r *model.Reaction) { r.FromBridge = true },
			wantWhy: ReasonFromBridge,
		},
		{
			name:    "an emoji the target platform has no form for",
			sinkErr: ErrUnsupported,
			wantWhy: ReasonUnsupportedEmoji,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, sink, _ := newCapableBridge(t)
			original := mirror(t, b, sink, "uuid-a:1", "1700000000.000100")
			sink.reactErr = tc.sinkErr

			r := model.Reaction{
				Platform: model.PlatformSignal,
				PairID:   original.PairID,
				Author:   model.Author{ExternalID: "uuid-b"},
				Emoji:    "👍",
				TargetID: original.ID,
			}
			if tc.mutate != nil {
				tc.mutate(&r)
			}
			delivered, reason, err := b.HandleReaction(context.Background(), r)
			if err != nil {
				t.Fatalf("HandleReaction(): %v", err)
			}
			if delivered {
				t.Fatal("delivered = true, want false")
			}
			if reason != tc.wantWhy {
				t.Fatalf("reason = %q, want %q", reason, tc.wantWhy)
			}
		})
	}
}

// A reaction the bridge placed comes back as an ordinary event on the far
// side. Mirroring it would put it back where it came from, forever.
func TestHandleReactionDropsItsOwnEcho(t *testing.T) {
	b, sink, _ := newCapableBridge(t)
	original := mirror(t, b, sink, "uuid-a:1", "1700000000.000100")

	if _, _, err := b.HandleReaction(context.Background(), model.Reaction{
		Platform: model.PlatformSignal,
		PairID:   original.PairID,
		Author:   model.Author{ExternalID: "uuid-b"},
		Emoji:    "👍",
		TargetID: original.ID,
	}); err != nil {
		t.Fatalf("HandleReaction(): %v", err)
	}

	// Slack now reports the same reaction on the mirrored message.
	delivered, reason, err := b.HandleReaction(context.Background(), model.Reaction{
		Platform: model.PlatformSlack,
		PairID:   original.PairID,
		Author:   model.Author{ExternalID: "U0BRIDGE"},
		Emoji:    "👍",
		TargetID: "1700000000.000100",
	})
	if err != nil {
		t.Fatalf("HandleReaction(echo): %v", err)
	}
	if delivered {
		t.Fatal("the bridge mirrored its own reaction back")
	}
	if reason != ReasonEcho {
		t.Fatalf("reason = %q, want %q", reason, ReasonEcho)
	}
}

// An annotation is a new message about another one, not a copy of it. Copying
// the whole message carried the original's attachments — uploading the file a
// second time — and its formatting ranges, which are offsets into a body this
// message no longer has.
func TestAnnotationDoesNotInheritTheOriginalsPayload(t *testing.T) {
	b, sink, _ := newCapableBridge(t)

	original := signalMessage()
	original.ID = "uuid-a:1"
	original.Text = "see the report"
	original.Attachments = []model.Attachment{{Name: "q3.pdf", SourceURL: "att-1"}}
	original.TextStyles = []model.TextStyle{{Style: model.StyleBold, Start: 4, Length: 3}}
	original.Mentions = []model.Mention{{ExternalID: "uuid-b", DisplayName: "Ada", Start: 0, Length: 1}}
	sink.nextID = "1700000000.000100"
	if _, _, err := b.Handle(context.Background(), original); err != nil {
		t.Fatalf("Handle(original): %v", err)
	}

	sink.updateErr = ErrOutOfWindow
	edit := original
	edit.ID = "uuid-a:2"
	edit.EditsID = original.ID
	edit.Kind = model.EventEdited
	edit.Text = "see the corrected report"

	if _, _, err := b.Handle(context.Background(), edit); err != nil {
		t.Fatalf("Handle(edit): %v", err)
	}
	if len(sink.calls) != 2 {
		t.Fatalf("sink posted %d messages, want 2", len(sink.calls))
	}
	annotation := sink.calls[1].msg
	if len(annotation.Attachments) != 0 {
		t.Fatalf("the annotation carried %d attachments; the file would be uploaded twice",
			len(annotation.Attachments))
	}
	if len(annotation.TextStyles) != 0 || len(annotation.Mentions) != 0 {
		t.Fatalf("the annotation carried ranges into a body it does not have: styles=%v mentions=%v",
			annotation.TextStyles, annotation.Mentions)
	}
	if annotation.Author.DisplayName != original.Author.DisplayName {
		t.Fatalf("the annotation lost its author: %+v", annotation.Author)
	}
}

// Slack addresses a thread by its root and warns against naming one of its
// replies. Signal has no threads and quotes whatever message a person tapped
// — regularly a reply this bridge posted into a Slack thread. Without the
// source-side root travelling with the mapping, the answer to that quote goes
// back to Slack naming a reply, which Slack's own documentation says to avoid.
func TestHandleResolvesBackToTheSlackThreadRoot(t *testing.T) {
	slackSink := &fakeSink{}
	signalSink := &fakeSink{}
	st := newFakeStore()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := New(signalPairConfig(), st, map[model.Platform]Sink{
		model.PlatformSlack:  slackSink,
		model.PlatformSignal: signalSink,
	}, log)

	fromSlack := func(ts, threadTS, signalID string) {
		t.Helper()
		signalSink.nextID = signalID
		if _, _, err := b.Handle(context.Background(), model.Message{
			Platform:     model.PlatformSlack,
			ID:           ts,
			ParentID:     threadTS,
			ThreadRootID: threadTS,
			PairID:       "acme-signal",
			Author:       model.Author{DisplayName: "Timur"},
			Text:         "hello",
			Kind:         model.EventCreated,
		}); err != nil {
			t.Fatalf("Handle(slack %s): %v", ts, err)
		}
	}

	// A thread in Slack: a root and a reply, both mirrored into Signal.
	fromSlack("1700000000.000100", "", "uuid-self:11")
	fromSlack("1700000000.000200", "1700000000.000100", "uuid-self:22")

	// Somebody in Signal quotes the mirrored *reply* and answers it.
	signalSink.nextID = ""
	slackSink.nextID = "1700000000.000300"
	if _, _, err := b.Handle(context.Background(), model.Message{
		Platform: model.PlatformSignal,
		ID:       "uuid-ada:33",
		ParentID: "uuid-self:22",
		PairID:   "acme-signal",
		Author:   model.Author{DisplayName: "Ada"},
		Text:     "answering the reply",
		Kind:     model.EventCreated,
	}); err != nil {
		t.Fatalf("Handle(signal): %v", err)
	}

	if len(slackSink.calls) != 1 {
		t.Fatalf("slack sink received %d calls, want 1", len(slackSink.calls))
	}
	if got := slackSink.calls[0].threadTarget; got != "1700000000.000100" {
		t.Fatalf("threadTarget = %q, want the thread root 1700000000.000100 — "+
			"Slack documents that naming a reply instead is to be avoided", got)
	}
}

// limitedSink is a capableSink whose platform bounds edits, like Signal's.
type limitedSink struct {
	capableSink
	max int
}

func (l *limitedSink) MaxEdits() int { return l.max }

// Signal accepts ten edits of a message and ignores the rest, which a sender
// discovers only by reading their own message back. Past the limit the change
// has to become an annotation, or the customer keeps reading a number the
// author has already corrected.
func TestHandleAnnotatesOnceTheEditLimitIsReached(t *testing.T) {
	sink := &limitedSink{max: 2}
	st := newFakeStore()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := New(signalPairConfig(), st, map[model.Platform]Sink{model.PlatformSlack: sink}, log)

	original := signalMessage()
	original.ID = "uuid-a:1"
	sink.nextID = "1700000000.000100"
	if _, _, err := b.Handle(context.Background(), original); err != nil {
		t.Fatalf("Handle(original): %v", err)
	}

	edit := func(n int) {
		t.Helper()
		e := original
		e.ID = fmt.Sprintf("uuid-a:%d", n+1)
		e.EditsID = original.ID
		e.Kind = model.EventEdited
		e.Text = fmt.Sprintf("correction %d", n)
		if _, _, err := b.Handle(context.Background(), e); err != nil {
			t.Fatalf("Handle(edit %d): %v", n, err)
		}
	}

	edit(1)
	edit(2)
	if len(sink.updates) != 2 {
		t.Fatalf("applied %d edits in place, want 2", len(sink.updates))
	}
	if len(sink.calls) != 1 {
		t.Fatalf("posted %d messages, want 1 — nothing should be annotated yet", len(sink.calls))
	}

	edit(3)
	if len(sink.updates) != 2 {
		t.Fatalf("applied %d edits in place, want 2 — the third is past the platform's limit",
			len(sink.updates))
	}
	if len(sink.calls) != 2 {
		t.Fatalf("posted %d messages, want 2 — the third edit becomes an annotation", len(sink.calls))
	}
	if got := sink.calls[1].msg.Text; got != "✅ Corrected: correction 3" {
		t.Fatalf("annotation = %q", got)
	}
}

// An annotation is a post like any other. A change that failed to apply may
// be replayed — it names the message it acts on — but a post whose outcome is
// unknown may not, or the reader sees the same correction twice.
func TestFailedAnnotationIsNotReplayedBlindly(t *testing.T) {
	cases := []struct {
		name        string
		deliverErr  error
		wantRelease bool
		why         string
	}{
		{
			name:        "the sink proved nothing was posted",
			deliverErr:  fmt.Errorf("refused: %w", ErrNotSent),
			wantRelease: true,
			why:         "nothing reached the channel, so a repeat cannot duplicate",
		},
		{
			name:        "the outcome is unknown",
			deliverErr:  errors.New("connection reset"),
			wantRelease: false,
			why:         "the annotation may have landed with only its answer lost",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, sink, st := newCapableBridge(t)
			original := mirror(t, b, sink, "uuid-a:1", "1700000000.000100")

			sink.updateErr = ErrOutOfWindow
			sink.deliverErr = tc.deliverErr

			edit := original
			edit.ID = "uuid-a:2"
			edit.EditsID = original.ID
			edit.Kind = model.EventEdited
			edit.Text = "corrected"

			if _, _, err := b.Handle(context.Background(), edit); err == nil {
				t.Fatal("Handle() error = nil, want the annotation failure to surface")
			}

			claimed := st.wasProcessed(model.PlatformSignal, edit.ID, model.EventEdited)
			if claimed == tc.wantRelease {
				t.Fatalf("claim released = %v, want %v — %s", !claimed, tc.wantRelease, tc.why)
			}
		})
	}
}

// The record has to keep describing what the platforms show until the
// platform has actually been told otherwise. Removing the row first and
// failing the call would strand a reaction nobody has a record of — which
// loop prevention then reads as a stranger's and mirrors back.
func TestWithdrawalKeepsItsRecordWhenTheCallFails(t *testing.T) {
	b, sink, st := newCapableBridge(t)
	original := mirror(t, b, sink, "uuid-a:1", "1700000000.000100")

	add := model.Reaction{
		Platform: model.PlatformSignal,
		PairID:   original.PairID,
		Author:   model.Author{ExternalID: "uuid-b"},
		Emoji:    "👍",
		TargetID: original.ID,
	}
	if _, _, err := b.HandleReaction(context.Background(), add); err != nil {
		t.Fatalf("HandleReaction(add): %v", err)
	}

	sink.reactErr = errors.New("connection reset")
	remove := add
	remove.Remove = true
	if _, _, err := b.HandleReaction(context.Background(), remove); err == nil {
		t.Fatal("HandleReaction(remove) error = nil, want the failure to surface")
	}

	if len(st.reactions) != 1 {
		t.Fatalf("the record was dropped even though the platform was never told: %+v", st.reactions)
	}
}

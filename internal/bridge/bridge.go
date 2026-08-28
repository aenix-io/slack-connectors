// Package bridge routes canonical messages between platforms. It owns loop
// prevention, idempotency and thread correlation, and imports no platform SDK.
package bridge

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/aenix-io/slack-connectors/internal/config"
	"github.com/aenix-io/slack-connectors/internal/model"
	"github.com/aenix-io/slack-connectors/internal/store"
)

// Drop reasons returned by Handle when it declines to route a message. Named
// so main.go can label the bridge_dropped_total metric without duplicating
// the strings.
const (
	ReasonFromBridge = "from_bridge"
	ReasonEcho       = "echo"
	ReasonDuplicate  = "duplicate"
	ReasonNotCreated = "not_created"
	// ReasonUnmapped covers a change or a reaction naming a message this
	// bridge never mirrored: it failed delivery, or it predates the pair.
	ReasonUnmapped = "unmapped"
	// ReasonUnsupportedEmoji covers a reaction with no equivalent on the
	// target platform — a Slack custom emoji has no unicode to send.
	ReasonUnsupportedEmoji = "unsupported_emoji"
)

// ErrOutOfWindow marks a change the target platform will accept but its
// clients will ignore, because the message is older than the platform allows
// editing or deleting. Signal's window is 24 hours.
var ErrOutOfWindow = errors.New("the message is outside the platform's change window")

// ErrUnsupported marks a change the sink cannot make at all — application-only
// Graph cannot edit or delete a Teams message. Both are answered with an
// annotation rather than silence.
var ErrUnsupported = errors.New("the platform cannot apply this change")

// ErrNotSent marks a delivery failure where the message provably did not
// reach the target platform — the platform answered and refused, or the
// request was never put on the wire.
//
// A sink that cannot tell must not wrap this. An error alone does not mean the
// post failed: a read timeout, a connection reset and a 502 from an
// intermediary all arrive after the platform has already committed the
// message. Treating those as failures releases the event claim and lets the
// recovery sweep post the same message a second time, which is the worst
// outcome this bridge has.
var ErrNotSent = errors.New("message was not sent")

// Sink delivers a canonical message to a platform. threadTarget is the
// platform-native id of the thread to reply into, or empty for a new thread.
//
// A returned error should wrap ErrNotSent when, and only when, the sink knows
// the message did not reach the platform.
type Sink interface {
	Deliver(ctx context.Context, msg model.Message, channelID, threadTarget string) (string, error)
}

// Editor, Deleter and Reactor are the optional capabilities a Sink may also
// implement. They are separate interfaces rather than methods on Sink because
// the platforms genuinely differ: application-only Graph cannot edit or delete
// a Teams message at all, and a sink that had to declare methods it cannot
// honour would be answering "no" at runtime, where the core cannot see it.
//
// The core type-asserts. A sink implementing none of these behaves exactly as
// the Teams sink does today: events other than a creation are dropped with
// ReasonNotCreated, and the caller counts the drop.
type Editor interface {
	Update(ctx context.Context, msg model.Message, channelID, targetID string) error
}

// Deleter removes a message previously delivered by this sink.
type Deleter interface {
	Delete(ctx context.Context, channelID, targetID string) error
}

// Reactor places or withdraws a reaction on a message previously delivered by
// this sink.
type Reactor interface {
	React(ctx context.Context, r model.Reaction, channelID, targetID string) error
}

// EditLimiter is an Editor whose platform bounds how many times one message
// may be changed. Signal accepts ten edits and then ignores them, which a
// sender discovers only by reading their own message back — so the count is
// kept by the core, which has the store, on behalf of the sink, which knows
// the number.
type EditLimiter interface {
	MaxEdits() int
}

// Bridge routes messages between configured channel pairs.
type Bridge struct {
	// overwritten counts reactions displaced on a platform that holds only
	// one per sender. It is a function so the core stays free of the metrics
	// registry, which is what keeps it testable without one.
	overwritten func(pairID string)

	cfg   config.Config
	store store.Store
	sinks map[model.Platform]Sink
	log   *slog.Logger
	locks *pairLocks
}

// New builds a Bridge. sinks is keyed by the platform each sink delivers to.
func New(cfg config.Config, st store.Store, sinks map[model.Platform]Sink, log *slog.Logger) *Bridge {
	return &Bridge{
		cfg: cfg, store: st, sinks: sinks, log: log, locks: newPairLocks(),
		overwritten: func(string) {},
	}
}

// OnReactionOverwritten registers a counter for reactions displaced by a
// newer one on a platform that holds only one per sender.
func (b *Bridge) OnReactionOverwritten(f func(pairID string)) {
	if f != nil {
		b.overwritten = f
	}
}

// pairLocks hands out one mutex per pair, created on first use.
//
// Delivery within a pair is serialized because both webhooks dispatch every
// request on its own goroutine: a thread root and a reply posted a moment
// later arrive as two concurrent calls, and unserialized the reply resolves
// its thread parent before the root has recorded one — so the reply is
// delivered detached from its thread. Serializing also keeps messages in the
// order they were sent, which is what makes a mirrored conversation readable.
//
// The lock is per pair rather than global so one busy or slow channel cannot
// stall another customer's.
type pairLocks struct {
	mu sync.Mutex
	m  map[string]*sync.Mutex
}

func newPairLocks() *pairLocks {
	return &pairLocks{m: make(map[string]*sync.Mutex)}
}

func (p *pairLocks) get(pairID string) *sync.Mutex {
	p.mu.Lock()
	defer p.mu.Unlock()
	lock, ok := p.m[pairID]
	if !ok {
		lock = &sync.Mutex{}
		p.m[pairID] = lock
	}
	return lock
}

// Handle routes one inbound message. It is safe to call concurrently and safe
// to call more than once with the same message.
//
// delivered reports whether the message actually reached the target
// platform. When delivered is false and err is nil, reason names why the
// message was dropped — one of the Reason constants — so the caller can
// record bridge_dropped_total instead of folding the drop into
// bridge_delivered_total. reason is meaningless whenever err is non-nil or
// delivered is true.
func (b *Bridge) Handle(ctx context.Context, msg model.Message) (delivered bool, reason string, err error) {
	// Routing resolves the target from the pair, which cannot invent one, but
	// an unrecognised source platform still has no meaning to anything
	// downstream — the store keys on it and the metrics label with it.
	if !msg.Platform.Valid() {
		return false, "", fmt.Errorf("unknown source platform %q", msg.Platform)
	}

	// Loop prevention, layer one: never re-route what we authored.
	if msg.FromBridge {
		b.log.Debug("dropping bridge-authored message",
			"platform", msg.Platform, "id", msg.ID)
		return false, ReasonFromBridge, nil
	}

	// Everything above is a pure decision on the message itself. From here on
	// the pair's store rows and its delivery order are both at stake, so one
	// pair's messages are handled one at a time. This is held across the sink
	// call deliberately: releasing earlier would let a reply overtake the root
	// it belongs to, and both platforms rate-limit posting to a single channel
	// anyway.
	lock := b.locks.get(msg.PairID)
	lock.Lock()
	defer lock.Unlock()

	// Loop prevention, layer two: never re-route a message we ourselves
	// delivered onto this platform.
	produced, err := b.store.IsDeliveryResult(ctx, msg.Platform, msg.ID)
	if err != nil {
		return false, "", fmt.Errorf("check delivery origin: %w", err)
	}
	if produced {
		b.log.Debug("dropping echo of our own delivery",
			"platform", msg.Platform, "id", msg.ID)
		return false, ReasonEcho, nil
	}

	pair, ok := b.cfg.PairByID(msg.PairID)
	if !ok {
		return false, "", fmt.Errorf("unknown pair %q", msg.PairID)
	}

	target, channelID, ok := pair.Counterpart(msg.Platform)
	if !ok {
		return false, "", fmt.Errorf("pair %q does not carry platform %q", pair.ID, msg.Platform)
	}
	sink, ok := b.sinks[target]
	if !ok {
		return false, "", fmt.Errorf("no sink registered for platform %q", target)
	}

	// Idempotency: claim the event before doing any work.
	fresh, err := b.store.MarkProcessed(ctx, msg.Platform, msg.ID, msg.Kind)
	if err != nil {
		return false, "", fmt.Errorf("claim event: %w", err)
	}
	if !fresh {
		b.log.Debug("dropping already-processed event",
			"platform", msg.Platform, "id", msg.ID, "kind", msg.Kind)
		return false, ReasonDuplicate, nil
	}

	// From here the event is claimed. Every failure path before the message
	// actually reaches the target must release that claim, or a redelivery
	// hits the idempotency gate and the message is lost forever with nothing
	// louder than a debug line to show for it.
	//
	// Releasing runs on context.WithoutCancel(ctx) rather than ctx itself:
	// this cleanup has to complete even when the operation that needed it was
	// cancelled. The webhook path already dispatches on a detached context,
	// but the recovery sweep runs on the long-lived process context, and a
	// SIGTERM arriving mid-delivery must not turn a released claim into a
	// permanently stuck one.
	releaseCtx := context.WithoutCancel(ctx)
	release := func() {
		if releaseErr := b.store.UnmarkProcessed(releaseCtx, msg.Platform, msg.ID, msg.Kind); releaseErr != nil {
			b.log.Error("could not release the event claim; this message will not be retried",
				"pair", pair.ID, "id", msg.ID, "error", releaseErr)
		}
	}

	// An edit or a delete changes a message that already exists on the target
	// rather than adding one, so it takes its own path and either succeeds or
	// degrades into an annotation.
	if msg.Kind != model.EventCreated {
		delivered, reason, replayable, err := b.applyChange(ctx, pair, msg, sink, target, channelID)
		switch {
		case delivered && err == nil:
		case replayable:
			// Applying a change again is harmless: it names the message it
			// acts on, so a repeat lands on the same one.
			release()
		default:
			// The annotation path is a post like any other, and a post whose
			// outcome is unknown must not be replayed — the reader would see
			// the same correction twice.
			b.log.Error("a change failed with an unknown outcome; keeping the event claim so it cannot be annotated twice",
				"pair", pair.ID, "id", msg.ID, "kind", msg.Kind, "error", err)
		}
		return delivered, reason, err
	}

	threadTarget, err := b.resolveThreadTarget(ctx, msg)
	if err != nil {
		release()
		return false, "", err
	}

	targetID, err := sink.Deliver(ctx, msg, channelID, threadTarget)
	if err != nil {
		// The claim is released only when the sink can prove nothing was
		// posted. Otherwise it is kept: the post may have landed and only its
		// response been lost, and releasing would let the recovery sweep post
		// the message a second time.
		//
		// The cost of keeping it is a message that is never retried, which is
		// why this is logged at error level rather than left to a metric — an
		// operator has to be able to find it.
		if errors.Is(err, ErrNotSent) {
			release()
		} else {
			b.log.Error("delivery failed with an unknown outcome; keeping the event claim so the message cannot be posted twice",
				"pair", pair.ID, "id", msg.ID, "to", target, "error", err)
		}
		return false, "", fmt.Errorf("deliver to %s: %w", target, err)
	}

	// Past this point the message exists on the target. The claim is
	// deliberately NOT released if recording fails: releasing it would post
	// the message a second time on retry. The cost is a delivered message
	// with no stored mapping, so replies to it will not thread — visible and
	// repairable, unlike a duplicate in a customer channel.
	if err := b.store.RecordDelivery(ctx, store.Delivery{
		PairID:         pair.ID,
		SourcePlatform: msg.Platform,
		SourceID:       msg.ID,
		TargetPlatform: target,
		TargetID:       targetID,
		// Where this message ended up, so a reply to it can be threaded to
		// the same root rather than to the reply itself.
		TargetThreadRoot: threadTarget,
		// And where it came from, for a reply arriving the other way.
		SourceThreadRoot: msg.ThreadRootID,
	}); err != nil {
		return false, "", fmt.Errorf("record delivery: %w", err)
	}

	b.log.Info("delivered",
		"pair", pair.ID,
		"from", msg.Platform, "source_id", msg.ID,
		"to", target, "target_id", targetID)
	return true, "", nil
}

// resolveThreadTarget maps a source parent id onto the target platform. The
// parent may sit at either end of its mapping — a thread started on the far
// platform has its root recorded as a target — so the lookup matches both. An
// orphan reply, one whose parent was never mirrored, is delivered to the
// channel rather than dropped.
//
// When the parent is itself inside a thread, the thread's root is what the
// target platform is addressed with. Slack threads are named by their root
// and are one level deep; Signal has no threads at all and quotes whatever
// message a person tapped, which is regularly one from the middle of a thread
// this bridge created. Replying to the reply would start a second thread
// beside the conversation it belongs to.
func (b *Bridge) resolveThreadTarget(ctx context.Context, msg model.Message) (string, error) {
	if !msg.IsReply() {
		return "", nil
	}
	counterpart, err := b.store.LookupCounterpart(ctx, msg.PairID, msg.Platform, msg.ParentID)
	switch {
	case errors.Is(err, store.ErrNotFound):
		b.log.Warn("reply parent was never mirrored; delivering to channel root",
			"pair", msg.PairID, "parent_id", msg.ParentID)
		return "", nil
	case err != nil:
		return "", fmt.Errorf("resolve thread target: %w", err)
	}
	if counterpart.ThreadRoot != "" {
		return counterpart.ThreadRoot, nil
	}
	return counterpart.ID, nil
}

// applyChange mirrors an edit or a delete onto a message the bridge has
// already delivered.
//
// Both platforms bound how long a message may be changed — Signal at 24
// hours, Slack by workspace policy — and a sink that knows the change will
// not land says so with ErrOutOfWindow rather than reporting a success
// nobody's client will honour. The bridge then posts an annotation in the
// thread instead, so the reader learns that the text they are looking at is
// no longer what the author meant. Stale numbers living forever in a
// customer's channel is the outcome this exists to prevent.
// replayable reports whether the caller may release its idempotency claim:
// true when nothing was posted, so a repeat of the event cannot duplicate
// anything a reader would see.
func (b *Bridge) applyChange(
	ctx context.Context,
	pair config.Pair,
	msg model.Message,
	sink Sink,
	target model.Platform,
	channelID string,
) (delivered bool, reason string, replayable bool, err error) {
	original := msg.EditsID
	if original == "" {
		original = msg.ID
	}

	counterpart, err := b.store.LookupCounterpart(ctx, pair.ID, msg.Platform, original)
	switch {
	case errors.Is(err, store.ErrNotFound):
		// The message this changes was never mirrored — it failed delivery, or
		// it predates the bridge. There is nothing on the far side to correct
		// and nothing useful to say about it.
		b.log.Debug("dropping a change to a message that was never mirrored",
			"pair", pair.ID, "platform", msg.Platform, "id", original, "kind", msg.Kind)
		return false, ReasonUnmapped, true, nil
	case err != nil:
		return false, "", true, fmt.Errorf("resolve the changed message: %w", err)
	}

	err = b.applyChangeWithinLimits(ctx, pair, sink, msg, target, channelID, counterpart.ID)
	switch {
	case err == nil:
		b.log.Info("applied change",
			"pair", pair.ID, "kind", msg.Kind,
			"from", msg.Platform, "source_id", original,
			"to", target, "target_id", counterpart.ID)
		return true, "", false, nil
	case errors.Is(err, ErrOutOfWindow), errors.Is(err, ErrUnsupported):
		// Fall through to the annotation.
	default:
		// An edit or a delete names the message it acts on, so repeating one
		// lands on the same message rather than making a second.
		return false, "", true, fmt.Errorf("apply %s to %s: %w", msg.Kind, target, err)
	}

	threadTarget := counterpart.ThreadRoot
	if threadTarget == "" {
		threadTarget = counterpart.ID
	}
	// An annotation is a new message that happens to be about another one, so
	// it keeps the author and the pair and nothing else. Copying the whole
	// message carried the original's attachments — uploading the file a
	// second time — and its formatting ranges, which are offsets into a body
	// this message no longer has.
	annotation := model.Message{
		Platform:  msg.Platform,
		ID:        msg.ID,
		PairID:    msg.PairID,
		Author:    msg.Author,
		Kind:      model.EventCreated,
		Timestamp: msg.Timestamp,
		Text:      annotationText(msg),
	}
	if _, err := sink.Deliver(ctx, annotation, channelID, threadTarget); err != nil {
		// Only a sink that can prove nothing was posted makes this safe to
		// repeat. Anything else may have reached the channel with just its
		// answer lost, and a replay would show the reader the same correction
		// twice.
		return false, "", errors.Is(err, ErrNotSent),
			fmt.Errorf("annotate %s on %s: %w", msg.Kind, target, err)
	}
	b.log.Info("annotated change the target could not apply in place",
		"pair", pair.ID, "kind", msg.Kind, "to", target, "target_id", counterpart.ID)
	return true, "", false, nil
}

// applyChangeWithinLimits refuses an edit the target platform would accept and
// then ignore, so it degrades into an annotation the reader can see.
func (b *Bridge) applyChangeWithinLimits(
	ctx context.Context,
	pair config.Pair,
	sink Sink,
	msg model.Message,
	target model.Platform,
	channelID, targetID string,
) error {
	limiter, bounded := sink.(EditLimiter)
	if msg.Kind != model.EventEdited || !bounded {
		return b.applyToSink(ctx, sink, msg, channelID, targetID)
	}

	count, err := b.store.CountEdit(ctx, pair.ID, target, targetID)
	switch {
	case errors.Is(err, store.ErrNotFound):
		// No row to count against. The mapping was found a moment ago, so
		// this is a race rather than a state worth refusing an edit over:
		// letting it through risks an edit the platform ignores, refusing it
		// guarantees the reader never sees the correction at all.
		count = 0
	case err != nil:
		return fmt.Errorf("count edits of %s: %w", targetID, err)
	}
	if count > limiter.MaxEdits() {
		b.log.Warn("the target platform will not accept another edit of this message",
			"pair", pair.ID, "target_id", targetID, "edits", count)
		return ErrOutOfWindow
	}
	return b.applyToSink(ctx, sink, msg, channelID, targetID)
}

// applyToSink asks the sink to carry out the change, reporting ErrUnsupported
// when it has no such capability at all.
func (b *Bridge) applyToSink(
	ctx context.Context, sink Sink, msg model.Message, channelID, targetID string,
) error {
	switch msg.Kind {
	case model.EventEdited:
		editor, ok := sink.(Editor)
		if !ok {
			return ErrUnsupported
		}
		return editor.Update(ctx, msg, channelID, targetID)
	case model.EventDeleted:
		deleter, ok := sink.(Deleter)
		if !ok {
			return ErrUnsupported
		}
		return deleter.Delete(ctx, channelID, targetID)
	default:
		return fmt.Errorf("unhandled event kind %q", msg.Kind)
	}
}

// annotationText is what a reader sees when a change could not be applied in
// place. The edited text is included because the point of the annotation is
// that the message above it is wrong.
func annotationText(msg model.Message) string {
	if msg.Kind == model.EventDeleted {
		return "🗑 Message retracted"
	}
	return "✅ Corrected: " + msg.Text
}

// HandleReaction mirrors one reaction. It is safe to call concurrently and
// safe to call more than once with the same reaction.
//
// Reactions are not claimed in processed_event the way messages are. Placing
// the same reaction twice is a no-op on both platforms and removing one that
// is not there is not an error, so the natural idempotency is stronger than a
// claim would be — and a claim would have to be released on every withdrawal
// to let the same person react again later.
func (b *Bridge) HandleReaction(ctx context.Context, r model.Reaction) (delivered bool, reason string, err error) {
	if !r.Platform.Valid() {
		return false, "", fmt.Errorf("unknown source platform %q", r.Platform)
	}
	if r.FromBridge {
		return false, ReasonFromBridge, nil
	}
	if r.Emoji == "" {
		return false, ReasonUnsupportedEmoji, nil
	}

	pair, ok := b.cfg.PairByID(r.PairID)
	if !ok {
		return false, "", fmt.Errorf("unknown pair %q", r.PairID)
	}
	lock := b.locks.get(r.PairID)
	lock.Lock()
	defer lock.Unlock()

	// Loop prevention comes before routing: a reaction this bridge placed is
	// one to drop whatever the far side can do with it, and asking about
	// sinks first turned a plain echo into an error about a platform that was
	// never going to be involved.
	mine, err := b.store.IsReactionResult(ctx, pair.ID, r.Platform, r.TargetID, r.Emoji)
	if err != nil {
		return false, "", fmt.Errorf("check reaction origin: %w", err)
	}
	if mine {
		return false, ReasonEcho, nil
	}

	target, channelID, ok := pair.Counterpart(r.Platform)
	if !ok {
		return false, "", fmt.Errorf("pair %q does not carry platform %q", pair.ID, r.Platform)
	}
	sink, ok := b.sinks[target]
	if !ok {
		return false, "", fmt.Errorf("no sink registered for platform %q", target)
	}
	reactor, ok := sink.(Reactor)
	if !ok {
		return false, ReasonNotCreated, nil
	}

	counterpart, err := b.store.LookupCounterpart(ctx, pair.ID, r.Platform, r.TargetID)
	switch {
	case errors.Is(err, store.ErrNotFound):
		b.log.Debug("dropping a reaction to a message that was never mirrored",
			"pair", pair.ID, "platform", r.Platform, "target_id", r.TargetID)
		return false, ReasonUnmapped, nil
	case err != nil:
		return false, "", fmt.Errorf("resolve the reacted message: %w", err)
	}

	if r.Remove {
		return b.withdrawReaction(ctx, pair, r, reactor, target, channelID, counterpart.ID)
	}

	// Placing one where the target platform holds only one per sender
	// displaces whatever was there. The counter is what tells us afterwards
	// whether that was noise or a nuisance.
	if previous, err := b.store.LatestReaction(
		ctx, pair.ID, target, counterpart.ID, store.ReactionKey{}); err == nil &&
		previous.Emoji != r.Emoji {
		b.overwritten(pair.ID)
	}

	if err := reactor.React(ctx, r, channelID, counterpart.ID); err != nil {
		// A workspace's custom emoji has no unicode to send, and a unicode
		// emoji the shortcode table does not know has no Slack name. Neither
		// is worth an error an operator has to look at.
		if errors.Is(err, ErrUnsupported) {
			return false, ReasonUnsupportedEmoji, nil
		}
		return false, "", fmt.Errorf("react on %s: %w", target, err)
	}
	if err := b.store.RecordReaction(ctx, store.ReactionDelivery{
		PairID:          pair.ID,
		SourcePlatform:  r.Platform,
		SourceAuthor:    r.Author.ExternalID,
		SourceMessageID: r.TargetID,
		Emoji:           r.Emoji,
		TargetPlatform:  target,
		TargetMessageID: counterpart.ID,
	}); err != nil {
		return false, "", fmt.Errorf("record reaction: %w", err)
	}
	return true, "", nil
}

// withdrawReaction removes one person's reaction, re-asserting the newest
// survivor when others remain.
//
// Signal holds one reaction per sender per message, and every reaction the
// bridge mirrors is sent by the same account. Removing ours because one
// person in Slack changed their mind would take everyone else's away from the
// customer's view at the same time.
func (b *Bridge) withdrawReaction(
	ctx context.Context,
	pair config.Pair,
	r model.Reaction,
	reactor Reactor,
	target model.Platform,
	channelID, targetMessageID string,
) (bool, string, error) {
	// What the target should show once this person's reaction is gone. The
	// record is read before it is removed and the withdrawal is discounted
	// here, so that a failure below leaves the store describing what the
	// platforms actually show — remove first and a failed call would strand a
	// reaction nobody has a record of, which loop prevention then treats as a
	// stranger's.
	survivor, err := b.survivingReaction(ctx, pair, target, targetMessageID, r)
	if err != nil {
		return false, "", err
	}

	act := r
	if survivor != "" {
		if survivor == r.Emoji {
			// Somebody else is still holding the same reaction, so the target
			// already shows what it should.
			return true, "", b.forgetReaction(ctx, pair, r)
		}
		act.Remove = false
		act.Emoji = survivor
	}
	if err := reactor.React(ctx, act, channelID, targetMessageID); err != nil {
		return false, "", fmt.Errorf("withdraw reaction on %s: %w", target, err)
	}
	return true, "", b.forgetReaction(ctx, pair, r)
}

// survivingReaction is the emoji the target should show once r is withdrawn,
// or empty when nothing is left.
func (b *Bridge) survivingReaction(
	ctx context.Context,
	pair config.Pair,
	target model.Platform,
	targetMessageID string,
	withdrawn model.Reaction,
) (string, error) {
	latest, err := b.store.LatestReaction(ctx, pair.ID, target, targetMessageID, store.ReactionKey{
		SourceAuthor:    withdrawn.Author.ExternalID,
		SourceMessageID: withdrawn.TargetID,
		Emoji:           withdrawn.Emoji,
	})
	switch {
	case errors.Is(err, store.ErrNotFound):
		return "", nil
	case err != nil:
		return "", fmt.Errorf("find the surviving reaction: %w", err)
	}
	return latest.Emoji, nil
}

func (b *Bridge) forgetReaction(ctx context.Context, pair config.Pair, r model.Reaction) error {
	if err := b.store.ForgetReaction(
		ctx, pair.ID, r.Platform, r.Author.ExternalID, r.TargetID, r.Emoji); err != nil {
		return fmt.Errorf("forget reaction: %w", err)
	}
	return nil
}

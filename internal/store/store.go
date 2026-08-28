// Package store persists the identifiers the bridge needs to correlate
// messages across platforms. It never stores message bodies.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/aenix-io/slack-connectors/internal/model"
)

// ErrNotFound signals that no mapping exists for the requested key.
var ErrNotFound = errors.New("store: not found")

// DeltaCursor is the persisted resume point for a pair's Teams channel delta
// chain: the link to resume from, and when it was last written. The
// timestamp lets recovery reseed the chain from the last moment it is known
// to have been caught up, rather than from "now", when Graph invalidates the
// link.
type DeltaCursor struct {
	Link      string
	UpdatedAt time.Time
}

// Subscription is the persisted identity of a pair's current Graph change
// notification subscription: just enough to delete it on the next restart
// and to alert on its expiry. It never carries message content.
type Subscription struct {
	ID        string
	ExpiresAt time.Time
}

// Delivery records that a source message was mirrored to a target message.
type Delivery struct {
	PairID         string
	SourcePlatform model.Platform
	SourceID       string
	TargetPlatform model.Platform
	TargetID       string
	// TargetThreadRoot is the thread this delivery was posted into on the
	// target platform, empty when it started one. Slack addresses a thread by
	// its root rather than by any message in it, so a reply to a reply has to
	// resolve back to the root — and Signal, which has no threads at all,
	// happily quotes a message from the middle of one.
	TargetThreadRoot string
	// SourceThreadRoot is the thread this message sat in on its own
	// platform, empty when it started one or when that platform has no
	// threads. It is what lets a reply coming back the other way resolve to
	// the thread's root rather than to one of its replies.
	SourceThreadRoot string
}

// ReactionKey identifies one person's reaction on one message, on the
// platform it was placed.
type ReactionKey struct {
	SourceAuthor    string
	SourceMessageID string
	Emoji           string
}

// Counterpart is a message's identity on the other platform: the mirrored
// message, and the thread it sits in when the platform has threads.
type Counterpart struct {
	ID         string
	ThreadRoot string
}

// ReactionDelivery records that a reaction on the source platform was
// mirrored onto a message on the target platform.
//
// It is keyed by who reacted rather than by what the mirror produced, because
// removal has to know what else is still standing: Signal allows one reaction
// per sender per message, so the bridge's single identity carries at most one
// at a time, and one person un-reacting must not wipe everyone else's.
type ReactionDelivery struct {
	PairID          string
	SourcePlatform  model.Platform
	SourceAuthor    string
	SourceMessageID string
	Emoji           string
	TargetPlatform  model.Platform
	TargetMessageID string
}

// Store is the persistence contract used by the bridge core.
type Store interface {
	// MarkProcessed claims an event for processing. It returns true when the
	// caller is the first to claim it and false when it was already handled.
	MarkProcessed(ctx context.Context, src model.Platform, srcID string, kind model.EventKind) (bool, error)

	// UnmarkProcessed releases a claim made by MarkProcessed so a failed
	// delivery can be retried. Releasing a claim that was never made is not
	// an error. Without this, a transient sink failure would leave the event
	// claimed forever and the message would never be delivered.
	UnmarkProcessed(ctx context.Context, src model.Platform, srcID string, kind model.EventKind) error

	// RecordDelivery stores the mapping produced by a successful delivery.
	RecordDelivery(ctx context.Context, d Delivery) error

	// LookupTarget returns the target message id a source message was
	// mirrored to, or ErrNotFound. It resolves forward only: it answers
	// "what did this source become", which is what the RecordDelivery
	// conflict check needs.
	LookupTarget(ctx context.Context, pairID string, src model.Platform, srcID string) (string, error)

	// LookupCounterpart returns the id a message has on the other platform,
	// whichever end of the mapping it sits at, or ErrNotFound.
	//
	// Thread correlation needs this rather than LookupTarget: with both
	// directions live, the root of a conversation started on the far platform
	// is the target of its mapping, not the source, and resolving it forward
	// only would deliver every reply to it as a new top-level message.
	LookupCounterpart(ctx context.Context, pairID string, p model.Platform, id string) (Counterpart, error)

	// IsDeliveryResult reports whether a message on a platform was itself
	// produced by this bridge. Loop prevention layer two.
	IsDeliveryResult(ctx context.Context, p model.Platform, id string) (bool, error)

	// CountEdit records that a mirrored message was edited once more and
	// returns how many edits it has now had.
	//
	// Platforms bound this: Signal accepts ten edits per message and ignores
	// the rest, which a sender only discovers by reading their own message
	// back. The count is kept here because the sink that knows the limit
	// keeps no state of its own.
	CountEdit(ctx context.Context, pairID string, target model.Platform, targetID string) (int, error)

	// RecordReaction stores a mirrored reaction. Recording one that is
	// already stored is not an error.
	RecordReaction(ctx context.Context, r ReactionDelivery) error

	// ForgetReaction removes the record of a mirrored reaction. Removing one
	// that was never recorded is not an error.
	ForgetReaction(ctx context.Context, pairID string, src model.Platform, author, sourceMessageID, emoji string) error

	// LatestReaction returns the most recently recorded reaction still
	// standing on a mirrored message, ignoring except, or ErrNotFound when
	// there is none.
	//
	// This is what makes removal correct on a platform that holds one
	// reaction per sender: when somebody withdraws theirs and others remain,
	// the bridge re-asserts the newest survivor instead of clearing the lot.
	// The exclusion exists so the answer can be read *before* the withdrawn
	// row is deleted — the record has to keep describing what the platforms
	// show until the platform has actually been told otherwise.
	LatestReaction(
		ctx context.Context,
		pairID string,
		target model.Platform,
		targetMessageID string,
		except ReactionKey,
	) (ReactionDelivery, error)

	// IsReactionResult reports whether a reaction on a platform was itself
	// placed by this bridge. Loop prevention for the reaction path.
	IsReactionResult(ctx context.Context, pairID string, target model.Platform, targetMessageID, emoji string) (bool, error)

	// SaveSubscription persists a pair's current Graph subscription so it can
	// be found and torn down on the next restart, instead of leaking and
	// double-delivering for up to an hour.
	SaveSubscription(ctx context.Context, pairID string, sub Subscription) error

	// LoadSubscription returns the stored subscription for a pair, or
	// ErrNotFound when none is stored.
	LoadSubscription(ctx context.Context, pairID string) (Subscription, error)

	// DeleteSubscription removes the stored subscription record for a pair.
	// Deleting an absent row is not an error.
	DeleteSubscription(ctx context.Context, pairID string) error

	Close() error
}

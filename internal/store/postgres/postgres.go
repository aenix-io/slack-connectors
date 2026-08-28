// Package postgres implements store.Store on PostgreSQL.
package postgres

import (
	"context"
	_ "embed"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aenix-io/slack-connectors/internal/model"
	"github.com/aenix-io/slack-connectors/internal/store"
)

//go:embed schema.sql
var schema string

// Store is the PostgreSQL-backed implementation of store.Store.
type Store struct {
	pool *pgxpool.Pool
}

// schemaLockKey namespaces the advisory lock guarding schema application.
const schemaLockKey int64 = 0x7B5D6C3A

// New connects to dsn and applies the schema.
//
// Schema application is wrapped in a transaction-scoped advisory lock:
// PostgreSQL does not make concurrent CREATE TABLE IF NOT EXISTS race-free, so
// two processes starting against a cold database can otherwise collide and one
// will fail to start for no reason an operator can act on.
func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := applySchema(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

func applySchema(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin schema transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, schemaLockKey); err != nil {
		return fmt.Errorf("acquire schema lock: %w", err)
	}
	if _, err := tx.Exec(ctx, schema); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit schema: %w", err)
	}
	return nil
}

// MarkProcessed claims an event, returning false when it was already claimed.
func (s *Store) MarkProcessed(ctx context.Context, src model.Platform, srcID string, kind model.EventKind) (bool, error) {
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO processed_event (source, source_id, event_kind)
		 VALUES ($1, $2, $3)
		 ON CONFLICT DO NOTHING`,
		string(src), srcID, string(kind))
	if err != nil {
		return false, fmt.Errorf("mark processed: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// UnmarkProcessed releases an event claim so a failed delivery can be retried.
func (s *Store) UnmarkProcessed(ctx context.Context, src model.Platform, srcID string, kind model.EventKind) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM processed_event
		 WHERE source = $1 AND source_id = $2 AND event_kind = $3`,
		string(src), srcID, string(kind))
	if err != nil {
		return fmt.Errorf("unmark processed: %w", err)
	}
	return nil
}

// RecordDelivery stores a source-to-target mapping.
//
// Recording the same mapping twice is a no-op. Recording a *different* target
// for a source that is already mapped is an error: it means the message was
// mirrored twice and one of the two copies is now orphaned on the far side,
// with the store quietly pointing at only one of them. Later tasks resolve
// edits and thread parents through this mapping, so a stale entry sends
// corrections to the wrong message.
func (s *Store) RecordDelivery(ctx context.Context, d store.Delivery) error {
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO delivery
		   (pair_id, source_platform, source_id, target_platform, target_id,
		    target_thread_root, source_thread_root)
		 VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''), NULLIF($7, ''))
		 ON CONFLICT (pair_id, source_platform, source_id) DO NOTHING`,
		d.PairID, string(d.SourcePlatform), d.SourceID, string(d.TargetPlatform), d.TargetID,
		d.TargetThreadRoot, d.SourceThreadRoot)
	if err != nil {
		return fmt.Errorf("record delivery: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}

	existing, err := s.LookupTarget(ctx, d.PairID, d.SourcePlatform, d.SourceID)
	if err != nil {
		return fmt.Errorf("record delivery: verify existing mapping: %w", err)
	}
	if existing != d.TargetID {
		return fmt.Errorf(
			"record delivery: %s message %q in pair %q already maps to %s target %q; refusing to remap to %q",
			d.SourcePlatform, d.SourceID, d.PairID, d.TargetPlatform, existing, d.TargetID)
	}
	return nil
}

// LookupTarget resolves the mirrored message id for a source message.
func (s *Store) LookupTarget(ctx context.Context, pairID string, src model.Platform, srcID string) (string, error) {
	var targetID string
	err := s.pool.QueryRow(ctx,
		`SELECT target_id FROM delivery
		 WHERE pair_id = $1 AND source_platform = $2 AND source_id = $3`,
		pairID, string(src), srcID).Scan(&targetID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", store.ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("lookup target: %w", err)
	}
	return targetID, nil
}

// LookupCounterpart resolves a message id to its id on the other platform,
// matching either end of the mapping. The backward half is served by
// delivery_target_idx, so both halves are index lookups.
//
// Both halves matching at once would make LIMIT 1 pick arbitrarily, so it is
// worth being precise about why that does not happen. An id recorded as a
// target is a message this bridge posted. For it to also become a source, the
// platform's echo of it would have to survive loop prevention:
//
//   - Layer one drops it on the bridge-authored flag, which for Slack is
//     bot_id and for Teams is from.application. This is a single field on each
//     side and carries most of the weight.
//   - Layer two reads this table, which is only written after the post
//     returns. On its own that leaves a window — the echo can arrive before
//     the mapping exists — but delivery is serialized per pair and the lock is
//     held across the post and the write, so an echo of a message this pair is
//     still delivering waits until the mapping is there.
//
// The window is therefore closed while a delivery is in progress, and outside
// one the mapping already exists. What remains is a delivery whose
// RecordDelivery failed: no mapping was written, so layer one is the only
// guard left.
// The thread the counterpart sits in comes back with it, taken from whichever
// side of the row the counterpart is on. Both halves matter: a Signal quote
// of a message the bridge posted into a Slack thread has to resolve back to
// that thread's root, because Slack addresses a thread by its root and warns
// against naming one of its replies.
func (s *Store) LookupCounterpart(
	ctx context.Context, pairID string, p model.Platform, id string,
) (store.Counterpart, error) {
	var counterpart store.Counterpart
	err := s.pool.QueryRow(ctx,
		`SELECT target_id, COALESCE(target_thread_root, '') FROM delivery
		  WHERE pair_id = $1 AND source_platform = $2 AND source_id = $3
		 UNION ALL
		 SELECT source_id, COALESCE(source_thread_root, '') FROM delivery
		  WHERE pair_id = $1 AND target_platform = $2 AND target_id = $3
		 LIMIT 1`,
		pairID, string(p), id).Scan(&counterpart.ID, &counterpart.ThreadRoot)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.Counterpart{}, store.ErrNotFound
	}
	if err != nil {
		return store.Counterpart{}, fmt.Errorf("lookup counterpart: %w", err)
	}
	return counterpart, nil
}

// IsDeliveryResult reports whether this bridge produced the given message.
func (s *Store) IsDeliveryResult(ctx context.Context, p model.Platform, id string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS (
			SELECT 1 FROM delivery WHERE target_platform = $1 AND target_id = $2
		 )`,
		string(p), id).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("is delivery result: %w", err)
	}
	return exists, nil
}

// SaveDeltaLink stores the resume point for a pair's channel history.
func (s *Store) SaveDeltaLink(ctx context.Context, pairID, link string) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO delta_cursor (pair_id, delta_link, updated_at)
		 VALUES ($1, $2, now())
		 ON CONFLICT (pair_id) DO UPDATE
		   SET delta_link = EXCLUDED.delta_link, updated_at = now()`,
		pairID, link)
	if err != nil {
		return fmt.Errorf("save delta link: %w", err)
	}
	return nil
}

// LoadDeltaCursor returns the stored resume point and when it was last
// written, or ErrNotFound on first run.
func (s *Store) LoadDeltaCursor(ctx context.Context, pairID string) (store.DeltaCursor, error) {
	var cursor store.DeltaCursor
	err := s.pool.QueryRow(ctx,
		`SELECT delta_link, updated_at FROM delta_cursor WHERE pair_id = $1`, pairID,
	).Scan(&cursor.Link, &cursor.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.DeltaCursor{}, store.ErrNotFound
	}
	if err != nil {
		return store.DeltaCursor{}, fmt.Errorf("load delta cursor: %w", err)
	}
	return cursor, nil
}

// SaveSubscription stores a pair's current Graph subscription, replacing
// whatever was stored before.
func (s *Store) SaveSubscription(ctx context.Context, pairID string, sub store.Subscription) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO subscription (pair_id, subscription_id, expires_at, updated_at)
		 VALUES ($1, $2, $3, now())
		 ON CONFLICT (pair_id) DO UPDATE
		   SET subscription_id = EXCLUDED.subscription_id,
		       expires_at = EXCLUDED.expires_at,
		       updated_at = now()`,
		pairID, sub.ID, sub.ExpiresAt)
	if err != nil {
		return fmt.Errorf("save subscription: %w", err)
	}
	return nil
}

// LoadSubscription returns the stored subscription for a pair, or
// ErrNotFound when none is stored.
func (s *Store) LoadSubscription(ctx context.Context, pairID string) (store.Subscription, error) {
	var sub store.Subscription
	err := s.pool.QueryRow(ctx,
		`SELECT subscription_id, expires_at FROM subscription WHERE pair_id = $1`, pairID,
	).Scan(&sub.ID, &sub.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.Subscription{}, store.ErrNotFound
	}
	if err != nil {
		return store.Subscription{}, fmt.Errorf("load subscription: %w", err)
	}
	return sub, nil
}

// DeleteSubscription removes the stored subscription record for a pair.
// Deleting an absent row is not an error.
func (s *Store) DeleteSubscription(ctx context.Context, pairID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM subscription WHERE pair_id = $1`, pairID)
	if err != nil {
		return fmt.Errorf("delete subscription: %w", err)
	}
	return nil
}

// Close releases the connection pool.
// Ping reports whether the database is reachable. Readiness uses it: a pod
// whose pool is dead can serve nothing, and answering Ready anyway means every
// message that arrives while it is broken is failed rather than retried
// somewhere healthy.
func (s *Store) Ping(ctx context.Context) error {
	if err := s.pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping database: %w", err)
	}
	return nil
}

func (s *Store) Close() error {
	s.pool.Close()
	return nil
}

// compile-time interface check
var _ store.Store = (*Store)(nil)

// CountEdit increments and returns a mirrored message's edit count.
func (s *Store) CountEdit(
	ctx context.Context, pairID string, target model.Platform, targetID string,
) (int, error) {
	var count int
	err := s.pool.QueryRow(ctx,
		`UPDATE delivery SET edit_count = edit_count + 1
		  WHERE pair_id = $1 AND target_platform = $2 AND target_id = $3
		 RETURNING edit_count`,
		pairID, string(target), targetID).Scan(&count)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, store.ErrNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("count edit: %w", err)
	}
	return count, nil
}

// RecordReaction stores a mirrored reaction.
func (s *Store) RecordReaction(ctx context.Context, r store.ReactionDelivery) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO reaction_delivery
		   (pair_id, source_platform, source_author, source_message_id, emoji,
		    target_platform, target_message_id)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 ON CONFLICT (pair_id, source_platform, source_author, source_message_id, emoji)
		 DO NOTHING`,
		r.PairID, string(r.SourcePlatform), r.SourceAuthor, r.SourceMessageID, r.Emoji,
		string(r.TargetPlatform), r.TargetMessageID)
	if err != nil {
		return fmt.Errorf("record reaction: %w", err)
	}
	return nil
}

// ForgetReaction removes the record of a mirrored reaction.
func (s *Store) ForgetReaction(
	ctx context.Context, pairID string, src model.Platform, author, sourceMessageID, emoji string,
) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM reaction_delivery
		  WHERE pair_id = $1 AND source_platform = $2 AND source_author = $3
		    AND source_message_id = $4 AND emoji = $5`,
		pairID, string(src), author, sourceMessageID, emoji)
	if err != nil {
		return fmt.Errorf("forget reaction: %w", err)
	}
	return nil
}

// LatestReaction returns the newest reaction still standing on a mirrored
// message.
func (s *Store) LatestReaction(
	ctx context.Context,
	pairID string,
	target model.Platform,
	targetMessageID string,
	except store.ReactionKey,
) (store.ReactionDelivery, error) {
	var r store.ReactionDelivery
	var sourcePlatform, targetPlatform string
	err := s.pool.QueryRow(ctx,
		`SELECT pair_id, source_platform, source_author, source_message_id, emoji,
		        target_platform, target_message_id
		   FROM reaction_delivery
		  WHERE pair_id = $1 AND target_platform = $2 AND target_message_id = $3
		    AND NOT (source_author = $4 AND source_message_id = $5 AND emoji = $6)
		  ORDER BY created_at DESC
		  LIMIT 1`,
		pairID, string(target), targetMessageID,
		except.SourceAuthor, except.SourceMessageID, except.Emoji).
		Scan(&r.PairID, &sourcePlatform, &r.SourceAuthor, &r.SourceMessageID, &r.Emoji,
			&targetPlatform, &r.TargetMessageID)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.ReactionDelivery{}, store.ErrNotFound
	}
	if err != nil {
		return store.ReactionDelivery{}, fmt.Errorf("latest reaction: %w", err)
	}
	r.SourcePlatform = model.Platform(sourcePlatform)
	r.TargetPlatform = model.Platform(targetPlatform)
	return r, nil
}

// IsReactionResult reports whether this bridge placed a reaction.
func (s *Store) IsReactionResult(
	ctx context.Context, pairID string, target model.Platform, targetMessageID, emoji string,
) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS (
		   SELECT 1 FROM reaction_delivery
		    WHERE pair_id = $1 AND target_platform = $2
		      AND target_message_id = $3 AND emoji = $4)`,
		pairID, string(target), targetMessageID, emoji).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check reaction origin: %w", err)
	}
	return exists, nil
}

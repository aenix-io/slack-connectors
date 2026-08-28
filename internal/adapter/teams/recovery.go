package teams

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/aenix-io/slack-connectors/internal/config"
	"github.com/aenix-io/slack-connectors/internal/model"
	"github.com/aenix-io/slack-connectors/internal/store"
)

// maxRecoveryPages bounds a single recovery pass so a very long outage cannot
// stall startup indefinitely. Anything beyond this is reported, not hidden.
const maxRecoveryPages = 100

// DefaultSweepInterval is used when Recovery.SweepInterval is left zero.
const DefaultSweepInterval = 10 * time.Minute

// watermarkSlack widens the replay window past the stored watermark. The
// watermark is the row's updated_at — stamped when a sweep SAVES, which is
// later than when its pages were FETCHED — so a message that arrived in
// between would otherwise fall between two windows. Overlap is cheap: the
// idempotency gate drops what was already delivered.
const watermarkSlack = 5 * time.Minute

// recoveryWatermark is stored in place of the old delta link. The link value
// is no longer consulted — the watermark is the row's updated_at, which the
// store refreshes on every save.
const recoveryWatermark = "rsc-watermark"

// CursorStore persists the per-pair recovery watermark. The stored link is
// vestigial under RSC; the resume point is the row's updated_at.
type CursorStore interface {
	SaveDeltaLink(ctx context.Context, pairID, link string) error
	LoadDeltaCursor(ctx context.Context, pairID string) (store.DeltaCursor, error)
}

// Recovery replays channel history missed while the service was down, and
// keeps replaying on a schedule while it runs: without a recurring sweep,
// only a restart ever advances the cursor, so a long uptime replays the
// entire uptime on its next restart instead of only the downtime.
type Recovery struct {
	client   *GraphClient
	cfg      config.Config
	cursors  CursorStore
	dispatch func(context.Context, model.Message) error
	log      *slog.Logger

	// SweepInterval controls how often Run repeats SweepOnce after the first
	// pass. Exported so tests can shorten it directly, the way now is
	// overridden below.
	SweepInterval time.Duration

	// now is the clock used to seed a first-run delta chain. Injectable so
	// tests can assert the exact filter sent to Graph.
	now func() time.Time

	// mu guards sweeping.
	mu sync.Mutex
	// sweeping is non-nil while a sweep runs and is closed when it ends.
	//
	// A WaitGroup is the obvious choice here and the wrong one: Run adds to it
	// on every tick while Drain waits on it, and adding to a WaitGroup whose
	// counter is zero concurrently with Wait is a data race — one the race
	// detector catches intermittently, because after cancellation Run's select
	// can still choose the ticker.
	sweeping chan struct{}
}

// Drain waits for a running sweep to finish, or until ctx expires.
//
// A sweep dispatches through the full delivery path, so at shutdown it can be
// mid-delivery while holding the database pool. Without this the pool was
// closed underneath it: the claim release then failed against a closed pool and
// the message stayed claimed forever, never delivered and never retried.
func (r *Recovery) Drain(ctx context.Context) error {
	r.mu.Lock()
	sweeping := r.sweeping
	r.mu.Unlock()

	if sweeping == nil {
		return nil
	}
	select {
	case <-sweeping:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// beginSweep marks a sweep as running and returns the function that ends it.
func (r *Recovery) beginSweep() func() {
	r.mu.Lock()
	r.sweeping = make(chan struct{})
	current := r.sweeping
	r.mu.Unlock()

	return func() {
		r.mu.Lock()
		close(current)
		if r.sweeping == current {
			r.sweeping = nil
		}
		r.mu.Unlock()
	}
}

// NewRecovery builds the startup recovery pass.
func NewRecovery(
	client *GraphClient,
	cfg config.Config,
	cursors CursorStore,
	dispatch func(context.Context, model.Message) error,
	log *slog.Logger,
) *Recovery {
	return &Recovery{
		client:        client,
		cfg:           cfg,
		cursors:       cursors,
		dispatch:      dispatch,
		log:           log,
		now:           time.Now,
		SweepInterval: DefaultSweepInterval,
	}
}

// SweepOnce performs a single pass over every configured pair. A failure on
// one pair is logged and the remaining pairs still run: a single
// misconfigured channel must not prevent the rest of the bridge from
// catching up.
func (r *Recovery) SweepOnce(ctx context.Context) error {
	for _, pair := range r.cfg.PairsFor(model.PlatformTeams) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := r.recoverPair(ctx, pair); err != nil {
			r.log.Error("gap recovery failed", "pair", pair.ID, "error", err)
		}
	}
	return nil
}

// Run repeats SweepOnce every SweepInterval until ctx is cancelled.
// Callers must invoke SweepOnce themselves before calling Run to ensure the
// startup replay completes before Graph subscriptions start delivering: the
// first sweep must finish before subscriptions begin, so messages delivered
// before the bridge was ready are not lost on the next restart.
// Each sweep advances the cursor; messages already delivered are dropped by
// the bridge's idempotency gate, so a sweep costs one store lookup per
// already-seen message and nothing else.
func (r *Recovery) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.SweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			end := r.beginSweep()
			err := r.SweepOnce(ctx)
			end()
			if err != nil {
				return err
			}
		}
	}
}

// listURL builds the channel message listing for a pair. Under RSC the delta
// endpoint is unavailable, so recovery pages through the plain list with
// replies expanded and cuts off client-side. Graph orders the listing by the
// last modified date of the entire reply chain — an edit or a late reply to
// an ancient thread surfaces that whole chain at the top, which is exactly
// what makes a time cutoff correct.
func listURL(pair config.Pair) string {
	return fmt.Sprintf("/teams/%s/channels/%s/messages?$top=50&$expand=replies",
		pair.Teams.TeamID, pair.Teams.ChannelID)
}

func (r *Recovery) recoverPair(ctx context.Context, pair config.Pair) error {
	cursor, err := r.cursors.LoadDeltaCursor(ctx, pair.ID)
	since := r.now()
	switch {
	case errors.Is(err, store.ErrNotFound):
		// First run: mirror from now on. Replaying the channel's whole
		// history would dump months of messages into Slack on first boot.
		r.log.Info("no recovery watermark yet; starting from the current time",
			"pair", pair.ID)
	case err != nil:
		return fmt.Errorf("load recovery watermark: %w", err)
	default:
		since = cursor.UpdatedAt.Add(-watermarkSlack)
	}
	return r.replayWindow(ctx, pair, since)
}

// replayWindow pages through the channel in chain-activity order and
// dispatches every message and reply changed after since.
//
// Replies arrive via $expand and Graph caps them per message; a thread that
// gathered more replies than one expanded page during an outage is the
// remaining — and narrow — recovery gap: replies@odata.nextLink is
// deliberately not followed yet.
//
// The watermark is advanced only when every message in the window was
// handled. This sweep is the only retry mechanism the bridge has — both
// webhooks answer before processing, so neither platform ever redelivers.
// Advancing over a failed delivery would lose the message for good, so a
// failure leaves the watermark where it was and the next sweep re-covers the
// range. Re-covering is cheap: messages already delivered are refused by the
// idempotency gate.
func (r *Recovery) replayWindow(ctx context.Context, pair config.Pair, since time.Time) error {
	url := listURL(pair)
	replayed, failed := 0, 0

	handle := func(cm chatMessage) {
		if !lastChange(cm).After(since) {
			return
		}
		if cm.MessageType != "" && cm.MessageType != "message" {
			return
		}
		if err := r.dispatch(ctx, toCanonical(cm, pair.ID, "created")); err != nil {
			// Deliberately not returned: the rest of the window is still
			// worth replaying, and the caller already logged this failure
			// in detail. What matters here is that the watermark stays put.
			failed++
			return
		}
		replayed++
	}

	for page := 0; page < maxRecoveryPages; page++ {
		p, err := r.client.FetchMessagePage(ctx, url)
		if err != nil {
			return fmt.Errorf("fetch message page %d: %w", page, err)
		}

		reachedCutoff := false
		for _, root := range p.Messages {
			if chainLastChange(root).Before(since) {
				// The listing is ordered by chain activity, so everything
				// after this root is older still.
				reachedCutoff = true
				break
			}
			handle(root)
			for _, reply := range root.Replies {
				handle(reply)
			}
		}

		if !reachedCutoff && p.NextLink != "" {
			url = p.NextLink
			continue
		}
		if failed > 0 {
			return fmt.Errorf(
				"%d of %d replayed messages for pair %s failed to deliver; watermark not advanced",
				failed, failed+replayed, pair.ID)
		}
		if err := r.cursors.SaveDeltaLink(ctx, pair.ID, recoveryWatermark); err != nil {
			return fmt.Errorf("save recovery watermark: %w", err)
		}
		r.log.Info("gap recovery complete", "pair", pair.ID, "replayed", replayed)
		return nil
	}
	return fmt.Errorf("gap recovery exceeded %d pages for pair %s; watermark not advanced",
		maxRecoveryPages, pair.ID)
}

// chainLastChange is the moment anything in the thread last changed — the
// root or any reply on the expanded page. It mirrors the key Graph sorts the
// listing by.
func chainLastChange(root chatMessage) time.Time {
	latest := lastChange(root)
	for _, reply := range root.Replies {
		if t := lastChange(reply); t.After(latest) {
			latest = t
		}
	}
	return latest
}

// lastChange picks the moment a message last changed: Graph fills
// lastModifiedDateTime on edits; for untouched messages it mirrors the
// creation time.
func lastChange(cm chatMessage) time.Time {
	if t := parseGraphTime(cm.LastModifiedDateTime); !t.IsZero() {
		return t
	}
	return parseGraphTime(cm.CreatedDateTime)
}

// parseGraphTime is forgiving: a missing or malformed stamp becomes zero and
// the message is treated as old rather than crashing the sweep.
func parseGraphTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

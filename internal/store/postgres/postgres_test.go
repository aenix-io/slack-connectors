package postgres

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/aenix-io/slack-connectors/internal/model"
	"github.com/aenix-io/slack-connectors/internal/store"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("bridge"),
		tcpostgres.WithUsername("bridge"),
		tcpostgres.WithPassword("bridge"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	s, err := New(ctx, dsn)
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestMarkProcessedIsIdempotent(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	first, err := s.MarkProcessed(ctx, model.PlatformTeams, "msg-1", model.EventCreated)
	if err != nil {
		t.Fatalf("MarkProcessed(): %v", err)
	}
	if !first {
		t.Fatal("first MarkProcessed() = false, want true")
	}

	second, err := s.MarkProcessed(ctx, model.PlatformTeams, "msg-1", model.EventCreated)
	if err != nil {
		t.Fatalf("MarkProcessed(): %v", err)
	}
	if second {
		t.Fatal("second MarkProcessed() = true, want false")
	}
}

func TestMarkProcessedSeparatesEventKinds(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	if _, err := s.MarkProcessed(ctx, model.PlatformTeams, "msg-1", model.EventCreated); err != nil {
		t.Fatalf("MarkProcessed(): %v", err)
	}
	edited, err := s.MarkProcessed(ctx, model.PlatformTeams, "msg-1", model.EventEdited)
	if err != nil {
		t.Fatalf("MarkProcessed(): %v", err)
	}
	if !edited {
		t.Fatal("edit of an already-created message = false, want true")
	}
}

func TestUnmarkProcessedAllowsReclaim(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	if _, err := s.MarkProcessed(ctx, model.PlatformTeams, "msg-1", model.EventCreated); err != nil {
		t.Fatalf("MarkProcessed(): %v", err)
	}
	if err := s.UnmarkProcessed(ctx, model.PlatformTeams, "msg-1", model.EventCreated); err != nil {
		t.Fatalf("UnmarkProcessed(): %v", err)
	}
	again, err := s.MarkProcessed(ctx, model.PlatformTeams, "msg-1", model.EventCreated)
	if err != nil {
		t.Fatalf("MarkProcessed(): %v", err)
	}
	if !again {
		t.Fatal("MarkProcessed() after UnmarkProcessed = false, want true — a released claim must be reclaimable")
	}

	// Releasing a claim that was never made must not be an error: the
	// release path runs on the failure branch, where the caller has no way
	// to know whether the claim survived.
	if err := s.UnmarkProcessed(ctx, model.PlatformTeams, "never-claimed", model.EventCreated); err != nil {
		t.Fatalf("UnmarkProcessed(absent): %v", err)
	}
}

func TestRecordAndLookupDelivery(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	d := store.Delivery{
		PairID:         "acme",
		SourcePlatform: model.PlatformTeams,
		SourceID:       "teams-1",
		TargetPlatform: model.PlatformSlack,
		TargetID:       "1700000000.000100",
	}
	if err := s.RecordDelivery(ctx, d); err != nil {
		t.Fatalf("RecordDelivery(): %v", err)
	}

	got, err := s.LookupTarget(ctx, "acme", model.PlatformTeams, "teams-1")
	if err != nil {
		t.Fatalf("LookupTarget(): %v", err)
	}
	if got != "1700000000.000100" {
		t.Fatalf("LookupTarget() = %q, want %q", got, "1700000000.000100")
	}

	if _, err := s.LookupTarget(ctx, "acme", model.PlatformTeams, "absent"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("LookupTarget(absent) error = %v, want ErrNotFound", err)
	}
}

func TestRecordDeliveryIsIdempotent(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	d := store.Delivery{
		PairID:         "acme",
		SourcePlatform: model.PlatformTeams,
		SourceID:       "teams-1",
		TargetPlatform: model.PlatformSlack,
		TargetID:       "1700000000.000100",
	}
	if err := s.RecordDelivery(ctx, d); err != nil {
		t.Fatalf("first RecordDelivery(): %v", err)
	}
	if err := s.RecordDelivery(ctx, d); err != nil {
		t.Fatalf("second RecordDelivery(): %v", err)
	}

	// Idempotency means the mapping survives unchanged, not merely that the
	// second call returned no error.
	got, err := s.LookupTarget(ctx, d.PairID, d.SourcePlatform, d.SourceID)
	if err != nil {
		t.Fatalf("LookupTarget(): %v", err)
	}
	if got != d.TargetID {
		t.Fatalf("LookupTarget() = %q, want the original %q", got, d.TargetID)
	}
}

func TestRecordDeliveryRejectsConflictingTarget(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	d := store.Delivery{
		PairID:         "acme",
		SourcePlatform: model.PlatformTeams,
		SourceID:       "teams-1",
		TargetPlatform: model.PlatformSlack,
		TargetID:       "1700000000.000100",
	}
	if err := s.RecordDelivery(ctx, d); err != nil {
		t.Fatalf("RecordDelivery(): %v", err)
	}

	// The same source message mapped to a different target means it was
	// mirrored twice and one copy is orphaned. That must not pass silently.
	conflicting := d
	conflicting.TargetID = "1700000000.000999"
	err := s.RecordDelivery(ctx, conflicting)
	if err == nil {
		t.Fatal("RecordDelivery(conflicting target) error = nil, want an error")
	}

	got, err := s.LookupTarget(ctx, d.PairID, d.SourcePlatform, d.SourceID)
	if err != nil {
		t.Fatalf("LookupTarget(): %v", err)
	}
	if got != d.TargetID {
		t.Fatalf("LookupTarget() = %q, want the original mapping %q to be preserved", got, d.TargetID)
	}
}

func TestMarkProcessedUnderConcurrency(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	const racers = 16
	var wg sync.WaitGroup
	claims := make(chan bool, racers)

	wg.Add(racers)
	for i := 0; i < racers; i++ {
		go func() {
			defer wg.Done()
			claimed, err := s.MarkProcessed(ctx, model.PlatformTeams, "contended", model.EventCreated)
			if err != nil {
				t.Errorf("MarkProcessed(): %v", err)
				return
			}
			claims <- claimed
		}()
	}
	wg.Wait()
	close(claims)

	won := 0
	for claimed := range claims {
		if claimed {
			won++
		}
	}
	// Exactly one caller may claim the event. More than one means the same
	// message gets mirrored twice; none means it is dropped entirely.
	if won != 1 {
		t.Fatalf("%d of %d concurrent callers claimed the event, want exactly 1", won, racers)
	}
}

func TestDeltaCursorRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	if _, err := s.LoadDeltaCursor(ctx, "acme"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("LoadDeltaCursor(absent) error = %v, want ErrNotFound", err)
	}

	if err := s.SaveDeltaLink(ctx, "acme", "https://graph.example/delta?token=1"); err != nil {
		t.Fatalf("SaveDeltaLink(): %v", err)
	}
	got, err := s.LoadDeltaCursor(ctx, "acme")
	if err != nil {
		t.Fatalf("LoadDeltaCursor(): %v", err)
	}
	if got.Link != "https://graph.example/delta?token=1" {
		t.Fatalf("LoadDeltaCursor().Link = %q", got.Link)
	}
	if got.UpdatedAt.IsZero() {
		t.Fatal("LoadDeltaCursor().UpdatedAt is zero, want the write timestamp")
	}

	if err := s.SaveDeltaLink(ctx, "acme", "https://graph.example/delta?token=2"); err != nil {
		t.Fatalf("SaveDeltaLink(overwrite): %v", err)
	}
	got, err = s.LoadDeltaCursor(ctx, "acme")
	if err != nil {
		t.Fatalf("LoadDeltaCursor(): %v", err)
	}
	if got.Link != "https://graph.example/delta?token=2" {
		t.Fatalf("LoadDeltaCursor().Link after overwrite = %q, want token=2", got.Link)
	}
	if got.UpdatedAt.IsZero() {
		t.Fatal("LoadDeltaCursor().UpdatedAt after overwrite is zero, want the new write timestamp")
	}
}

func TestSubscriptionRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	if _, err := s.LoadSubscription(ctx, "acme"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("LoadSubscription(absent) error = %v, want ErrNotFound", err)
	}

	expiresAt := time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC)
	if err := s.SaveSubscription(ctx, "acme", store.Subscription{ID: "sub-1", ExpiresAt: expiresAt}); err != nil {
		t.Fatalf("SaveSubscription(): %v", err)
	}
	got, err := s.LoadSubscription(ctx, "acme")
	if err != nil {
		t.Fatalf("LoadSubscription(): %v", err)
	}
	if got.ID != "sub-1" {
		t.Fatalf("LoadSubscription().ID = %q, want sub-1", got.ID)
	}
	if !got.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("LoadSubscription().ExpiresAt = %v, want %v", got.ExpiresAt, expiresAt)
	}

	// Saving again for the same pair replaces the stored subscription rather
	// than erroring or accumulating rows — only the latest matters.
	newExpiry := expiresAt.Add(time.Hour)
	if err := s.SaveSubscription(ctx, "acme", store.Subscription{ID: "sub-2", ExpiresAt: newExpiry}); err != nil {
		t.Fatalf("SaveSubscription(overwrite): %v", err)
	}
	got, err = s.LoadSubscription(ctx, "acme")
	if err != nil {
		t.Fatalf("LoadSubscription(): %v", err)
	}
	if got.ID != "sub-2" {
		t.Fatalf("LoadSubscription().ID after overwrite = %q, want sub-2", got.ID)
	}
	if !got.ExpiresAt.Equal(newExpiry) {
		t.Fatalf("LoadSubscription().ExpiresAt after overwrite = %v, want %v", got.ExpiresAt, newExpiry)
	}

	if err := s.DeleteSubscription(ctx, "acme"); err != nil {
		t.Fatalf("DeleteSubscription(): %v", err)
	}
	if _, err := s.LoadSubscription(ctx, "acme"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("LoadSubscription() after delete error = %v, want ErrNotFound", err)
	}

	// Deleting a row that was never there, or was already deleted, must not
	// be an error: the caller has no reliable way to know in advance.
	if err := s.DeleteSubscription(ctx, "never-existed"); err != nil {
		t.Fatalf("DeleteSubscription(absent): %v", err)
	}
}

func TestIsDeliveryResult(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	if err := s.RecordDelivery(ctx, store.Delivery{
		PairID:         "acme",
		SourcePlatform: model.PlatformTeams,
		SourceID:       "teams-1",
		TargetPlatform: model.PlatformSlack,
		TargetID:       "1700000000.000100",
	}); err != nil {
		t.Fatalf("RecordDelivery(): %v", err)
	}

	produced, err := s.IsDeliveryResult(ctx, model.PlatformSlack, "1700000000.000100")
	if err != nil {
		t.Fatalf("IsDeliveryResult(): %v", err)
	}
	if !produced {
		t.Fatal("IsDeliveryResult(own message) = false, want true")
	}

	foreign, err := s.IsDeliveryResult(ctx, model.PlatformSlack, "1700000000.999999")
	if err != nil {
		t.Fatalf("IsDeliveryResult(): %v", err)
	}
	if foreign {
		t.Fatal("IsDeliveryResult(foreign message) = true, want false")
	}
}

// TestLookupCounterpartResolvesBothDirections pins the property thread
// correlation depends on once both directions are live: a message may be the
// source or the target of a mapping, and the thread root of a conversation
// started on the far platform is always the target.
func TestLookupCounterpartResolvesBothDirections(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	d := store.Delivery{
		PairID:         "acme",
		SourcePlatform: model.PlatformTeams,
		SourceID:       "teams-1",
		TargetPlatform: model.PlatformSlack,
		TargetID:       "1700000000.000100",
	}
	if err := s.RecordDelivery(ctx, d); err != nil {
		t.Fatalf("RecordDelivery(): %v", err)
	}

	got, err := s.LookupCounterpart(ctx, "acme", model.PlatformTeams, "teams-1")
	if err != nil {
		t.Fatalf("LookupCounterpart(source side): %v", err)
	}
	if got.ID != "1700000000.000100" {
		t.Fatalf("LookupCounterpart(source side) = %q, want %q", got.ID, "1700000000.000100")
	}

	got, err = s.LookupCounterpart(ctx, "acme", model.PlatformSlack, "1700000000.000100")
	if err != nil {
		t.Fatalf("LookupCounterpart(target side): %v", err)
	}
	if got.ID != "teams-1" {
		t.Fatalf("LookupCounterpart(target side) = %q, want %q", got.ID, "teams-1")
	}

	if _, err := s.LookupCounterpart(ctx, "acme", model.PlatformTeams, "absent"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("LookupCounterpart(absent) error = %v, want ErrNotFound", err)
	}

	// A mapping belongs to its pair. Resolving across pairs would thread a
	// reply into another customer's channel.
	if _, err := s.LookupCounterpart(ctx, "other", model.PlatformSlack, "1700000000.000100"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("LookupCounterpart(other pair) error = %v, want ErrNotFound", err)
	}
}

// TestLookupTargetStaysForwardOnly guards the RecordDelivery conflict check,
// which asks "what is this source already mapped to". Answering that question
// with a backward match would report a conflict that does not exist.
func TestLookupTargetStaysForwardOnly(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	if err := s.RecordDelivery(ctx, store.Delivery{
		PairID:         "acme",
		SourcePlatform: model.PlatformTeams,
		SourceID:       "teams-1",
		TargetPlatform: model.PlatformSlack,
		TargetID:       "1700000000.000100",
	}); err != nil {
		t.Fatalf("RecordDelivery(): %v", err)
	}

	if _, err := s.LookupTarget(ctx, "acme", model.PlatformSlack, "1700000000.000100"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("LookupTarget(target id) error = %v, want ErrNotFound", err)
	}
}

// Readiness depends on knowing whether the database is actually reachable.
// Without this the pod stays Ready with a dead pool and every delivery fails.
func TestPingReportsReachability(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	if err := s.Ping(ctx); err != nil {
		t.Fatalf("Ping() on a live pool: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	if err := s.Ping(ctx); err == nil {
		t.Fatal("Ping() on a closed pool returned nil, want an error")
	}
}

// Slack names a thread by its root, and a Signal quote regularly points at a
// message from the middle of one. Without the root travelling with the
// mapping, a reply to a reply starts a second thread beside the conversation
// it belongs to.
func TestLookupCounterpartCarriesTheThreadRoot(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	root := store.Delivery{
		PairID:         "acme",
		SourcePlatform: model.PlatformSignal,
		SourceID:       "uuid-a:1754400000000",
		TargetPlatform: model.PlatformSlack,
		TargetID:       "1700000000.000100",
	}
	reply := store.Delivery{
		PairID:           "acme",
		SourcePlatform:   model.PlatformSignal,
		SourceID:         "uuid-b:1754400000001",
		TargetPlatform:   model.PlatformSlack,
		TargetID:         "1700000000.000200",
		TargetThreadRoot: "1700000000.000100",
	}
	for _, d := range []store.Delivery{root, reply} {
		if err := s.RecordDelivery(ctx, d); err != nil {
			t.Fatalf("RecordDelivery(): %v", err)
		}
	}

	got, err := s.LookupCounterpart(ctx, "acme", model.PlatformSignal, reply.SourceID)
	if err != nil {
		t.Fatalf("LookupCounterpart(): %v", err)
	}
	if got.ID != "1700000000.000200" {
		t.Fatalf("ID = %q, want the mirrored reply", got.ID)
	}
	if got.ThreadRoot != "1700000000.000100" {
		t.Fatalf("ThreadRoot = %q, want the thread root", got.ThreadRoot)
	}

	// A thread root of a message that started one is empty, not itself.
	got, err = s.LookupCounterpart(ctx, "acme", model.PlatformSignal, root.SourceID)
	if err != nil {
		t.Fatalf("LookupCounterpart(root): %v", err)
	}
	if got.ThreadRoot != "" {
		t.Fatalf("ThreadRoot = %q, want empty for a thread root", got.ThreadRoot)
	}
}

// Read from the target side, a mapping has to answer with the thread the
// message sat in on its *source* platform. Slack addresses a thread by its
// root, and a Signal quote regularly names a reply the bridge posted into one.
func TestLookupCounterpartCarriesTheSourceThreadRoot(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	reply := store.Delivery{
		PairID:           "acme",
		SourcePlatform:   model.PlatformSlack,
		SourceID:         "1700000000.000200",
		TargetPlatform:   model.PlatformSignal,
		TargetID:         "uuid-self:22",
		SourceThreadRoot: "1700000000.000100",
	}
	if err := s.RecordDelivery(ctx, reply); err != nil {
		t.Fatalf("RecordDelivery(): %v", err)
	}

	got, err := s.LookupCounterpart(ctx, "acme", model.PlatformSignal, "uuid-self:22")
	if err != nil {
		t.Fatalf("LookupCounterpart(): %v", err)
	}
	if got.ID != "1700000000.000200" {
		t.Fatalf("ID = %q", got.ID)
	}
	if got.ThreadRoot != "1700000000.000100" {
		t.Fatalf("ThreadRoot = %q, want the Slack thread root", got.ThreadRoot)
	}
}

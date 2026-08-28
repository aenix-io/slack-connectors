package main

import (
	"errors"
	"net/http"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/aenix-io/slack-connectors/internal/bridge"
	"github.com/aenix-io/slack-connectors/internal/metrics"
	"github.com/aenix-io/slack-connectors/internal/model"
)

// TestRecordOutcome exercises recordOutcome — the sole production consumer of
// Handle's (delivered, reason, err) contract — against the actual
// metrics package counters. Each case uses a pair/target/reason combination
// unique to that case, since the counters are process-global: that keeps
// assertions exact (a fresh label starts at zero) without one subtest's
// increments leaking into another's expectation.
func TestRecordOutcome(t *testing.T) {
	t.Run("delivered increments delivered only", func(t *testing.T) {
		const pair, target = "pair-delivered", "slack"

		recordOutcome(pair, target, true, "", nil)

		if got := testutil.ToFloat64(metrics.DeliveredTotal.WithLabelValues(pair, target)); got != 1 {
			t.Fatalf("DeliveredTotal(%s,%s) = %v, want 1", pair, target, got)
		}
		if got := testutil.ToFloat64(metrics.FailedTotal.WithLabelValues(pair, target)); got != 0 {
			t.Fatalf("FailedTotal(%s,%s) = %v, want 0 — a delivered message must not also count as failed", pair, target, got)
		}
	})

	// Both directions are live once the Teams sink is registered, so the
	// target label is no longer always "slack". A metric that silently only
	// ever counted one direction would hide half the traffic.
	t.Run("the teams target is counted under its own label", func(t *testing.T) {
		const pair = "pair-bidirectional"

		recordOutcome(pair, string(model.PlatformTeams), true, "", nil)

		if got := testutil.ToFloat64(metrics.DeliveredTotal.WithLabelValues(pair, "teams")); got != 1 {
			t.Fatalf("DeliveredTotal(%s,teams) = %v, want 1", pair, got)
		}
		if got := testutil.ToFloat64(metrics.DeliveredTotal.WithLabelValues(pair, "slack")); got != 0 {
			t.Fatalf("DeliveredTotal(%s,slack) = %v, want 0 — a Teams delivery must not count as a Slack one", pair, got)
		}

		recordOutcome(pair, string(model.PlatformTeams), false, "", errors.New("post message to channel-1: graph 403 Forbidden"))

		if got := testutil.ToFloat64(metrics.FailedTotal.WithLabelValues(pair, "teams")); got != 1 {
			t.Fatalf("FailedTotal(%s,teams) = %v, want 1", pair, got)
		}
	})

	t.Run("each drop reason increments dropped with that exact label only", func(t *testing.T) {
		reasons := []string{
			bridge.ReasonFromBridge,
			bridge.ReasonEcho,
			bridge.ReasonDuplicate,
			bridge.ReasonNotCreated,
		}
		for _, reason := range reasons {
			t.Run(reason, func(t *testing.T) {
				// Each reason gets its own pair label: the counters are
				// process-global, and sharing one pair across subtests would
				// let an earlier reason's increment still be sitting under
				// this subtest's "must not have fired" checks below.
				pair, target := "pair-dropped-"+reason, "slack"

				recordOutcome(pair, target, false, reason, nil)

				if got := testutil.ToFloat64(metrics.DroppedTotal.WithLabelValues(pair, reason)); got != 1 {
					t.Fatalf("DroppedTotal(%s,%s) = %v, want 1", pair, reason, got)
				}
				if got := testutil.ToFloat64(metrics.DeliveredTotal.WithLabelValues(pair, target)); got != 0 {
					t.Fatalf("DeliveredTotal(%s,%s) = %v, want 0 — a drop must never count as a delivery", pair, target, got)
				}
				if got := testutil.ToFloat64(metrics.FailedTotal.WithLabelValues(pair, target)); got != 0 {
					t.Fatalf("FailedTotal(%s,%s) = %v, want 0 — a drop is not a failure", pair, target, got)
				}
				// A drop must land under exactly its own reason label, not
				// bleed into a sibling reason's bucket.
				for _, other := range reasons {
					if other == reason {
						continue
					}
					if got := testutil.ToFloat64(metrics.DroppedTotal.WithLabelValues(pair, other)); got != 0 {
						t.Fatalf("DroppedTotal(%s,%s) = %v, want 0 — reason %q must not increment reason %q",
							pair, other, got, reason, other)
					}
				}
			})
		}
	})

	t.Run("an error increments failed only", func(t *testing.T) {
		const pair, target = "pair-failed", "slack"

		recordOutcome(pair, target, false, "", errors.New("deliver to slack: slack is down"))

		if got := testutil.ToFloat64(metrics.FailedTotal.WithLabelValues(pair, target)); got != 1 {
			t.Fatalf("FailedTotal(%s,%s) = %v, want 1", pair, target, got)
		}
		if got := testutil.ToFloat64(metrics.DeliveredTotal.WithLabelValues(pair, target)); got != 0 {
			t.Fatalf("DeliveredTotal(%s,%s) = %v, want 0 — an error must never count as a delivery", pair, target, got)
		}
		if got := testutil.ToFloat64(metrics.DroppedTotal.WithLabelValues(pair, "")); got != 0 {
			t.Fatalf("DroppedTotal(%s,\"\") = %v, want 0 — an error is not a drop", pair, got)
		}
	})
}

// Both webhooks claim a processing slot before reading the request body, so an
// unbounded read holds a slot for as long as the client cares to stall. With
// only ReadHeaderTimeout set, thirty-two unauthenticated connections that send
// headers and then dawdle mid-body took the endpoint permanently to 503.
func TestNewServerBoundsEveryPhaseOfARequest(t *testing.T) {
	srv := newServer(http.NewServeMux())

	if srv.ReadHeaderTimeout <= 0 {
		t.Error("ReadHeaderTimeout is unset")
	}
	if srv.ReadTimeout <= 0 {
		t.Error("ReadTimeout is unset — a stalled body can hold a processing slot indefinitely")
	}
	if srv.WriteTimeout <= 0 {
		t.Error("WriteTimeout is unset")
	}
	if srv.IdleTimeout <= 0 {
		t.Error("IdleTimeout is unset")
	}
}

// A delivery runs on a detached context, so a call with no timeout cannot be
// cancelled by anything: it parks its goroutine and its processing slot for
// good.
func TestNewOutboundClientHasATimeout(t *testing.T) {
	if got := newOutboundClient(nil).Timeout; got <= 0 {
		t.Fatalf("Timeout = %v, want a positive bound", got)
	}
}

// The oauth2 transport carries the Graph authentication. Replacing it instead
// of wrapping it would produce a client that reaches Graph unauthenticated.
func TestNewOutboundClientKeepsTheSuppliedTransport(t *testing.T) {
	marker := &markerTransport{}
	if got := newOutboundClient(marker).Transport; got != marker {
		t.Fatalf("Transport = %v, want the transport it was given", got)
	}
}

type markerTransport struct{}

func (*markerTransport) RoundTrip(*http.Request) (*http.Response, error) { return nil, nil }

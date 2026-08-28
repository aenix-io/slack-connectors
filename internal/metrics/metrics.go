// Package metrics exposes bridge counters in Prometheus exposition format,
// which VictoriaMetrics scrapes directly.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// DeliveredTotal counts messages successfully mirrored.
	DeliveredTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bridge_delivered_total",
		Help: "Messages successfully delivered to the target platform.",
	}, []string{"pair", "target"})

	// FailedTotal counts messages that could not be mirrored.
	FailedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bridge_failed_total",
		Help: "Messages that failed delivery to the target platform.",
	}, []string{"pair", "target"})

	// DroppedTotal counts messages the bridge deliberately declined to route:
	// messages it authored itself, echoes of its own deliveries, duplicate
	// events and non-created events. Folding these into DeliveredTotal is
	// what made that counter overstate real traffic, especially after a
	// restart replays history through the recovery sweep.
	DroppedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bridge_dropped_total",
		Help: "Messages deliberately not routed, by reason.",
	}, []string{"pair", "reason"})

	// SubscriptionRenewalFailuresTotal counts failed Graph subscription
	// renewals. A stalled renewal loop makes the bridge go deaf within an
	// hour with no other symptom, so any increase here must be alertable.
	SubscriptionRenewalFailuresTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bridge_subscription_renewal_failures_total",
		Help: "Graph subscription renewals that failed.",
	}, []string{"pair"})

	// SubscriptionExpiresSeconds is the unix time, in seconds, at which the
	// current subscription for a pair expires. An alert should fire when
	// this value falls below the current time.
	SubscriptionExpiresSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bridge_subscription_expires_seconds",
		Help: "Unix time in seconds at which the pair's current Graph subscription expires.",
	}, []string{"pair"})

	// SignalSendFailuresTotal counts per-recipient delivery failures reported
	// by signal-cli. A group send can succeed as a whole while individual
	// members' copies fail, which is invisible in the delivered counter.
	SignalSendFailuresTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bridge_signal_send_failures_total",
		Help: "Per-recipient Signal delivery failures, by outcome.",
	}, []string{"pair", "type"})

	// SignalProofRequiredTotal counts CAPTCHA challenges from Signal's
	// anti-abuse system. Until an operator solves one by hand and submits it,
	// the account is throttled or blocked — and the send that triggered the
	// challenge still reports a timestamp, so nothing else makes this visible.
	SignalProofRequiredTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bridge_signal_proof_required_total",
		Help: "CAPTCHA challenges raised by Signal while sending.",
	}, []string{"pair"})

	// ReactionDeliveredTotal and ReactionFailedTotal count mirrored
	// reactions. They are separate from the message counters because the
	// volumes differ by an order of magnitude and would drown the numbers
	// anyone actually watches.
	ReactionDeliveredTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bridge_reaction_delivered_total",
		Help: "Reactions successfully mirrored to the target platform.",
	}, []string{"pair"})

	ReactionFailedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bridge_reaction_failed_total",
		Help: "Reactions that failed to mirror.",
	}, []string{"pair"})

	// ReactionOverwrittenTotal counts reactions displaced because the target
	// platform holds only one per sender and every mirrored reaction comes
	// from the bridge's single identity. After the pilot this says whether
	// the compromise was noise or a nuisance.
	ReactionOverwrittenTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bridge_reaction_overwritten_total",
		Help: "Mirrored reactions displaced by a newer one on the target platform.",
	}, []string{"pair"})

	// SignalConnected and SignalSubscribed are the two ways this bridge goes
	// deaf: the socket to the sidecar dies, or the subscription that makes it
	// deliver is gone. Neither produces an error on its own — the pod simply
	// stops hearing anything — so both are gauges an alert can watch.
	SignalConnected = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "bridge_signal_connected",
		Help: "1 when the JSON-RPC connection to signal-cli is up.",
	})

	SignalSubscribed = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "bridge_signal_subscription_active",
		Help: "1 when the bridge is subscribed to incoming Signal messages.",
	})

	// SignalClientReleaseTimestampSeconds is when the signal-cli in the
	// sidecar was released. Clients expire about three months after release
	// and the server then refuses them, so this is the clock an alert counts
	// down — and it keeps working when the workflow that bumps the image does
	// not.
	SignalClientReleaseTimestampSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "bridge_signal_client_release_timestamp_seconds",
		Help: "Unix time at which the running signal-cli was released.",
	})

	// SignalInflightLostTotal counts messages read from the socket but not
	// delivered when the process stopped. signal-cli has already acknowledged
	// them to the Signal server, and nothing replays them, so this is the one
	// gap in the delivery guarantee and it has to be measurable.
	SignalInflightLostTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bridge_signal_inflight_lost_total",
		Help: "Messages received but not delivered before shutdown.",
	}, []string{"pair"})

	// AttachmentBytesTotal is transfer volume, which is also how fast the
	// sidecar's volume would fill if cleanup ever stopped working.
	AttachmentBytesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bridge_attachment_bytes_total",
		Help: "Bytes transferred as attachments.",
	}, []string{"pair", "direction"})

	// ExpiringMessagesObservedTotal counts messages arriving in a
	// conversation with a disappearing-message timer. The Slack copy does not
	// disappear, and nobody should learn that from the customer.
	ExpiringMessagesObservedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "bridge_expiring_messages_observed_total",
		Help: "Messages seen in a conversation with a disappearing-message timer.",
	}, []string{"pair"})

	registry = prometheus.NewRegistry()
)

func init() {
	registry.MustRegister(DeliveredTotal, FailedTotal, DroppedTotal,
		SubscriptionRenewalFailuresTotal, SubscriptionExpiresSeconds,
		SignalSendFailuresTotal, SignalProofRequiredTotal,
		ReactionDeliveredTotal, ReactionFailedTotal, ReactionOverwrittenTotal,
		SignalConnected, SignalSubscribed, SignalClientReleaseTimestampSeconds,
		SignalInflightLostTotal, AttachmentBytesTotal, ExpiringMessagesObservedTotal)
}

// Handler serves the metrics endpoint.
func Handler() http.Handler {
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
}

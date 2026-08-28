// Command signal-bridge mirrors a Signal conversation into a Slack channel.
//
// It speaks to a signal-cli daemon running as a sidecar over a unix socket.
// That socket is unauthenticated and grants full control of the bridge's
// Signal account, so it must never be reachable from the pod network.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	goslack "github.com/slack-go/slack"

	signaladapter "github.com/aenix-io/slack-connectors/internal/adapter/signal"
	"github.com/aenix-io/slack-connectors/internal/adapter/slack"
	"github.com/aenix-io/slack-connectors/internal/bridge"
	"github.com/aenix-io/slack-connectors/internal/config"
	"github.com/aenix-io/slack-connectors/internal/format/signaltext"
	"github.com/aenix-io/slack-connectors/internal/health"
	"github.com/aenix-io/slack-connectors/internal/metrics"
	"github.com/aenix-io/slack-connectors/internal/model"
	"github.com/aenix-io/slack-connectors/internal/store/postgres"
	"github.com/aenix-io/slack-connectors/internal/version"
)

// outboundTimeout bounds every call this service makes to Slack. Without it a
// blackholed connection parks the receive loop indefinitely — and because
// that loop is what applies back-pressure to signal-cli, a stall there stops
// the whole conversation rather than one message.
const outboundTimeout = 30 * time.Second

// signalCallTimeout bounds a single JSON-RPC call to the sidecar.
const signalCallTimeout = 30 * time.Second

// sourceName is what a Signal message is attributed to in Slack when its
// author has no profile name we can read.
const sourceName = "Signal"

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(log); err != nil {
		log.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	log.Info("starting", "version", version.String())

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	var e env
	databaseURL := e.require("DATABASE_URL")
	slackToken := e.require("SLACK_BOT_TOKEN")
	slackSigningSecret := e.require("SLACK_SIGNING_SECRET")
	signalAccount := e.require("SIGNAL_ACCOUNT")
	if err := e.err(); err != nil {
		return err
	}
	socketPath := os.Getenv("SIGNAL_SOCKET")
	if socketPath == "" {
		socketPath = "/run/signal/socket"
	}
	// Where the sidecar writes received files. The bridge deletes them once
	// they have been transferred; signal-cli never does, and the volume it
	// would fill also holds the account's identity keys. An empty path means
	// the volume is not shared with this container, so cleanup is skipped.
	attachmentsDir := os.Getenv("SIGNAL_ATTACHMENTS_DIR")

	st, err := postgres.New(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer func() { _ = st.Close() }()

	slackHTTP := &http.Client{Timeout: outboundTimeout}
	slackClient := goslack.New(slackToken, goslack.OptionHTTPClient(slackHTTP))

	// The Signal sink needs the connection, which needs the stream, which
	// needs the dispatcher, which needs the bridge — so the sink is built
	// first and handed its client once there is one.
	signalSink := signaladapter.NewSink(nil, cfg)
	slackSink := slack.New(slackClient, renderSignal(cfg), sourceName)
	slackSink.SetLogger(log)
	signalSink.SetLogger(log)

	br := bridge.New(cfg, st, map[model.Platform]bridge.Sink{
		model.PlatformSlack:  slackSink,
		model.PlatformSignal: signalSink,
	}, log)

	br.OnReactionOverwritten(func(pairID string) {
		metrics.ReactionOverwrittenTotal.WithLabelValues(pairID).Inc()
	})

	dispatch := func(ctx context.Context, msg model.Message) error {
		delivered, reason, err := br.Handle(ctx, msg)
		recordOutcome(msg.PairID, targetLabel(cfg, msg), delivered, reason, err)
		if err != nil {
			log.Error("handle message", "pair", msg.PairID, "id", msg.ID, "error", err)
		}
		return err
	}

	dispatchReaction := func(ctx context.Context, r model.Reaction) error {
		delivered, reason, err := br.HandleReaction(ctx, r)
		recordReactionOutcome(r.PairID, delivered, reason, err)
		if err != nil {
			log.Error("handle reaction",
				"pair", r.PairID, "target", r.TargetID, "error", err)
		}
		return err
	}

	// state tracks whether this pod can carry a message at all. A bridge
	// whose socket died is deaf, and a deaf pod reporting Ready is how an
	// outage becomes invisible.
	var state connectionState

	// The stream is built before the client because the client needs its
	// handler, and the handler is what runs on the read loop.
	stream := signaladapter.NewStream(cfg, dispatch, "", signalAccount, log)
	stream.OnReaction(dispatchReaction)
	stream.SetAttachmentsDir(attachmentsDir)

	// A file that reached Slack is one the sidecar no longer needs to keep.
	slackSink.OnAttachmentTransferred(func(a model.Attachment) {
		if err := stream.DiscardAttachment(a.SourceURL); err != nil {
			log.Warn("could not remove a transferred attachment from the signal volume",
				"id", a.SourceURL, "error", err)
		}
	})

	client, err := dialWithRetry(ctx, socketPath, stream.Handle, log)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()
	signalSink.SetClient(client)
	stream.SetClient(client)

	serveErr := make(chan error, 1)
	go func() {
		err := client.Serve(ctx)
		state.connected.Store(false)
		state.subscribed.Store(false)
		metrics.SignalConnected.Set(0)
		metrics.SignalSubscribed.Set(0)
		serveErr <- err
	}()
	state.connected.Store(true)
	metrics.SignalConnected.Set(1)

	// Ask signal-cli who we are before subscribing. Sync transcripts of
	// messages sent from a linked phone are attributed to this identity, and
	// a quote of one names the account by service id — so without it those
	// replies would not thread.
	if uuid, err := selfUUID(ctx, client, signalAccount); err != nil {
		// Not fatal: everything except sync attribution works without it.
		log.Warn("could not resolve the bridge account's service id; messages sent from a linked device may not thread",
			"error", err)
	} else {
		stream.SetSelf(uuid, signalAccount)
		// The sink needs it too: a message this bridge posts is recorded
		// under this identity, and an incoming quote of it names the same one.
		signalSink.SetSelf(uuid)
		log.Info("resolved the bridge account", "account", signalAccount)
	}

	subscribeCtx, cancelSubscribe := context.WithTimeout(ctx, signalCallTimeout)
	err = stream.Subscribe(subscribeCtx, client)
	cancelSubscribe()
	if err != nil {
		return err
	}
	state.subscribed.Store(true)
	metrics.SignalSubscribed.Set(1)

	// The sidecar's release date is baked into its image and passed in, so the
	// expiry alert works even when the workflow that bumps the image does not.
	if release := os.Getenv("SIGNAL_CLI_RELEASED_AT"); release != "" {
		if t, err := time.Parse(time.RFC3339, release); err == nil {
			metrics.SignalClientReleaseTimestampSeconds.Set(float64(t.Unix()))
		} else {
			log.Warn("SIGNAL_CLI_RELEASED_AT is not an RFC3339 timestamp; the client-expiry alert will not fire",
				"value", release)
		}
	} else {
		log.Warn("SIGNAL_CLI_RELEASED_AT is unset; nothing will warn before the signal-cli client expires")
	}

	slackWebhook := slack.NewHandler(
		slackSigningSecret, cfg, slack.NewUserDirectory(slackClient), dispatch, log)
	slackWebhook.OnReaction(dispatchReaction)
	slackWebhook.OnDownload(func(ctx context.Context, url string) (io.ReadCloser, error) {
		return downloadSlackFile(ctx, slackHTTP, slackToken, url)
	})

	mux := http.NewServeMux()
	mux.Handle("/webhook/slack", slackWebhook)
	mux.Handle("/metrics", metrics.Handler())
	mux.Handle("/healthz", health.LivenessHandler())
	mux.Handle("/readyz", health.ReadinessHandler(log,
		health.Check{Name: "database", Fn: st.Ping},
		health.Check{Name: "signal_connection", Fn: state.checkConnected},
		health.Check{Name: "signal_subscription", Fn: state.checkSubscribed},
	))

	srv := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", srv.Addr, err)
	}
	go func() {
		log.Info("listening", "addr", ln.Addr().String())
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()

	shutdown := func() error {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		// Stop accepting first, then wait for events already acknowledged to
		// Slack. Skipping the drain would mean telling Slack we took
		// responsibility for a message and then dropping it.
		serveErr := srv.Shutdown(shutdownCtx)
		if serveErr != nil {
			log.Error("shutdown http server", "error", serveErr)
		}
		if err := slackWebhook.Drain(shutdownCtx); err != nil {
			log.Error("slack events did not drain before the deadline", "error", err)
		}
		return serveErr
	}

	select {
	case err := <-serveErr:
		_ = shutdown()
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	case <-ctx.Done():
		log.Info("shutting down")
	}
	return shutdown()
}

// targetLabel names the platform a message is headed for, for the metric
// label alone. The delivery itself resolves its target inside Handle, from
// the same pair.
func targetLabel(cfg config.Config, msg model.Message) string {
	pair, ok := cfg.PairByID(msg.PairID)
	if !ok {
		return "unknown"
	}
	target, _, ok := pair.Counterpart(msg.Platform)
	if !ok {
		return "unknown"
	}
	return string(target)
}

// dialStartupBudget is how long the bridge waits for the sidecar's socket to
// appear before giving up.
//
// The two containers start together and signal-cli has an account to open and
// a native library to unpack, so the socket is reliably absent for the first
// moments of a pod's life. Failing immediately turns every rollout into a
// CrashLoopBackOff that resolves itself and looks, in the meantime, exactly
// like a broken deployment.
const dialStartupBudget = 2 * time.Minute

// dialWithRetry connects to the sidecar, waiting for it to come up.
func dialWithRetry(
	ctx context.Context, socketPath string, handler signaladapter.NotificationHandler, log *slog.Logger,
) (*signaladapter.Client, error) {
	deadline := time.Now().Add(dialStartupBudget)
	backoff := 200 * time.Millisecond

	for attempt := 1; ; attempt++ {
		client, err := signaladapter.Dial(ctx, "unix", socketPath, handler, log)
		if err == nil {
			return client, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("signal-cli socket at %s did not appear within %s: %w",
				socketPath, dialStartupBudget, err)
		}
		log.Info("waiting for the signal-cli socket",
			"path", socketPath, "attempt", attempt, "retry_in", backoff)

		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

// downloadSlackFile fetches a Slack-hosted file.
//
// url_private is not public: without the bot token in the header Slack
// answers with an HTML login page and a 200, so a caller that trusted the
// status code would hand that page to the customer as their document.
func downloadSlackFile(ctx context.Context, client *http.Client, token, url string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build file request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", url, err)
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("fetch %s: status %d", url, resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); strings.HasPrefix(ct, "text/html") {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("fetch %s: got a login page, the token cannot read this file", url)
	}
	return resp.Body, nil
}

// renderSignal converts a Signal body — plain text with formatting carried in
// ranges beside it — into Slack mrkdwn, turning mentions of mapped people
// into real Slack mentions.
func renderSignal(cfg config.Config) slack.Renderer {
	return func(msg model.Message) string {
		return signaltext.ToMrkdwn(msg.Text, msg.TextStyles, msg.Mentions,
			func(serviceID string) (string, bool) {
				return cfg.SlackUserFor(msg.PairID, serviceID)
			})
	}
}

// selfUUID asks signal-cli for the service id of the account it runs.
//
// listAccounts returns phone numbers only, so the account's own registration
// status is what carries the uuid.
func selfUUID(ctx context.Context, client *signaladapter.Client, account string) (string, error) {
	callCtx, cancel := context.WithTimeout(ctx, signalCallTimeout)
	defer cancel()

	var statuses []struct {
		Recipient    string `json:"recipient"`
		Number       string `json:"number"`
		UUID         string `json:"uuid"`
		IsRegistered bool   `json:"isRegistered"`
	}
	if err := client.Call(callCtx, "getUserStatus",
		map[string]any{"recipient": []string{account}}, &statuses); err != nil {
		return "", err
	}
	for _, s := range statuses {
		if s.UUID != "" {
			return s.UUID, nil
		}
	}
	return "", fmt.Errorf("signal-cli reported no service id for %s", account)
}

// connectionState is the readiness state the probes read.
type connectionState struct {
	connected  atomic.Bool
	subscribed atomic.Bool
}

func (h *connectionState) checkConnected(context.Context) error {
	if !h.connected.Load() {
		return errors.New("the signal-cli connection is down")
	}
	return nil
}

func (h *connectionState) checkSubscribed(context.Context) error {
	if !h.subscribed.Load() {
		return errors.New("not subscribed to signal messages")
	}
	return nil
}

// recordOutcome turns Handle's (delivered, reason, err) contract into exactly
// one metric, the same way the Teams binary does.
func recordOutcome(pairID, target string, delivered bool, reason string, err error) {
	switch {
	case err != nil:
		metrics.FailedTotal.WithLabelValues(pairID, target).Inc()
	case !delivered:
		metrics.DroppedTotal.WithLabelValues(pairID, reason).Inc()
	default:
		metrics.DeliveredTotal.WithLabelValues(pairID, target).Inc()
	}
}

// recordReactionOutcome mirrors recordOutcome for the reaction path, where
// "delivered" means the target platform now shows what the source does.
func recordReactionOutcome(pairID string, delivered bool, reason string, err error) {
	switch {
	case err != nil:
		metrics.ReactionFailedTotal.WithLabelValues(pairID).Inc()
	case !delivered:
		metrics.DroppedTotal.WithLabelValues(pairID, reason).Inc()
	default:
		metrics.ReactionDeliveredTotal.WithLabelValues(pairID).Inc()
	}
}

func loadConfig() (config.Config, error) {
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		path = "/etc/bridge/config.yaml"
	}
	f, err := os.Open(path) //nolint:gosec // operator-supplied configuration path
	if err != nil {
		return config.Config{}, fmt.Errorf("open config: %w", err)
	}
	defer func() { _ = f.Close() }()
	return config.Load(f)
}

// env collects required configuration, reporting every missing variable at
// once rather than failing on whichever is read first.
type env struct {
	missing []string
}

func (e *env) require(name string) string {
	v := os.Getenv(name)
	if v == "" {
		e.missing = append(e.missing, name)
	}
	return v
}

func (e *env) err() error {
	if len(e.missing) == 0 {
		return nil
	}
	return fmt.Errorf("required environment variables are empty: %s",
		strings.Join(e.missing, ", "))
}

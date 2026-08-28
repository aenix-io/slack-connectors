// Command bridge mirrors Microsoft Teams channel messages into Slack.
package main

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	goslack "github.com/slack-go/slack"
	"golang.org/x/oauth2/clientcredentials"

	"github.com/aenix-io/slack-connectors/internal/adapter/slack"
	"github.com/aenix-io/slack-connectors/internal/adapter/teams"
	"github.com/aenix-io/slack-connectors/internal/bridge"
	"github.com/aenix-io/slack-connectors/internal/config"
	"github.com/aenix-io/slack-connectors/internal/format/teamshtml"
	"github.com/aenix-io/slack-connectors/internal/health"
	"github.com/aenix-io/slack-connectors/internal/metrics"
	"github.com/aenix-io/slack-connectors/internal/model"
	"github.com/aenix-io/slack-connectors/internal/store/postgres"
	"github.com/aenix-io/slack-connectors/internal/version"
)

const graphBaseURL = "https://graph.microsoft.com/v1.0"

// outboundTimeout bounds every call this service makes to Graph and to Slack.
//
// Without it a blackholed connection parks a delivery goroutine forever while
// it holds a webhook processing slot, and since deliveries run on a detached
// context there is nothing left to cancel it. Thirty-two such calls take the
// endpoint permanently to 503.
const outboundTimeout = 30 * time.Second

// newServer builds the HTTP surface with every timeout set.
//
// ReadTimeout is the one that matters most: both webhooks claim a processing
// slot before reading the body, so a request that sends its headers and then
// stalls mid-body holds a slot for as long as the read is allowed to take.
// With only ReadHeaderTimeout set, that was forever, and thirty-two
// unauthenticated connections could take the endpoint down — no signature, no
// clientState, nothing but the public URL required.
func newServer(handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              ":8080",
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

// newOutboundClient builds an HTTP client for a platform API. transport may be
// nil, in which case the default is used.
func newOutboundClient(transport http.RoundTripper) *http.Client {
	return &http.Client{Timeout: outboundTimeout, Transport: transport}
}

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

	// Read every required variable before failing, so an operator sees the
	// whole list at once instead of fixing them one restart at a time.
	var e env
	databaseURL := e.require("DATABASE_URL")
	keyPath := e.require("GRAPH_ENCRYPTION_KEY_PATH")
	slackToken := e.require("SLACK_BOT_TOKEN")
	slackSigningSecret := e.require("SLACK_SIGNING_SECRET")
	clientState := e.require("GRAPH_CLIENT_STATE")
	azureClientID := e.require("AZURE_CLIENT_ID")
	azureClientSecret := e.require("AZURE_CLIENT_SECRET")
	azureTenantID := e.require("AZURE_TENANT_ID")
	botAppID := e.require("BOT_APP_ID")
	botAppSecret := e.require("BOT_APP_SECRET")
	publicWebhookURL := e.require("PUBLIC_WEBHOOK_URL")
	encryptionCert := e.require("GRAPH_ENCRYPTION_CERT")
	encryptionCertID := e.require("GRAPH_ENCRYPTION_CERT_ID")
	if err := e.err(); err != nil {
		return err
	}

	st, err := postgres.New(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	// Closing is conditional: if in-flight work does not drain before the
	// deadline, those goroutines are still using this pool.
	closeStore := true
	defer func() {
		if closeStore {
			_ = st.Close()
		}
	}()

	privateKey, err := loadPrivateKey(keyPath)
	if err != nil {
		return err
	}

	// The oauth2 helper returns a client wrapping http.DefaultClient, which has
	// no timeout. Its transport carries the authentication, so it is kept and
	// only the timeout is added.
	graphHTTP := newOutboundClient((&clientcredentials.Config{
		ClientID:     azureClientID,
		ClientSecret: azureClientSecret,
		TokenURL: fmt.Sprintf("https://login.microsoftonline.com/%s/oauth2/v2.0/token",
			azureTenantID),
		Scopes: []string{"https://graph.microsoft.com/.default"},
	}).Client(ctx).Transport)

	graphClient := teams.NewGraphClient(graphBaseURL, graphHTTP)

	// Posting into Teams goes through the Bot Framework connector, not Graph:
	// application-only Graph cannot create channel messages, and RSC has no
	// send permission. The bot is its own registration with its own
	// credentials — the Teams Developer Portal mints a separate id.
	// Loop prevention needs no knowledge of that id: the bridge recognises
	// its own posts by the recorded delivery result, not by author.
	//
	// Multi-tenant bots take their token from the botframework.com authority.
	// Azure has deprecated multi-tenant bot creation, so new registrations are
	// single-tenant, and the connector then rejects a botframework.com token
	// with 401. For those, set BOT_APP_TENANT_ID to the bot app's home tenant
	// and the token is minted from that tenant's authority instead.
	//nolint:gosec // An OAuth token endpoint, not a credential.
	botTokenURL := "https://login.microsoftonline.com/botframework.com/oauth2/v2.0/token"
	if botTenantID := os.Getenv("BOT_APP_TENANT_ID"); botTenantID != "" {
		botTokenURL = fmt.Sprintf(
			"https://login.microsoftonline.com/%s/oauth2/v2.0/token", botTenantID)
	}
	botHTTP := newOutboundClient((&clientcredentials.Config{
		ClientID:     botAppID,
		ClientSecret: botAppSecret,
		TokenURL:     botTokenURL,
		Scopes:       []string{"https://api.botframework.com/.default"},
	}).Client(ctx).Transport)

	botServiceURL := os.Getenv("TEAMS_BOT_SERVICE_URL")
	if botServiceURL == "" {
		botServiceURL = "https://smba.trafficmanager.net/teams/"
	}
	botClient := teams.NewBotClient(botServiceURL, botHTTP)

	slackClient := goslack.New(slackToken,
		goslack.OptionHTTPClient(newOutboundClient(nil)))

	// A sink for each platform: the routing core has always been
	// bidirectional, and registering both is what makes it act on it.
	br := bridge.New(cfg, st, map[model.Platform]bridge.Sink{
		model.PlatformSlack: slack.New(slackClient, renderTeams, "Microsoft Teams"),
		// Posting into Teams goes through the Bot Framework connector, not
		// Graph: application-only Graph cannot create channel messages.
		model.PlatformTeams: teams.NewSink(botClient, cfg),
	}, log)

	// The error is returned as well as recorded: the recovery sweep needs it to
	// decide whether its delta cursor may advance, and advancing past a
	// message that never arrived loses it for good.
	dispatch := func(ctx context.Context, msg model.Message) error {
		target := targetLabel(cfg, msg)
		delivered, reason, err := br.Handle(ctx, msg)
		recordOutcome(msg.PairID, target, delivered, reason, err)
		if err != nil {
			log.Error("handle message", "pair", msg.PairID, "id", msg.ID, "error", err)
		}
		return err
	}

	graphWebhook := teams.NewHandler(cfg, privateKey, clientState, dispatch, log)
	slackWebhook := slack.NewHandler(
		slackSigningSecret, cfg, slack.NewUserDirectory(slackClient), dispatch, log)

	// Constructed before the mux so readiness can consult it. Creating it
	// starts nothing; Run below does that, deliberately last.
	manager := teams.NewSubscriptionManager(
		graphClient,
		cfg,
		st,
		subscriptionMetricsAdapter{},
		teams.ManagerOptions{
			NotificationURL:         publicWebhookURL,
			ClientState:             clientState,
			EncryptionCertificate:   encryptionCert,
			EncryptionCertificateID: encryptionCertID,
		},
		log,
	)

	mux := http.NewServeMux()
	mux.Handle("/webhook/graph", graphWebhook)
	mux.Handle("/webhook/slack", slackWebhook)
	mux.Handle("/metrics", metrics.Handler())
	mux.Handle("/healthz", health.LivenessHandler())
	mux.Handle("/readyz", health.ReadinessHandler(log,
		health.Check{Name: "database", Fn: st.Ping},
		// A pod with no subscription hears nothing from Teams. Reporting Ready
		// in that state is how the bridge went deaf with no visible symptom.
		health.Check{Name: "graph_subscriptions", Fn: func(context.Context) error {
			return manager.Healthy()
		}},
	))

	srv := newServer(mux)

	// Bind the listener synchronously. Graph subscriptions outlive this
	// process, so a subscription created by a previous run can be delivering
	// notifications right now; anything that arrives before the socket is
	// bound is refused. net.Listen returning means the port is accepting.
	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", srv.Addr, err)
	}

	// Constructed before shutdown so that closure can drain it.
	recovery := teams.NewRecovery(graphClient, cfg, st, dispatch, log)

	// Sized for every goroutine that can send on it — serve, recovery, and the
	// subscription manager — so two simultaneous failures cannot block the
	// second sender forever waiting for a reader that has already moved on to
	// shutdown.
	errCh := make(chan error, 3)
	go func() {
		log.Info("listening", "addr", ln.Addr().String())
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	// Every return from here on must go through shutdown: the listener has
	// been accepting since before recovery started, so notifications may
	// already be in flight on detached goroutines holding the database pool.
	// drainers are every source of in-flight delivery work. Each one may be
	// mid-delivery, holding the database pool, at the moment SIGTERM arrives.
	//
	// The recovery sweep belongs here as much as the two webhooks do: it
	// dispatches through the same path, and when nothing waited for it the pool
	// was closed underneath, the claim release failed against a closed pool, and
	// the message stayed claimed forever — never delivered, never retried.
	type drainer struct {
		name  string
		drain func(context.Context) error
	}

	shutdown := func() error {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		// A failure here is reported but does not skip the drains. Returning
		// early left every acknowledged message to be dropped, which is the
		// exact defect this closure exists to prevent.
		serveErr := srv.Shutdown(shutdownCtx)
		if serveErr != nil {
			log.Error("shutdown http server", "error", serveErr)
		}

		// The server stops accepting first; only then is it meaningful to wait
		// for work already acknowledged to Graph and to Slack. Every drainer is
		// attempted regardless of how the others fared.
		//
		// A drain failure leaves detached goroutines still holding the pool.
		// Closing it now would fault them mid-delivery, so leave it open and
		// let process exit reclaim the sockets.
		for _, d := range []drainer{
			{name: "graph notifications", drain: graphWebhook.Drain},
			{name: "slack events", drain: slackWebhook.Drain},
			{name: "recovery sweep", drain: recovery.Drain},
		} {
			if err := d.drain(shutdownCtx); err != nil {
				log.Error("in-flight work did not drain before the deadline; leaving the database pool open",
					"source", d.name, "error", err)
				closeStore = false
			}
		}

		if serveErr != nil {
			return fmt.Errorf("shutdown http server: %w", serveErr)
		}
		return nil
	}

	// Only now replay what was missed. Notifications never replay on their
	// own, so skipping this loses every message sent during a restart. The
	// first sweep runs synchronously and must finish before subscriptions
	// start delivering — this is the ordering the rest of this function
	// establishes: listener bound, then replay, then subscriptions.
	if err := recovery.SweepOnce(ctx); err != nil {
		// Best effort: the recovery error is the one worth reporting, but the
		// drain still has to happen before the deferred store close.
		_ = shutdown()
		if errors.Is(err, context.Canceled) {
			// A shutdown signal arrived mid-recovery. That is a clean stop,
			// not a failure worth alerting an orchestrator about.
			log.Info("gap recovery interrupted by shutdown")
			return nil
		}
		return fmt.Errorf("gap recovery: %w", err)
	}

	// Recovery keeps sweeping on its own interval so the cursor keeps
	// advancing while the service runs, not only at the next restart.
	go func() {
		if err := recovery.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()

	// Subscriptions come last: creating one tells Graph to start delivering,
	// and by now both the listener and the first replay are done.
	go func() {
		if err := manager.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		_ = shutdown()
		return err
	case <-ctx.Done():
		log.Info("shutting down")
	}

	return shutdown()
}

// renderTeams adapts the Teams HTML converter to the sink's renderer, which
// takes a whole message because another connector's formatting does not live
// in the body.
func renderTeams(msg model.Message) string { return teamshtml.ToMrkdwn(msg.Text) }

// targetLabel names the platform a message is headed for. It exists for the
// metric label alone: the delivery itself resolves its target inside Handle,
// from the same pair.
//
// An unroutable message is labelled "unknown" rather than guessed at. The
// guess is what Platform.Opposite used to do, and with three platforms it
// would attribute a Signal failure to Teams.
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

// recordOutcome is the sole place that turns Handle's (delivered, reason,
// err) contract into a metric. It exists so that decision can be unit tested
// in isolation: a swapped counter or wrong label here would ship silently
// otherwise, which matters because a previous defect existed precisely
// because bridge_delivered_total counted drops as deliveries.
//
// Exactly one counter is incremented per call:
//   - err != nil: FailedTotal, labelled pair/target.
//   - err == nil && !delivered: DroppedTotal, labelled pair/reason.
//   - err == nil && delivered: DeliveredTotal, labelled pair/target.
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

func loadPrivateKey(path string) (*rsa.PrivateKey, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // operator-supplied key path
	if err != nil {
		return nil, fmt.Errorf("read private key: %w", err)
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, errors.New("private key is not PEM encoded")
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse private key: %w", err)
	}
	key, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("private key is not RSA")
	}
	return key, nil
}

// subscriptionMetricsAdapter satisfies teams.SubscriptionMetrics by
// forwarding to the package-level Prometheus vectors. The manager package
// takes the narrow interface instead of importing internal/metrics directly,
// so it stays testable without a registry.
type subscriptionMetricsAdapter struct{}

func (subscriptionMetricsAdapter) RecordRenewalFailure(pair string) {
	metrics.SubscriptionRenewalFailuresTotal.WithLabelValues(pair).Inc()
}

func (subscriptionMetricsAdapter) SetExpiry(pair string, expiresAt time.Time) {
	metrics.SubscriptionExpiresSeconds.WithLabelValues(pair).Set(float64(expiresAt.Unix()))
}

// env collects required configuration from the environment, reporting every
// missing variable at once rather than failing on whichever is read first.
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

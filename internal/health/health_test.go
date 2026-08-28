package health

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestReadinessHandler(t *testing.T) {
	t.Run("ready when every check passes", func(t *testing.T) {
		h := ReadinessHandler(quietLogger(),
			Check{Name: "database", Fn: func(context.Context) error { return nil }},
			Check{Name: "subscriptions", Fn: func(context.Context) error { return nil }},
		)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
	})

	t.Run("not ready names every failing check", func(t *testing.T) {
		h := ReadinessHandler(quietLogger(),
			Check{Name: "database", Fn: func(context.Context) error { return errors.New("pool closed") }},
			Check{Name: "subscriptions", Fn: func(context.Context) error { return errors.New("no live subscription for pair acme") }},
		)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", rec.Code)
		}
		body := rec.Body.String()
		for _, want := range []string{"database", "pool closed", "subscriptions", "acme"} {
			if !strings.Contains(body, want) {
				t.Fatalf("body = %q, want it to mention %q so an operator can see what is wrong", body, want)
			}
		}
	})

	t.Run("one failing check is enough to refuse traffic", func(t *testing.T) {
		h := ReadinessHandler(quietLogger(),
			Check{Name: "database", Fn: func(context.Context) error { return nil }},
			Check{Name: "subscriptions", Fn: func(context.Context) error { return errors.New("none") }},
		)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503 — a half-working pod must not be sent traffic", rec.Code)
		}
	})

	t.Run("a hanging check is bounded rather than left to hang", func(t *testing.T) {
		h := ReadinessHandler(quietLogger(),
			Check{Name: "slow", Fn: func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			}},
		)
		rec := httptest.NewRecorder()
		done := make(chan struct{})
		go func() {
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(healthCheckTimeout + 2*time.Second):
			t.Fatal("readiness never answered")
		}
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", rec.Code)
		}
	})
}

// Liveness must not depend on anything external. A database outage should stop
// traffic, not make Kubernetes restart a pod that would come back to the same
// outage.
func TestLivenessHandlerIgnoresDependencies(t *testing.T) {
	rec := httptest.NewRecorder()
	LivenessHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

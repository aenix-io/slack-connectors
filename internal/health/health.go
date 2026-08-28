// Package health serves the liveness and readiness endpoints both bridge
// binaries expose.
package health

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// healthCheckTimeout bounds one readiness probe. A probe that hangs is a probe
// Kubernetes reads as a failure anyway, so it is better to answer.
const healthCheckTimeout = 3 * time.Second

// Check is one named readiness condition.
type Check struct {
	Name string
	Fn   func(context.Context) error
}

// readinessHandler answers whether this pod can actually carry messages.
//
// It exists because the previous probe returned 200 unconditionally on both
// liveness and readiness, so a pod that had lost its database, its Graph
// subscription or its signal-cli connection stayed Ready and Live for as long
// as it ran — precisely the state in which a bridge silently stops delivering.
func ReadinessHandler(log *slog.Logger, checks ...Check) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), healthCheckTimeout)
		defer cancel()

		var failures []string
		for _, c := range checks {
			if err := c.Fn(ctx); err != nil {
				failures = append(failures, fmt.Sprintf("%s: %v", c.Name, err))
			}
		}

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if len(failures) > 0 {
			// Logged at warn rather than error: during startup this is the
			// expected state, and readiness failing is the system working.
			log.Warn("not ready", "failures", strings.Join(failures, "; "))
			w.WriteHeader(http.StatusServiceUnavailable)
			writeString(w, log, "not ready\n"+strings.Join(failures, "\n")+"\n")
			return
		}
		w.WriteHeader(http.StatusOK)
		writeString(w, log, "ready\n")
	})
}

// livenessHandler answers whether the process is still serving. It deliberately
// checks nothing else: a dependency being down is a reason to stop taking
// traffic, not a reason for Kubernetes to kill a pod that would come back to
// the same dependency.
func LivenessHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}

func writeString(w http.ResponseWriter, log *slog.Logger, s string) {
	if _, err := fmt.Fprint(w, s); err != nil {
		log.Error("write health response", "error", err)
	}
}

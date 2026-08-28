package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandlerExposesCounters(t *testing.T) {
	DeliveredTotal.WithLabelValues("acme", "slack").Inc()
	FailedTotal.WithLabelValues("acme", "slack").Inc()
	DroppedTotal.WithLabelValues("acme", "echo").Inc()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, req)

	body := rec.Body.String()
	for _, want := range []string{
		"bridge_delivered_total",
		"bridge_failed_total",
		"bridge_dropped_total",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics output missing %q", want)
		}
	}
}

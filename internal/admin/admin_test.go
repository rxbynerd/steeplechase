package admin

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/rxbynerd/steeplechase/internal/metrics"
)

func TestAdmin_Healthz(t *testing.T) {
	s := NewServer(":0", prometheus.NewRegistry())
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	s.srv.Handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("healthz status = %d, want 200", w.Code)
	}
	if !strings.Contains(w.Body.String(), "ok") {
		t.Errorf("healthz body = %q, want 'ok'", w.Body.String())
	}
}

func TestAdmin_ReadyzFlipsOnMarkReady(t *testing.T) {
	s := NewServer(":0", prometheus.NewRegistry())

	// Before MarkReady: 503.
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	s.srv.Handler.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("readyz before MarkReady = %d, want 503", w.Code)
	}

	s.MarkReady()

	// After MarkReady: 200.
	w2 := httptest.NewRecorder()
	s.srv.Handler.ServeHTTP(w2, req)
	if w2.Code != http.StatusOK {
		t.Errorf("readyz after MarkReady = %d, want 200", w2.Code)
	}

	s.MarkNotReady()
	w3 := httptest.NewRecorder()
	s.srv.Handler.ServeHTTP(w3, req)
	if w3.Code != http.StatusServiceUnavailable {
		t.Errorf("readyz after MarkNotReady = %d, want 503", w3.Code)
	}
}

func TestAdmin_MetricsExposesRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	rec := metrics.NewRecorder(reg)
	rec.SetBuildInfo("v-test")

	s := NewServer(":0", reg)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	s.srv.Handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("metrics status = %d, want 200", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "steeplechase_build_info") {
		t.Errorf("metrics body missing build_info, got: %s", body)
	}
	if !strings.Contains(body, `version="v-test"`) {
		t.Errorf("metrics body missing version label, got: %s", body)
	}
}

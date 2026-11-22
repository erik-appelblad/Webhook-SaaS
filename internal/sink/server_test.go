package sink

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSinkCapturesRequestsNewestFirstAndCapsHistory(t *testing.T) {
	srv := NewServer()
	handler := srv.Handler()

	// Send 205 requests across default and custom bucket paths.
	for i := 1; i <= 205; i++ {
		path := "/webhook"
		if i%2 == 0 {
			path = "/webhook/orders"
		}
		body := fmt.Sprintf(`{"seq":%d}`, i)
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("req %d status = %d, want 200", i, rec.Code)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/api/requests", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/requests status = %d", rec.Code)
	}

	var snapshot requestsSnapshot
	if err := json.NewDecoder(rec.Body).Decode(&snapshot); err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Requests) != maxHistory {
		t.Fatalf("captured requests = %d, want %d", len(snapshot.Requests), maxHistory)
	}
	if !strings.Contains(snapshot.Requests[0].Body, `"seq":205`) {
		t.Fatalf("expected newest request first, got body %s", snapshot.Requests[0].Body)
	}
	if snapshot.Requests[0].Bucket != "default" {
		t.Fatalf("expected default bucket, got %q", snapshot.Requests[0].Bucket)
	}
	if snapshot.Requests[1].Bucket != "orders" {
		t.Fatalf("expected orders bucket, got %q", snapshot.Requests[1].Bucket)
	}
}

func TestSinkFailFirstNRecoversAndDecrements(t *testing.T) {
	srv := NewServer()
	handler := srv.Handler()

	setConfig(t, handler, Config{
		Mode:       "fail_first_n",
		StatusCode: http.StatusServiceUnavailable,
		FailCount:  2,
		RetryAfter: "3",
	})

	for i := 1; i <= 3; i++ {
		req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(`{"ok":true}`))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if i <= 2 {
			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("attempt %d status = %d, want 503", i, rec.Code)
			}
			if rec.Header().Get("Retry-After") != "3" {
				t.Fatalf("attempt %d Retry-After = %q, want 3", i, rec.Header().Get("Retry-After"))
			}
		} else {
			if rec.Code != http.StatusOK {
				t.Fatalf("attempt %d status = %d, want 200", i, rec.Code)
			}
			if rec.Header().Get("Retry-After") != "" {
				t.Fatalf("attempt %d Retry-After should be empty on 200, got %q", i, rec.Header().Get("Retry-After"))
			}
		}
	}

	cfg := getConfig(t, handler)
	if cfg.FailCount != 0 {
		t.Fatalf("remaining FailCount = %d, want 0", cfg.FailCount)
	}
}

func TestSinkStatusAndCustomBody(t *testing.T) {
	srv := NewServer()
	handler := srv.Handler()

	setConfig(t, handler, Config{
		Mode:         "status",
		StatusCode:   http.StatusTooManyRequests,
		RetryAfter:   "12",
		ResponseBody: "slow down",
	})

	req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if rec.Header().Get("Retry-After") != "12" {
		t.Fatalf("Retry-After = %q, want 12", rec.Header().Get("Retry-After"))
	}
	if rec.Body.String() != "slow down" {
		t.Fatalf("body = %q, want %q", rec.Body.String(), "slow down")
	}
}

func TestSinkFlakyModeBounds(t *testing.T) {
	srv := NewServer()
	handler := srv.Handler()

	setConfig(t, handler, Config{
		Mode:        "flaky",
		StatusCode:  http.StatusBadGateway,
		FailureRate: 1.0,
	})
	for i := 0; i < 5; i++ {
		req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(`{}`))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadGateway {
			t.Fatalf("failure_rate 1.0 returned %d, want 502", rec.Code)
		}
	}

	setConfig(t, handler, Config{
		Mode:        "flaky",
		StatusCode:  http.StatusBadGateway,
		FailureRate: 0.0,
	})
	for i := 0; i < 5; i++ {
		req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(`{}`))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("failure_rate 0.0 returned %d, want 200", rec.Code)
		}
	}
}

func TestSinkDelayRespectsCancelledContext(t *testing.T) {
	srv := NewServer()
	handler := srv.Handler()

	setConfig(t, handler, Config{
		Mode:    "ok",
		DelayMS: 5000,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	start := time.Now()
	req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(`{}`)).WithContext(ctx)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	elapsed := time.Since(start)

	if elapsed > 500*time.Millisecond {
		t.Fatalf("handler waited %v despite cancelled context", elapsed)
	}
}

func TestSinkQueryOverridesAndValidation(t *testing.T) {
	srv := NewServer()
	handler := srv.Handler()

	req := httptest.NewRequest(http.MethodPost, "/webhook?status=429&retry_after=7", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if rec.Header().Get("Retry-After") != "7" {
		t.Fatalf("Retry-After = %q, want 7", rec.Header().Get("Retry-After"))
	}

	invalidPaths := []string{
		"/webhook?status=99",
		"/webhook?status=abc",
		"/webhook?delay_ms=-1",
		"/webhook?delay_ms=40000",
	}
	for _, path := range invalidPaths {
		badReq := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
		badRec := httptest.NewRecorder()
		handler.ServeHTTP(badRec, badReq)
		if badRec.Code != http.StatusBadRequest {
			t.Fatalf("%s returned %d, want 400", path, badRec.Code)
		}
	}
}

func TestSinkConfigValidation(t *testing.T) {
	srv := NewServer()
	handler := srv.Handler()

	invalidConfigs := []Config{
		{Mode: "unknown", StatusCode: 503},
		{Mode: "status", StatusCode: 999},
		{Mode: "fail_first_n", StatusCode: 503, FailCount: -1},
		{Mode: "flaky", StatusCode: 503, FailureRate: 1.5},
		{Mode: "ok", StatusCode: 503, DelayMS: 30001},
	}

	for _, cfg := range invalidConfigs {
		body, _ := json.Marshal(cfg)
		req := httptest.NewRequest(http.MethodPut, "/api/config", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 for %+v, got %d", cfg, rec.Code)
		}
	}
}

func TestSinkClearAndReset(t *testing.T) {
	srv := NewServer()
	handler := srv.Handler()

	setConfig(t, handler, Config{Mode: "status", StatusCode: http.StatusTeapot})

	req := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(`{"a":1}`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusTeapot {
		t.Fatalf("status = %d, want 418", rec.Code)
	}

	delReq := httptest.NewRequest(http.MethodDelete, "/api/requests", nil)
	delRec := httptest.NewRecorder()
	handler.ServeHTTP(delRec, delReq)
	if delRec.Code != http.StatusNoContent {
		t.Fatalf("DELETE /api/requests status = %d, want 204", delRec.Code)
	}

	snap := listRequests(t, handler)
	if len(snap.Requests) != 0 || snap.Config.Mode != "status" {
		t.Fatalf("expected cleared history with preserved config, got %+v", snap)
	}

	// Send one more and then POST /api/reset.
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(`{}`)))
	resetReq := httptest.NewRequest(http.MethodPost, "/api/reset", nil)
	resetRec := httptest.NewRecorder()
	handler.ServeHTTP(resetRec, resetReq)
	if resetRec.Code != http.StatusOK {
		t.Fatalf("POST /api/reset status = %d, want 200", resetRec.Code)
	}

	snap = listRequests(t, handler)
	if len(snap.Requests) != 0 || snap.Config.Mode != "ok" {
		t.Fatalf("expected empty history and ok mode after reset, got %+v", snap)
	}
}

func TestSinkHMACSignatureVerification(t *testing.T) {
	srv := NewServer()
	handler := srv.Handler()

	const secret = "super-secret"
	setConfig(t, handler, Config{
		Mode:          "ok",
		SigningSecret: secret,
	})

	payload := []byte(`{"event":"invoice.paid"}`)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	hexSig := hex.EncodeToString(mac.Sum(nil))

	cases := []struct {
		name      string
		headerKey string
		headerVal string
		wantValid *bool
	}{
		{name: "raw hex", headerKey: "X-Webhook-Signature", headerVal: hexSig, wantValid: boolPtr(true)},
		{name: "sha256 prefix", headerKey: "webhook-signature", headerVal: "sha256=" + hexSig, wantValid: boolPtr(true)},
		{name: "bad signature", headerKey: "X-Webhook-Signature", headerVal: "sha256=deadbeef", wantValid: boolPtr(false)},
		{name: "missing header", wantValid: nil},
	}

	for _, tc := range cases {
		req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(payload))
		if tc.headerKey != "" {
			req.Header.Set(tc.headerKey, tc.headerVal)
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status = %d, want 200", tc.name, rec.Code)
		}
	}

	snap := listRequests(t, handler)
	if len(snap.Requests) != len(cases) {
		t.Fatalf("requests = %d, want %d", len(snap.Requests), len(cases))
	}

	// Newest first, so reverse order of cases.
	for idx, tc := range cases {
		got := snap.Requests[len(cases)-1-idx].SignatureValid
		if tc.wantValid == nil {
			if got != nil {
				t.Fatalf("%s SignatureValid = %v, want nil", tc.name, *got)
			}
			continue
		}
		if got == nil || *got != *tc.wantValid {
			t.Fatalf("%s SignatureValid = %v, want %t", tc.name, got, *tc.wantValid)
		}
	}
}

func TestSinkUIAndHealthz(t *testing.T) {
	srv := NewServer()
	handler := srv.Handler()

	uiRec := httptest.NewRecorder()
	handler.ServeHTTP(uiRec, httptest.NewRequest(http.MethodGet, "/", nil))
	if uiRec.Code != http.StatusOK || !strings.Contains(uiRec.Header().Get("Content-Type"), "text/html") {
		t.Fatalf("unexpected UI response: code=%d ct=%q", uiRec.Code, uiRec.Header().Get("Content-Type"))
	}
	if !strings.Contains(uiRec.Body.String(), "Webhook Chaos Sink") {
		t.Fatal("UI body missing title")
	}

	hzRec := httptest.NewRecorder()
	handler.ServeHTTP(hzRec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if hzRec.Code != http.StatusOK || !strings.Contains(hzRec.Body.String(), `"ok"`) {
		t.Fatalf("unexpected /healthz response: code=%d body=%q", hzRec.Code, hzRec.Body.String())
	}
}

func setConfig(t *testing.T, handler http.Handler, cfg Config) {
	t.Helper()
	body, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPut, "/api/config", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT /api/config status = %d: %s", rec.Code, rec.Body.String())
	}
}

func getConfig(t *testing.T, handler http.Handler) Config {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/config", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/config status = %d", rec.Code)
	}
	var cfg Config
	if err := json.NewDecoder(rec.Body).Decode(&cfg); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func listRequests(t *testing.T, handler http.Handler) requestsSnapshot {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/requests", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/requests status = %d", rec.Code)
	}
	var snap requestsSnapshot
	if err := json.NewDecoder(rec.Body).Decode(&snap); err != nil {
		t.Fatal(err)
	}
	return snap
}

func boolPtr(v bool) *bool {
	return &v
}

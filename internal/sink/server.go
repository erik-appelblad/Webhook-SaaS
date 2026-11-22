package sink

import (
	"crypto/hmac"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxHistory   = 200
	maxBodyBytes = 1 << 20
	maxDelayMS   = 30000
)

//go:embed ui.html
var uiHTML string

// Config controls how the local chaos sink responds to incoming webhook requests.
type Config struct {
	Mode          string  `json:"mode"`
	StatusCode    int     `json:"status_code"`
	FailCount     int     `json:"fail_count"`
	FailureRate   float64 `json:"failure_rate"`
	DelayMS       int     `json:"delay_ms"`
	RetryAfter    string  `json:"retry_after"`
	ResponseBody  string  `json:"response_body"`
	SigningSecret string  `json:"signing_secret"`
}

// CapturedRequest records a single webhook delivery received by the sink.
type CapturedRequest struct {
	ID             string              `json:"id"`
	ReceivedAt     time.Time           `json:"received_at"`
	Bucket         string              `json:"bucket"`
	Method         string              `json:"method"`
	Path           string              `json:"path"`
	Query          string              `json:"query,omitempty"`
	Headers        map[string][]string `json:"headers"`
	Body           string              `json:"body"`
	ResponseStatus int                 `json:"response_status"`
	DelayMS        int                 `json:"delay_ms"`
	ChaosRule      string              `json:"chaos_rule"`
	SignatureValid *bool               `json:"signature_valid,omitempty"`
}

type requestsSnapshot struct {
	Config   Config            `json:"config"`
	Requests []CapturedRequest `json:"requests"`
}

type deliveryDecision struct {
	status        int
	delayMS       int
	retryAfter    string
	responseBody  string
	signingSecret string
	chaosRule     string
	applyRetryHdr bool
}

// Server provides an in-memory webhook sink with configurable chaos rules and a debug UI.
type Server struct {
	mu      sync.Mutex
	config  Config
	history []CapturedRequest
	seq     atomic.Uint64
}

// NewServer creates a new chaos sink server with default 200 OK behavior.
func NewServer() *Server {
	return &Server{
		config:  defaultConfig(),
		history: make([]CapturedRequest, 0),
	}
}

func defaultConfig() Config {
	return Config{
		Mode:       "ok",
		StatusCode: http.StatusServiceUnavailable,
	}
}

// Handler returns the HTTP handler for the chaos sink API, UI, and webhook endpoints.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", s.handleUI)
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /api/config", s.handleGetConfig)
	mux.HandleFunc("PUT /api/config", s.handleSetConfig)
	mux.HandleFunc("POST /api/config", s.handleSetConfig)
	mux.HandleFunc("GET /api/requests", s.handleListRequests)
	mux.HandleFunc("DELETE /api/requests", s.handleClearRequests)
	mux.HandleFunc("POST /api/reset", s.handleReset)
	mux.HandleFunc("POST /webhook", s.handleWebhook)
	mux.HandleFunc("POST /webhook/{bucket}", s.handleWebhook)
	return mux
}

func (s *Server) handleUI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, uiHTML)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleGetConfig(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	cfg := s.config
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, cfg)
}

func (s *Server) handleSetConfig(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	var next Config
	if err := json.NewDecoder(r.Body).Decode(&next); err != nil {
		http.Error(w, "invalid config JSON", http.StatusBadRequest)
		return
	}
	normalized, err := validateAndNormalizeConfig(next)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	s.config = normalized
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, normalized)
}

func (s *Server) handleListRequests(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	cfg := s.config
	requests := make([]CapturedRequest, len(s.history))
	copy(requests, s.history)
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, requestsSnapshot{
		Config:   cfg,
		Requests: requests,
	})
}

func (s *Server) handleClearRequests(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	s.history = s.history[:0]
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleReset(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	s.config = defaultConfig()
	s.history = s.history[:0]
	cfg := s.config
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, requestsSnapshot{
		Config:   cfg,
		Requests: []CapturedRequest{},
	})
}

func (s *Server) handleWebhook(w http.ResponseWriter, r *http.Request) {
	queryOverrides, err := parseQueryOverrides(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read webhook body", http.StatusBadRequest)
		return
	}

	decision := s.evaluateDecision(queryOverrides)

	if decision.delayMS > 0 {
		timer := time.NewTimer(time.Duration(decision.delayMS) * time.Millisecond)
		select {
		case <-timer.C:
		case <-r.Context().Done():
			timer.Stop()
			return
		}
	}

	signatureValid := verifySignatureIfPresent(decision.signingSecret, r.Header, bodyBytes)
	bucket := r.PathValue("bucket")
	if bucket == "" {
		bucket = "default"
	}

	reqID := fmt.Sprintf("req-%06d", s.seq.Add(1))
	captured := CapturedRequest{
		ID:             reqID,
		ReceivedAt:     time.Now().UTC(),
		Bucket:         bucket,
		Method:         r.Method,
		Path:           r.URL.Path,
		Query:          r.URL.RawQuery,
		Headers:        cloneHeaders(r.Header),
		Body:           string(bodyBytes),
		ResponseStatus: decision.status,
		DelayMS:        decision.delayMS,
		ChaosRule:      decision.chaosRule,
		SignatureValid: signatureValid,
	}

	s.mu.Lock()
	s.history = append([]CapturedRequest{captured}, s.history...)
	if len(s.history) > maxHistory {
		s.history = s.history[:maxHistory]
	}
	s.mu.Unlock()

	if decision.applyRetryHdr && decision.retryAfter != "" {
		w.Header().Set("Retry-After", decision.retryAfter)
	}

	if decision.responseBody != "" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(decision.status)
		_, _ = io.WriteString(w, decision.responseBody)
		return
	}

	writeJSON(w, decision.status, map[string]any{
		"received":   true,
		"id":         reqID,
		"bucket":     bucket,
		"status":     decision.status,
		"chaos_rule": decision.chaosRule,
	})
}

type queryOverrides struct {
	hasStatus     bool
	status        int
	hasDelayMS    bool
	delayMS       int
	hasRetryAfter bool
	retryAfter    string
}

func parseQueryOverrides(r *http.Request) (queryOverrides, error) {
	q := r.URL.Query()
	var overrides queryOverrides

	if rawStatus := q.Get("status"); rawStatus != "" {
		statusCode, err := strconv.Atoi(rawStatus)
		if err != nil || statusCode < 100 || statusCode > 599 {
			return queryOverrides{}, fmt.Errorf("query status must be between 100 and 599")
		}
		overrides.hasStatus = true
		overrides.status = statusCode
	}

	if rawDelay := q.Get("delay_ms"); rawDelay != "" {
		delayMS, err := strconv.Atoi(rawDelay)
		if err != nil || delayMS < 0 || delayMS > maxDelayMS {
			return queryOverrides{}, fmt.Errorf("query delay_ms must be between 0 and %d", maxDelayMS)
		}
		overrides.hasDelayMS = true
		overrides.delayMS = delayMS
	}

	if q.Has("retry_after") {
		overrides.hasRetryAfter = true
		overrides.retryAfter = q.Get("retry_after")
	}

	return overrides, nil
}

func (s *Server) evaluateDecision(overrides queryOverrides) deliveryDecision {
	s.mu.Lock()
	cfg := &s.config
	decision := deliveryDecision{
		delayMS:       cfg.DelayMS,
		retryAfter:    cfg.RetryAfter,
		responseBody:  cfg.ResponseBody,
		signingSecret: cfg.SigningSecret,
	}

	if overrides.hasDelayMS {
		decision.delayMS = overrides.delayMS
	}
	if overrides.hasRetryAfter {
		decision.retryAfter = overrides.retryAfter
	}

	if overrides.hasStatus {
		decision.status = overrides.status
		decision.chaosRule = fmt.Sprintf("query:status:%d", overrides.status)
		decision.applyRetryHdr = overrides.hasRetryAfter || !isSuccessStatus(decision.status)
		s.mu.Unlock()
		return decision
	}

	switch cfg.Mode {
	case "status":
		decision.status = cfg.StatusCode
		decision.chaosRule = fmt.Sprintf("status:%d", cfg.StatusCode)
	case "fail_first_n":
		if cfg.FailCount > 0 {
			cfg.FailCount--
			decision.status = cfg.StatusCode
			decision.chaosRule = fmt.Sprintf("fail_first_n:%d_left", cfg.FailCount)
		} else {
			decision.status = http.StatusOK
			decision.chaosRule = "fail_first_n:recovered"
		}
	case "flaky":
		if cfg.FailureRate > 0 && rand.Float64() < cfg.FailureRate {
			decision.status = cfg.StatusCode
			decision.chaosRule = fmt.Sprintf("flaky:%.2f:fail", cfg.FailureRate)
		} else {
			decision.status = http.StatusOK
			decision.chaosRule = fmt.Sprintf("flaky:%.2f:ok", cfg.FailureRate)
		}
	default:
		decision.status = http.StatusOK
		decision.chaosRule = "ok"
	}

	decision.applyRetryHdr = overrides.hasRetryAfter || !isSuccessStatus(decision.status)
	s.mu.Unlock()
	return decision
}

func isSuccessStatus(code int) bool {
	return code >= http.StatusOK && code < http.StatusMultipleChoices
}

func validateAndNormalizeConfig(cfg Config) (Config, error) {
	if cfg.Mode == "" {
		cfg.Mode = "ok"
	}
	switch cfg.Mode {
	case "ok", "status", "fail_first_n", "flaky":
	default:
		return Config{}, fmt.Errorf("invalid mode %q: must be ok, status, fail_first_n, or flaky", cfg.Mode)
	}

	if cfg.StatusCode == 0 {
		cfg.StatusCode = http.StatusServiceUnavailable
	}
	if cfg.StatusCode < 100 || cfg.StatusCode > 599 {
		return Config{}, fmt.Errorf("status_code must be between 100 and 599")
	}
	if cfg.FailCount < 0 {
		return Config{}, fmt.Errorf("fail_count must be >= 0")
	}
	if math.IsNaN(cfg.FailureRate) || cfg.FailureRate < 0 || cfg.FailureRate > 1 {
		return Config{}, fmt.Errorf("failure_rate must be between 0.0 and 1.0")
	}
	if cfg.DelayMS < 0 || cfg.DelayMS > maxDelayMS {
		return Config{}, fmt.Errorf("delay_ms must be between 0 and %d", maxDelayMS)
	}

	return cfg, nil
}

func verifySignatureIfPresent(secret string, headers http.Header, body []byte) *bool {
	if secret == "" {
		return nil
	}
	sigHeader := headers.Get("X-Webhook-Signature")
	if sigHeader == "" {
		sigHeader = headers.Get("Webhook-Signature")
	}
	if sigHeader == "" {
		return nil
	}

	sigHex := strings.TrimSpace(sigHeader)
	sigHex = strings.TrimPrefix(sigHex, "sha256=")
	sigHex = strings.TrimPrefix(sigHex, "SHA256=")

	decoded, err := hex.DecodeString(sigHex)
	if err != nil {
		valid := false
		return &valid
	}

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	valid := hmac.Equal(decoded, mac.Sum(nil))
	return &valid
}

func cloneHeaders(headers http.Header) map[string][]string {
	cloned := make(map[string][]string, len(headers))
	for k, values := range headers {
		copied := make([]string, len(values))
		copy(copied, values)
		cloned[k] = copied
	}
	return cloned
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

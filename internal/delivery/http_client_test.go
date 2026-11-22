package delivery

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"reflect"
	"sync/atomic"
	"testing"
)

func TestIsPublicAddress(t *testing.T) {
	tests := []struct {
		address string
		public  bool
	}{
		{address: "8.8.8.8", public: true},
		{address: "2606:4700:4700::1111", public: true},
		{address: "127.0.0.1", public: false},
		{address: "10.0.0.1", public: false},
		{address: "169.254.169.254", public: false},
		{address: "100.64.0.1", public: false},
		{address: "198.51.100.10", public: false},
		{address: "fd00::1", public: false},
		{address: "2001:db8::1", public: false},
	}

	for _, test := range tests {
		t.Run(test.address, func(t *testing.T) {
			address, err := netip.ParseAddr(test.address)
			if err != nil {
				t.Fatal(err)
			}
			if got := isPublicAddress(address); got != test.public {
				t.Fatalf("isPublicAddress(%s) = %t, want %t", address, got, test.public)
			}
		})
	}
}

func TestIsAllowedDevAddress(t *testing.T) {
	tests := []struct {
		address string
		allowed bool
	}{
		{address: "8.8.8.8", allowed: true},
		{address: "127.0.0.1", allowed: true},
		{address: "::1", allowed: true},
		{address: "10.0.0.5", allowed: true},
		{address: "172.17.0.2", allowed: true},
		{address: "192.168.1.5", allowed: true},
		{address: "fd00::1", allowed: true},
		{address: "169.254.169.254", allowed: false},
		{address: "fe80::1", allowed: false},
		{address: "0.0.0.0", allowed: false},
		{address: "::", allowed: false},
		{address: "224.0.0.1", allowed: false},
		{address: "100.64.0.1", allowed: false},
		{address: "198.51.100.10", allowed: false},
	}

	for _, test := range tests {
		t.Run(test.address, func(t *testing.T) {
			address, err := netip.ParseAddr(test.address)
			if err != nil {
				t.Fatal(err)
			}
			if got := isAllowedDevAddress(address); got != test.allowed {
				t.Fatalf("isAllowedDevAddress(%s) = %t, want %t", address, got, test.allowed)
			}
		})
	}
}

func TestParseAllowedPrivateHosts(t *testing.T) {
	hosts, err := ParseAllowedPrivateHosts(" Localhost:9099 , 127.0.0.1:9099, localhost.:9099 , [::1]:9099 , ")
	if err != nil {
		t.Fatalf("ParseAllowedPrivateHosts returned unexpected error: %v", err)
	}
	want := []string{"localhost:9099", "127.0.0.1:9099", "[::1]:9099"}
	if !reflect.DeepEqual(hosts, want) {
		t.Fatalf("ParseAllowedPrivateHosts = %v, want %v", hosts, want)
	}

	empty, err := ParseAllowedPrivateHosts("   ")
	if err != nil || len(empty) != 0 {
		t.Fatalf("expected empty slice and nil error, got (%v, %v)", empty, err)
	}

	invalidCases := []string{
		"localhost",
		"127.0.0.1",
		":9099",
		"localhost:0",
		"localhost:70000",
		"localhost:abc",
	}
	for _, raw := range invalidCases {
		if _, err := ParseAllowedPrivateHosts(raw); err == nil {
			t.Fatalf("expected error for invalid allowed host %q, got nil", raw)
		}
	}
}

func TestWebhookHTTPClientRejectsLoopback(t *testing.T) {
	var requestReceived atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requestReceived.Store(true)
	}))
	defer server.Close()

	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := newWebhookHTTPClient().Do(request)
	if response != nil {
		response.Body.Close()
	}
	if err == nil {
		t.Fatal("expected loopback destination to be rejected")
	}
	if requestReceived.Load() {
		t.Fatal("request reached the loopback server")
	}
}

func TestWebhookHTTPClientAllowsAllowlistedLoopback(t *testing.T) {
	var requestReceived atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requestReceived.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	parsed, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	client := NewWebhookHTTPClient([]string{parsed.Host})
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("expected allowlisted loopback request to succeed, got %v", err)
	}
	response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.StatusCode)
	}
	if !requestReceived.Load() {
		t.Fatal("expected request to reach allowlisted server")
	}
}

func TestWebhookHTTPClientRejectsOtherPortOnAllowlistedHost(t *testing.T) {
	serverA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer serverA.Close()

	var serverBHit atomic.Bool
	serverB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		serverBHit.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer serverB.Close()

	parsedA, err := url.Parse(serverA.URL)
	if err != nil {
		t.Fatal(err)
	}

	client := NewWebhookHTTPClient([]string{parsedA.Host})
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, serverB.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if response != nil {
		response.Body.Close()
	}
	if err == nil {
		t.Fatal("expected non-allowlisted port on same host to be rejected")
	}
	if serverBHit.Load() {
		t.Fatal("request reached non-allowlisted serverB")
	}
}

func TestWebhookHTTPClientDoesNotFollowRedirectsFromAllowlistedHost(t *testing.T) {
	var targetHit atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		targetHit.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer redirector.Close()

	parsedRedirector, err := url.Parse(redirector.URL)
	if err != nil {
		t.Fatal(err)
	}

	client := NewWebhookHTTPClient([]string{parsedRedirector.Host})
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, redirector.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("unexpected error on redirect response: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusFound {
		t.Fatalf("status = %d, want 302", response.StatusCode)
	}
	if targetHit.Load() {
		t.Fatal("client followed redirect to target server")
	}
}

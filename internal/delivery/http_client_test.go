package delivery

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/netip"
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

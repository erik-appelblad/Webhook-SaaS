package main

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestServeHTTPGracefulShutdownDrainsInFlightRequests(t *testing.T) {
	requestStarted := make(chan struct{})
	releaseResponse := make(chan struct{})

	mux := http.NewServeMux()
	mux.HandleFunc("GET /slow", func(w http.ResponseWriter, _ *http.Request) {
		close(requestStarted)
		<-releaseResponse
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("done"))
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serveDone := make(chan error, 1)
	go func() {
		serveDone <- serveHTTP(ctx, server, listener, 2*time.Second)
	}()

	respCh := make(chan *http.Response, 1)
	errCh := make(chan error, 1)
	go func() {
		resp, reqErr := http.Get("http://" + listener.Addr().String() + "/slow")
		if reqErr != nil {
			errCh <- reqErr
			return
		}
		respCh <- resp
	}()

	<-requestStarted
	cancel()

	// Keep the request in flight briefly after shutdown begins to ensure Shutdown drains it.
	time.Sleep(100 * time.Millisecond)
	close(releaseResponse)

	select {
	case reqErr := <-errCh:
		t.Fatalf("in-flight HTTP request failed during shutdown: %v", reqErr)
	case resp := <-respCh:
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for in-flight HTTP response")
	}

	select {
	case serveErr := <-serveDone:
		if serveErr != nil {
			t.Fatalf("serveHTTP returned error: %v", serveErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for serveHTTP to return")
	}
}

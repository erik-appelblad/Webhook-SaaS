package delivery

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"erikappelblad.dev/saas/internal/store"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type fakeDeliveryStore struct {
	target          store.DeliveryTarget
	targetErr       error
	startedCalls    []string
	failedAttempts  []string
	deliveredCalls  []string
	deliveryFailed  []string
	failMessage     string
	markStartedErr  error
	markFailedErr   error
	markDeliveredEr error
}

func (f *fakeDeliveryStore) GetDeliveryTarget(_ context.Context, _ string) (store.DeliveryTarget, error) {
	if f.targetErr != nil {
		return store.DeliveryTarget{}, f.targetErr
	}
	return f.target, nil
}

func (f *fakeDeliveryStore) MarkAttemptStarted(_ context.Context, id string) error {
	f.startedCalls = append(f.startedCalls, id)
	return f.markStartedErr
}

func (f *fakeDeliveryStore) MarkAttemptFailed(_ context.Context, id, message string) error {
	f.failedAttempts = append(f.failedAttempts, id)
	f.failMessage = message
	return f.markFailedErr
}

func (f *fakeDeliveryStore) MarkDelivered(_ context.Context, id string) error {
	f.deliveredCalls = append(f.deliveredCalls, id)
	return f.markDeliveredEr
}

func (f *fakeDeliveryStore) MarkDeliveryFailed(_ context.Context, id, message string) error {
	f.deliveryFailed = append(f.deliveryFailed, id)
	f.failMessage = message
	return nil
}

func TestDeliverWorkflowSucceedsFirstAttempt(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(DeliverWorkflow, workflow.RegisterOptions{Name: DeliverWorkflowName})

	var attempts atomic.Int32
	var markFailedCalled atomic.Bool

	env.RegisterActivityWithOptions(func(_ context.Context, id string) error {
		if id != "del-1" {
			t.Fatalf("deliveryID = %q, want del-1", id)
		}
		attempts.Add(1)
		return nil
	}, activity.RegisterOptions{Name: deliverActivityName})

	env.RegisterActivityWithOptions(func(_ context.Context, _, _ string) error {
		markFailedCalled.Store(true)
		return nil
	}, activity.RegisterOptions{Name: failActivityName})

	env.ExecuteWorkflow(DeliverWorkflow, "del-1")
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("unexpected workflow error: %v", err)
	}
	if attempts.Load() != 1 {
		t.Fatalf("attempts = %d, want 1", attempts.Load())
	}
	if markFailedCalled.Load() {
		t.Fatal("MarkWebhookDeliveryFailed should not be called on success")
	}
}

func TestDeliverWorkflowRetriesThenSucceeds(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(DeliverWorkflow, workflow.RegisterOptions{Name: DeliverWorkflowName})

	var attempts atomic.Int32
	var markFailedCalled atomic.Bool

	env.RegisterActivityWithOptions(func(_ context.Context, _ string) error {
		n := attempts.Add(1)
		if n < 3 {
			return errors.New("temporary 503")
		}
		return nil
	}, activity.RegisterOptions{Name: deliverActivityName})

	env.RegisterActivityWithOptions(func(_ context.Context, _, _ string) error {
		markFailedCalled.Store(true)
		return nil
	}, activity.RegisterOptions{Name: failActivityName})

	env.ExecuteWorkflow(DeliverWorkflow, "del-2")
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("unexpected workflow error: %v", err)
	}
	if attempts.Load() != 3 {
		t.Fatalf("attempts = %d, want 3", attempts.Load())
	}
	if markFailedCalled.Load() {
		t.Fatal("MarkWebhookDeliveryFailed should not be called when retry succeeds")
	}
}

func TestDeliverWorkflowExhaustsRetriesAndMarksFailed(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(DeliverWorkflow, workflow.RegisterOptions{Name: DeliverWorkflowName})

	var attempts atomic.Int32
	var failCalls atomic.Int32
	var failedID, failedMsg string

	env.RegisterActivityWithOptions(func(_ context.Context, _ string) error {
		attempts.Add(1)
		return errors.New("webhook returned HTTP 503")
	}, activity.RegisterOptions{Name: deliverActivityName})

	env.RegisterActivityWithOptions(func(_ context.Context, id, msg string) error {
		failCalls.Add(1)
		failedID = id
		failedMsg = msg
		return nil
	}, activity.RegisterOptions{Name: failActivityName})

	env.ExecuteWorkflow(DeliverWorkflow, "del-3")
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err == nil {
		t.Fatal("expected workflow error after exhausting retries")
	}
	if attempts.Load() != 5 {
		t.Fatalf("attempts = %d, want 5", attempts.Load())
	}
	if failCalls.Load() != 1 {
		t.Fatalf("failCalls = %d, want 1", failCalls.Load())
	}
	if failedID != "del-3" || !strings.Contains(failedMsg, "503") {
		t.Fatalf("unexpected fail recording: id=%q msg=%q", failedID, failedMsg)
	}
}

func TestDeliverWorkflowMarkFailedErrorPropagates(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(DeliverWorkflow, workflow.RegisterOptions{Name: DeliverWorkflowName})

	env.RegisterActivityWithOptions(func(_ context.Context, _ string) error {
		return errors.New("send failed")
	}, activity.RegisterOptions{Name: deliverActivityName})

	env.RegisterActivityWithOptions(func(_ context.Context, _, _ string) error {
		return errors.New("postgres unavailable")
	}, activity.RegisterOptions{Name: failActivityName})

	env.ExecuteWorkflow(DeliverWorkflow, "del-4")
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("expected workflow error, got nil")
	}
	if !strings.Contains(err.Error(), "recording its failure also failed") {
		t.Fatalf("expected combined error message, got %v", err)
	}
}

func TestDeliverWebhookAttemptSuccess(t *testing.T) {
	var gotMethod, gotContentType, gotUserAgent, gotBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotContentType = r.Header.Get("Content-Type")
		gotUserAgent = r.Header.Get("User-Agent")
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	fakeStore := &fakeDeliveryStore{
		target: store.DeliveryTarget{
			Delivery: store.Delivery{ID: "del-ok", Payload: `{"type":"invoice.paid"}`},
			URL:      server.URL,
		},
	}
	activities := NewActivities(fakeStore, server.Client())

	if err := activities.DeliverWebhookAttempt(context.Background(), "del-ok"); err != nil {
		t.Fatalf("DeliverWebhookAttempt failed: %v", err)
	}

	if gotMethod != http.MethodPost || gotContentType != "application/json" || gotUserAgent != "saas-webhook/0.1" {
		t.Fatalf("unexpected request metadata: method=%q content-type=%q ua=%q", gotMethod, gotContentType, gotUserAgent)
	}
	if gotBody != `{"type":"invoice.paid"}` {
		t.Fatalf("body = %q, want %q", gotBody, `{"type":"invoice.paid"}`)
	}
	if len(fakeStore.startedCalls) != 1 || fakeStore.startedCalls[0] != "del-ok" {
		t.Fatalf("unexpected startedCalls: %v", fakeStore.startedCalls)
	}
	if len(fakeStore.deliveredCalls) != 1 || fakeStore.deliveredCalls[0] != "del-ok" {
		t.Fatalf("unexpected deliveredCalls: %v", fakeStore.deliveredCalls)
	}
	if len(fakeStore.failedAttempts) != 0 {
		t.Fatalf("unexpected failedAttempts: %v", fakeStore.failedAttempts)
	}
}

func TestDeliverWebhookAttemptNon2xx(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	fakeStore := &fakeDeliveryStore{
		target: store.DeliveryTarget{
			Delivery: store.Delivery{ID: "del-503", Payload: `{"ok":false}`},
			URL:      server.URL,
		},
	}
	activities := NewActivities(fakeStore, server.Client())

	err := activities.DeliverWebhookAttempt(context.Background(), "del-503")
	if err == nil || !strings.Contains(err.Error(), "HTTP 503") {
		t.Fatalf("expected HTTP 503 error, got %v", err)
	}
	if len(fakeStore.startedCalls) != 1 || len(fakeStore.failedAttempts) != 1 {
		t.Fatalf("expected 1 started and 1 failed attempt, got started=%v failed=%v", fakeStore.startedCalls, fakeStore.failedAttempts)
	}
	if !strings.Contains(fakeStore.failMessage, "HTTP 503") {
		t.Fatalf("expected recorded failMessage to contain HTTP 503, got %q", fakeStore.failMessage)
	}
	if len(fakeStore.deliveredCalls) != 0 {
		t.Fatalf("deliveredCalls should be empty, got %v", fakeStore.deliveredCalls)
	}
}

func TestDeliverWebhookAttemptNetworkError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	serverURL := server.URL
	client := server.Client()
	server.Close()

	fakeStore := &fakeDeliveryStore{
		target: store.DeliveryTarget{
			Delivery: store.Delivery{ID: "del-net", Payload: `{}`},
			URL:      serverURL,
		},
	}
	activities := NewActivities(fakeStore, client)

	err := activities.DeliverWebhookAttempt(context.Background(), "del-net")
	if err == nil || !strings.Contains(err.Error(), "send webhook") {
		t.Fatalf("expected send webhook error, got %v", err)
	}
	if len(fakeStore.startedCalls) != 1 || len(fakeStore.failedAttempts) != 1 {
		t.Fatalf("expected attempt started & failed to be recorded, got started=%v failed=%v", fakeStore.startedCalls, fakeStore.failedAttempts)
	}
}

func TestDeliverWebhookAttemptTargetLoadError(t *testing.T) {
	fakeStore := &fakeDeliveryStore{
		targetErr: errors.New("not found"),
	}
	activities := NewActivities(fakeStore, nil)

	err := activities.DeliverWebhookAttempt(context.Background(), "missing")
	if err == nil || !strings.Contains(err.Error(), "load delivery target") {
		t.Fatalf("expected load delivery target error, got %v", err)
	}
	if len(fakeStore.startedCalls) != 0 {
		t.Fatalf("MarkAttemptStarted should not be called when target fails to load")
	}
}

func TestMarkWebhookDeliveryFailed(t *testing.T) {
	fakeStore := &fakeDeliveryStore{}
	activities := NewActivities(fakeStore, nil)

	if err := activities.MarkWebhookDeliveryFailed(context.Background(), "del-x", "gave up"); err != nil {
		t.Fatalf("MarkWebhookDeliveryFailed failed: %v", err)
	}
	if len(fakeStore.deliveryFailed) != 1 || fakeStore.deliveryFailed[0] != "del-x" || fakeStore.failMessage != "gave up" {
		t.Fatalf("unexpected deliveryFailed state: ids=%v msg=%q", fakeStore.deliveryFailed, fakeStore.failMessage)
	}
}

package delivery

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"erikappelblad.dev/saas/internal/store"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	DeliverWorkflowName = "DeliverWebhook"
	deliverActivityName = "DeliverWebhookAttempt"
	failActivityName    = "MarkWebhookDeliveryFailed"
)

type DeliveryStore interface {
	GetDeliveryTarget(ctx context.Context, id string) (store.DeliveryTarget, error)
	MarkAttemptStarted(ctx context.Context, id string) error
	MarkAttemptFailed(ctx context.Context, id, message string) error
	MarkDelivered(ctx context.Context, id string) error
	MarkDeliveryFailed(ctx context.Context, id, message string) error
}

type Activities struct {
	store  DeliveryStore
	client *http.Client
}

func NewActivities(database DeliveryStore, httpClient *http.Client) *Activities {
	if httpClient == nil {
		httpClient = newWebhookHTTPClient()
	}
	return &Activities{
		store:  database,
		client: httpClient,
	}
}

func DeliverWorkflow(ctx workflow.Context, deliveryID string) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    2 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    1 * time.Minute,
			MaximumAttempts:    5,
		},
	})

	err := workflow.ExecuteActivity(ctx, deliverActivityName, deliveryID).Get(ctx, nil)
	if err == nil {
		return nil
	}
	if markErr := workflow.ExecuteActivity(ctx, failActivityName, deliveryID, err.Error()).Get(ctx, nil); markErr != nil {
		return fmt.Errorf("delivery failed (%v), and recording its failure also failed: %w", err, markErr)
	}
	return err
}

func (a *Activities) DeliverWebhookAttempt(ctx context.Context, deliveryID string) error {
	target, err := a.store.GetDeliveryTarget(ctx, deliveryID)
	if err != nil {
		return fmt.Errorf("load delivery target: %w", err)
	}
	if err := a.store.MarkAttemptStarted(ctx, deliveryID); err != nil {
		return err
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target.URL, bytes.NewBufferString(target.Payload))
	if err != nil {
		return fmt.Errorf("create webhook request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "saas-webhook/0.1")

	response, err := a.client.Do(request)
	if err != nil {
		if recordErr := a.store.MarkAttemptFailed(ctx, deliveryID, err.Error()); recordErr != nil {
			return fmt.Errorf("webhook request failed (%v), and recording the failure failed: %w", err, recordErr)
		}
		return fmt.Errorf("send webhook: %w", err)
	}
	_, readErr := io.Copy(io.Discard, io.LimitReader(response.Body, 64*1024))
	closeErr := response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		err = fmt.Errorf("webhook returned HTTP %d", response.StatusCode)
	} else if readErr != nil {
		err = fmt.Errorf("read webhook response: %w", readErr)
	} else if closeErr != nil {
		err = fmt.Errorf("close webhook response: %w", closeErr)
	} else {
		return a.store.MarkDelivered(ctx, deliveryID)
	}

	if recordErr := a.store.MarkAttemptFailed(ctx, deliveryID, err.Error()); recordErr != nil {
		return fmt.Errorf("webhook request failed (%v), and recording the failure failed: %w", err, recordErr)
	}
	return err
}

func (a *Activities) MarkWebhookDeliveryFailed(ctx context.Context, deliveryID, message string) error {
	return a.store.MarkDeliveryFailed(ctx, deliveryID, message)
}

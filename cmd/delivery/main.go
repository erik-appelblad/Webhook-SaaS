package main

import (
	"context"
	"errors"
	"log"
	"net"
	"os"

	"erikappelblad.dev/saas/internal/delivery"
	deliveryv1 "erikappelblad.dev/saas/internal/gen/delivery/v1"
	"erikappelblad.dev/saas/internal/store"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	databaseURL := envOr("DATABASE_URL", "postgres://saas:saas@localhost:5433/saas?sslmode=disable")
	temporalAddress := envOr("TEMPORAL_ADDRESS", "localhost:7233")
	grpcAddress := envOr("DELIVERY_GRPC_ADDRESS", "localhost:9097")

	ctx := context.Background()
	database, err := store.Open(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer database.Close()
	if err := database.Migrate(ctx); err != nil {
		return err
	}

	temporalClient, err := client.Dial(client.Options{HostPort: temporalAddress})
	if err != nil {
		return err
	}
	defer temporalClient.Close()

	taskWorker := worker.New(temporalClient, delivery.TaskQueue, worker.Options{})
	taskWorker.RegisterWorkflowWithOptions(delivery.DeliverWorkflow, workflow.RegisterOptions{
		Name: delivery.DeliverWorkflowName,
	})
	activities := delivery.NewActivities(database)
	taskWorker.RegisterActivityWithOptions(activities.DeliverWebhookAttempt, activity.RegisterOptions{
		Name: "DeliverWebhookAttempt",
	})
	taskWorker.RegisterActivityWithOptions(activities.MarkWebhookDeliveryFailed, activity.RegisterOptions{
		Name: "MarkWebhookDeliveryFailed",
	})

	listener, err := net.Listen("tcp", grpcAddress)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	deliveryv1.RegisterDeliveryServiceServer(grpcServer, delivery.NewServer(database, temporalClient))
	serveErrors := make(chan error, 1)
	go func() {
		serveErrors <- grpcServer.Serve(listener)
	}()

	workerErrors := make(chan error, 1)
	go func() {
		workerErrors <- taskWorker.Run(worker.InterruptCh())
	}()

	select {
	case err := <-serveErrors:
		taskWorker.Stop()
		if !errors.Is(err, net.ErrClosed) {
			grpcServer.Stop()
			<-workerErrors
			return err
		}
	case err := <-workerErrors:
		grpcServer.Stop()
		return err
	}
	return nil
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

package main

import (
	"context"
	"errors"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"erikappelblad.dev/saas/internal/delivery"
	deliveryv1 "erikappelblad.dev/saas/internal/gen/delivery/v1"
	"erikappelblad.dev/saas/internal/store"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/grpc"
)

const shutdownTimeout = 10 * time.Second

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	databaseURL := envOr("DATABASE_URL", "postgres://saas:saas@localhost:5433/saas?sslmode=disable")
	temporalAddress := envOr("TEMPORAL_ADDRESS", "localhost:7233")
	grpcAddress := envOr("DELIVERY_GRPC_ADDRESS", "localhost:9097")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	allowedHosts, err := delivery.ParseAllowedPrivateHosts(os.Getenv("DEV_ALLOWED_WEBHOOK_HOSTS"))
	if err != nil {
		return err
	}
	if len(allowedHosts) > 0 {
		log.Printf("WARNING: DEV_ALLOWED_WEBHOOK_HOSTS enabled for %v; for local development only", allowedHosts)
	}

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
	httpClient := delivery.NewWebhookHTTPClient(allowedHosts)
	activities := delivery.NewActivities(database, httpClient)
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

	workerStop := make(chan interface{})
	var stopWorkerOnce sync.Once
	stopWorker := func() {
		stopWorkerOnce.Do(func() {
			close(workerStop)
		})
	}

	workerErrors := make(chan error, 1)
	go func() {
		workerErrors <- taskWorker.Run(workerStop)
	}()

	log.Printf("Delivery gRPC listening at %s (task queue: %s)", grpcAddress, delivery.TaskQueue)

	select {
	case <-ctx.Done():
		log.Println("Shutting down delivery service...")
		stopWorker()
		gracefulStopGRPC(grpcServer, shutdownTimeout)
		workerErr := <-workerErrors
		serveErr := <-serveErrors
		if workerErr != nil {
			return workerErr
		}
		if serveErr != nil && !errors.Is(serveErr, net.ErrClosed) {
			return serveErr
		}
		return nil
	case serveErr := <-serveErrors:
		stopWorker()
		workerErr := <-workerErrors
		if serveErr != nil && !errors.Is(serveErr, net.ErrClosed) {
			return serveErr
		}
		return workerErr
	case workerErr := <-workerErrors:
		gracefulStopGRPC(grpcServer, shutdownTimeout)
		serveErr := <-serveErrors
		if workerErr != nil {
			return workerErr
		}
		if serveErr != nil && !errors.Is(serveErr, net.ErrClosed) {
			return serveErr
		}
		return nil
	}
}

func gracefulStopGRPC(server *grpc.Server, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(done)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-done:
	case <-timer.C:
		server.Stop()
		<-done
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

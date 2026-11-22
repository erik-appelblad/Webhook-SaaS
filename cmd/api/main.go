package main

import (
	"log"
	"net/http"
	"os"

	deliveryv1 "erikappelblad.dev/saas/internal/gen/delivery/v1"
	"erikappelblad.dev/saas/internal/graph"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	grpcAddress := envOr("DELIVERY_GRPC_ADDRESS", "localhost:9097")
	httpAddress := envOr("GRAPHQL_ADDRESS", "localhost:8081")

	connection, err := grpc.NewClient(grpcAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("connect to delivery service: %v", err)
	}
	defer connection.Close()

	client := deliveryv1.NewDeliveryServiceClient(connection)
	handler, err := graph.NewHandler(client)
	if err != nil {
		log.Fatalf("create GraphQL handler: %v", err)
	}
	http.Handle("/query", handler)

	log.Printf("GraphQL endpoint listening at http://%s/query", httpAddress)
	if err := http.ListenAndServe(httpAddress, nil); err != nil {
		log.Fatal(err)
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"time"

	deliveryv1 "erikappelblad.dev/saas/internal/gen/delivery/v1"
	"erikappelblad.dev/saas/internal/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	temporalclient "go.temporal.io/sdk/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const TaskQueue = "webhook-delivery"

type Server struct {
	deliveryv1.UnimplementedDeliveryServiceServer
	store    *store.Store
	temporal temporalclient.Client
}

func NewServer(database *store.Store, temporal temporalclient.Client) *Server {
	return &Server{store: database, temporal: temporal}
}

func (s *Server) CreateEndpoint(ctx context.Context, request *deliveryv1.CreateEndpointRequest) (*deliveryv1.Endpoint, error) {
	name := strings.TrimSpace(request.GetName())
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "endpoint name is required")
	}
	parsed, err := url.ParseRequestURI(request.GetUrl())
	if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.Hostname() == "" || parsed.User != nil {
		return nil, status.Error(codes.InvalidArgument, "endpoint URL must be an absolute HTTP or HTTPS URL without user info")
	}

	endpoint, err := s.store.CreateEndpoint(ctx, name, request.GetUrl())
	if err != nil {
		return nil, status.Error(codes.Internal, "create endpoint")
	}
	return endpointMessage(endpoint), nil
}

func (s *Server) DispatchEvent(ctx context.Context, request *deliveryv1.DispatchEventRequest) (*deliveryv1.Delivery, error) {
	if _, err := uuid.Parse(request.GetEndpointId()); err != nil {
		return nil, status.Error(codes.InvalidArgument, "endpoint ID must be a UUID")
	}
	if !json.Valid([]byte(request.GetPayload())) {
		return nil, status.Error(codes.InvalidArgument, "payload must be valid JSON")
	}
	delivery, err := s.store.CreateDelivery(ctx, request.GetEndpointId(), request.GetPayload())
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, status.Error(codes.NotFound, "endpoint not found")
	}
	if err != nil {
		return nil, status.Error(codes.Internal, "create delivery")
	}

	_, err = s.temporal.ExecuteWorkflow(ctx, temporalclient.StartWorkflowOptions{
		ID:        "webhook-delivery-" + delivery.ID,
		TaskQueue: TaskQueue,
	}, DeliverWorkflowName, delivery.ID)
	if err != nil {
		if markErr := s.store.MarkDeliveryFailed(ctx, delivery.ID, "could not start delivery workflow"); markErr != nil {
			return nil, status.Error(codes.Internal, "start delivery workflow and record failure")
		}
		return nil, status.Error(codes.Unavailable, "could not start delivery workflow")
	}
	return deliveryMessage(delivery), nil
}

func (s *Server) ListEndpoints(ctx context.Context, _ *deliveryv1.ListEndpointsRequest) (*deliveryv1.ListEndpointsResponse, error) {
	endpoints, err := s.store.ListEndpoints(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, "list endpoints")
	}
	response := &deliveryv1.ListEndpointsResponse{Endpoints: make([]*deliveryv1.Endpoint, 0, len(endpoints))}
	for _, endpoint := range endpoints {
		response.Endpoints = append(response.Endpoints, endpointMessage(endpoint))
	}
	return response, nil
}

func (s *Server) ListDeliveries(ctx context.Context, request *deliveryv1.ListDeliveriesRequest) (*deliveryv1.ListDeliveriesResponse, error) {
	if request.GetEndpointId() != "" {
		if _, err := uuid.Parse(request.GetEndpointId()); err != nil {
			return nil, status.Error(codes.InvalidArgument, "endpoint ID must be a UUID")
		}
	}
	deliveries, err := s.store.ListDeliveries(ctx, request.GetEndpointId(), request.GetLimit())
	if err != nil {
		return nil, status.Error(codes.Internal, "list deliveries")
	}
	response := &deliveryv1.ListDeliveriesResponse{Deliveries: make([]*deliveryv1.Delivery, 0, len(deliveries))}
	for _, item := range deliveries {
		response.Deliveries = append(response.Deliveries, deliveryMessage(item))
	}
	return response, nil
}

func endpointMessage(endpoint store.Endpoint) *deliveryv1.Endpoint {
	return &deliveryv1.Endpoint{
		Id:        endpoint.ID,
		Name:      endpoint.Name,
		Url:       endpoint.URL,
		CreatedAt: endpoint.CreatedAt.Format(time.RFC3339Nano),
	}
}

func deliveryMessage(delivery store.Delivery) *deliveryv1.Delivery {
	message := &deliveryv1.Delivery{
		Id:         delivery.ID,
		EndpointId: delivery.EndpointID,
		Payload:    delivery.Payload,
		Status:     delivery.Status,
		Attempts:   delivery.Attempts,
		CreatedAt:  delivery.CreatedAt.Format(time.RFC3339Nano),
		UpdatedAt:  delivery.UpdatedAt.Format(time.RFC3339Nano),
	}
	if delivery.LastError != nil {
		message.LastError = *delivery.LastError
	}
	return message
}

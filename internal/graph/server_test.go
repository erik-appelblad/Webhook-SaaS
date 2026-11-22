package graph

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	deliveryv1 "erikappelblad.dev/saas/internal/gen/delivery/v1"
	"google.golang.org/grpc"
)

type fakeDeliveryClient struct {
	dispatched *deliveryv1.DispatchEventRequest
}

func (f *fakeDeliveryClient) CreateEndpoint(_ context.Context, request *deliveryv1.CreateEndpointRequest, _ ...grpc.CallOption) (*deliveryv1.Endpoint, error) {
	return &deliveryv1.Endpoint{Id: "endpoint-1", Name: request.GetName(), Url: request.GetUrl()}, nil
}

func (f *fakeDeliveryClient) DispatchEvent(_ context.Context, request *deliveryv1.DispatchEventRequest, _ ...grpc.CallOption) (*deliveryv1.Delivery, error) {
	f.dispatched = request
	return &deliveryv1.Delivery{
		Id:         "delivery-1",
		EndpointId: request.GetEndpointId(),
		Payload:    request.GetPayload(),
		Status:     "pending",
		Attempts:   1,
	}, nil
}

func (f *fakeDeliveryClient) ListEndpoints(context.Context, *deliveryv1.ListEndpointsRequest, ...grpc.CallOption) (*deliveryv1.ListEndpointsResponse, error) {
	return &deliveryv1.ListEndpointsResponse{
		Endpoints: []*deliveryv1.Endpoint{{Id: "endpoint-1", Name: "alerts", Url: "https://example.com"}},
	}, nil
}

func (f *fakeDeliveryClient) ListDeliveries(context.Context, *deliveryv1.ListDeliveriesRequest, ...grpc.CallOption) (*deliveryv1.ListDeliveriesResponse, error) {
	return &deliveryv1.ListDeliveriesResponse{}, nil
}

func TestGraphQLDispatchEventCallsDeliveryService(t *testing.T) {
	client := &fakeDeliveryClient{}
	handler, err := NewHandler(client)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(`{
		"query":"mutation Dispatch($endpointId: ID!, $payload: String!) { dispatchEvent(endpointId: $endpointId, payload: $payload) { id status attempts } }",
		"variables":{"endpointId":"endpoint-1","payload":"{\"type\":\"invoice.paid\"}"}
	}`))
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("HTTP status = %d, want %d", response.Code, http.StatusOK)
	}
	if client.dispatched == nil {
		t.Fatal("delivery service was not called")
	}
	if client.dispatched.GetPayload() != `{"type":"invoice.paid"}` {
		t.Fatalf("payload = %q", client.dispatched.GetPayload())
	}
	var result struct {
		Data struct {
			DispatchEvent struct {
				ID       string `json:"id"`
				Status   string `json:"status"`
				Attempts int    `json:"attempts"`
			} `json:"dispatchEvent"`
		} `json:"data"`
		Errors []json.RawMessage `json:"errors"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if len(result.Errors) != 0 {
		t.Fatalf("GraphQL errors: %s", result.Errors)
	}
	if result.Data.DispatchEvent.ID != "delivery-1" ||
		result.Data.DispatchEvent.Status != "pending" ||
		result.Data.DispatchEvent.Attempts != 1 {
		t.Fatalf("unexpected GraphQL response: %+v", result.Data.DispatchEvent)
	}
}

func TestGraphQLRequiresPOST(t *testing.T) {
	handler, err := NewHandler(&fakeDeliveryClient{})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/query", nil)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("HTTP status = %d, want %d", response.Code, http.StatusMethodNotAllowed)
	}
}

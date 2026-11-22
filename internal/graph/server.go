package graph

import (
	"encoding/json"
	"fmt"
	"net/http"

	deliveryv1 "erikappelblad.dev/saas/internal/gen/delivery/v1"
	"github.com/graphql-go/graphql"
)

type graphRequest struct {
	Query         string                 `json:"query"`
	OperationName string                 `json:"operationName"`
	Variables     map[string]interface{} `json:"variables"`
}

func NewHandler(client deliveryv1.DeliveryServiceClient) (http.Handler, error) {
	endpointType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Endpoint",
		Fields: graphql.Fields{
			"id":        &graphql.Field{Type: graphql.NewNonNull(graphql.ID)},
			"name":      &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
			"url":       &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
			"createdAt": &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
		},
	})
	deliveryType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Delivery",
		Fields: graphql.Fields{
			"id":         &graphql.Field{Type: graphql.NewNonNull(graphql.ID)},
			"endpointId": &graphql.Field{Type: graphql.NewNonNull(graphql.ID)},
			"payload":    &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
			"status":     &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
			"attempts":   &graphql.Field{Type: graphql.NewNonNull(graphql.Int)},
			"lastError":  &graphql.Field{Type: graphql.String},
			"createdAt":  &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
			"updatedAt":  &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
		},
	})

	queryType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Query",
		Fields: graphql.Fields{
			"endpoints": &graphql.Field{
				Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(endpointType))),
				Resolve: func(params graphql.ResolveParams) (interface{}, error) {
					response, err := client.ListEndpoints(params.Context, &deliveryv1.ListEndpointsRequest{})
					if err != nil {
						return nil, err
					}
					items := make([]map[string]interface{}, 0, len(response.GetEndpoints()))
					for _, endpoint := range response.GetEndpoints() {
						items = append(items, endpointObject(endpoint))
					}
					return items, nil
				},
			},
			"deliveries": &graphql.Field{
				Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(deliveryType))),
				Args: graphql.FieldConfigArgument{
					"endpointId": &graphql.ArgumentConfig{Type: graphql.ID},
				},
				Resolve: func(params graphql.ResolveParams) (interface{}, error) {
					response, err := client.ListDeliveries(params.Context, &deliveryv1.ListDeliveriesRequest{
						EndpointId: optionalString(params.Args, "endpointId"),
						Limit:      50,
					})
					if err != nil {
						return nil, err
					}
					items := make([]map[string]interface{}, 0, len(response.GetDeliveries()))
					for _, delivery := range response.GetDeliveries() {
						items = append(items, deliveryObject(delivery))
					}
					return items, nil
				},
			},
		},
	})

	mutationType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Mutation",
		Fields: graphql.Fields{
			"createEndpoint": &graphql.Field{
				Type: graphql.NewNonNull(endpointType),
				Args: graphql.FieldConfigArgument{
					"name": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)},
					"url":  &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)},
				},
				Resolve: func(params graphql.ResolveParams) (interface{}, error) {
					response, err := client.CreateEndpoint(params.Context, &deliveryv1.CreateEndpointRequest{
						Name: params.Args["name"].(string),
						Url:  params.Args["url"].(string),
					})
					if err != nil {
						return nil, err
					}
					return endpointObject(response), nil
				},
			},
			"dispatchEvent": &graphql.Field{
				Type: graphql.NewNonNull(deliveryType),
				Args: graphql.FieldConfigArgument{
					"endpointId": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.ID)},
					"payload":    &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)},
				},
				Resolve: func(params graphql.ResolveParams) (interface{}, error) {
					response, err := client.DispatchEvent(params.Context, &deliveryv1.DispatchEventRequest{
						EndpointId: fmt.Sprint(params.Args["endpointId"]),
						Payload:    params.Args["payload"].(string),
					})
					if err != nil {
						return nil, err
					}
					return deliveryObject(response), nil
				},
			},
		},
	})

	schema, err := graphql.NewSchema(graphql.SchemaConfig{Query: queryType, Mutation: mutationType})
	if err != nil {
		return nil, fmt.Errorf("create GraphQL schema: %w", err)
	}
	return http.HandlerFunc(serveGraphQL(schema)), nil
}

func serveGraphQL(schema graphql.Schema) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			http.Error(writer, "GraphQL requests must use POST", http.StatusMethodNotAllowed)
			return
		}

		request.Body = http.MaxBytesReader(writer, request.Body, 1<<20)
		var input graphRequest
		if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
			http.Error(writer, "invalid GraphQL request body", http.StatusBadRequest)
			return
		}
		if input.Query == "" {
			http.Error(writer, "GraphQL query is required", http.StatusBadRequest)
			return
		}

		result := graphql.Do(graphql.Params{
			Schema:         schema,
			RequestString:  input.Query,
			OperationName:  input.OperationName,
			VariableValues: input.Variables,
			Context:        request.Context(),
		})
		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(result); err != nil {
			return
		}
	}
}

func endpointObject(endpoint *deliveryv1.Endpoint) map[string]interface{} {
	return map[string]interface{}{
		"id":        endpoint.GetId(),
		"name":      endpoint.GetName(),
		"url":       endpoint.GetUrl(),
		"createdAt": endpoint.GetCreatedAt(),
	}
}

func deliveryObject(delivery *deliveryv1.Delivery) map[string]interface{} {
	var lastError interface{}
	if delivery.GetLastError() != "" {
		lastError = delivery.GetLastError()
	}
	return map[string]interface{}{
		"id":         delivery.GetId(),
		"endpointId": delivery.GetEndpointId(),
		"payload":    delivery.GetPayload(),
		"status":     delivery.GetStatus(),
		"attempts":   delivery.GetAttempts(),
		"lastError":  lastError,
		"createdAt":  delivery.GetCreatedAt(),
		"updatedAt":  delivery.GetUpdatedAt(),
	}
}

func optionalString(arguments map[string]interface{}, key string) string {
	if value, ok := arguments[key].(string); ok {
		return value
	}
	return ""
}

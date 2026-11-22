package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

type Endpoint struct {
	ID        string
	Name      string
	URL       string
	CreatedAt time.Time
}

type Delivery struct {
	ID         string
	EndpointID string
	Payload    string
	Status     string
	Attempts   int32
	LastError  *string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

type DeliveryTarget struct {
	Delivery
	URL string
}

func Open(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}

	return &Store{pool: pool}, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

func (s *Store) CreateEndpoint(ctx context.Context, name, url string) (Endpoint, error) {
	var endpoint Endpoint
	err := s.pool.QueryRow(ctx, `
		INSERT INTO endpoints (name, url)
		VALUES ($1, $2)
		RETURNING id::text, name, url, created_at
	`, name, url).Scan(&endpoint.ID, &endpoint.Name, &endpoint.URL, &endpoint.CreatedAt)
	if err != nil {
		return Endpoint{}, fmt.Errorf("create endpoint: %w", err)
	}
	return endpoint, nil
}

func (s *Store) ListEndpoints(ctx context.Context) ([]Endpoint, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id::text, name, url, created_at
		FROM endpoints
		ORDER BY created_at DESC
		LIMIT 100
	`)
	if err != nil {
		return nil, fmt.Errorf("list endpoints: %w", err)
	}
	defer rows.Close()

	endpoints := make([]Endpoint, 0)
	for rows.Next() {
		var endpoint Endpoint
		if err := rows.Scan(&endpoint.ID, &endpoint.Name, &endpoint.URL, &endpoint.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan endpoint: %w", err)
		}
		endpoints = append(endpoints, endpoint)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate endpoints: %w", err)
	}
	return endpoints, nil
}

func (s *Store) CreateDelivery(ctx context.Context, endpointID, payload string) (Delivery, error) {
	var delivery Delivery
	err := s.pool.QueryRow(ctx, `
		INSERT INTO deliveries (endpoint_id, payload, status)
		SELECT id, $2::jsonb, 'pending'
		FROM endpoints
		WHERE id = $1
		RETURNING id::text, endpoint_id::text, payload::text, status, attempts,
		          last_error, created_at, updated_at
	`, endpointID, payload).Scan(
		&delivery.ID,
		&delivery.EndpointID,
		&delivery.Payload,
		&delivery.Status,
		&delivery.Attempts,
		&delivery.LastError,
		&delivery.CreatedAt,
		&delivery.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Delivery{}, pgx.ErrNoRows
	}
	if err != nil {
		return Delivery{}, fmt.Errorf("create delivery: %w", err)
	}
	return delivery, nil
}

func (s *Store) GetDelivery(ctx context.Context, id string) (Delivery, error) {
	var delivery Delivery
	err := s.pool.QueryRow(ctx, `
		SELECT id::text, endpoint_id::text, payload::text, status, attempts,
		       last_error, created_at, updated_at
		FROM deliveries
		WHERE id = $1
	`, id).Scan(
		&delivery.ID,
		&delivery.EndpointID,
		&delivery.Payload,
		&delivery.Status,
		&delivery.Attempts,
		&delivery.LastError,
		&delivery.CreatedAt,
		&delivery.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Delivery{}, pgx.ErrNoRows
	}
	if err != nil {
		return Delivery{}, fmt.Errorf("get delivery: %w", err)
	}
	return delivery, nil
}

func (s *Store) GetDeliveryTarget(ctx context.Context, id string) (DeliveryTarget, error) {
	var target DeliveryTarget
	err := s.pool.QueryRow(ctx, `
		SELECT d.id::text, d.endpoint_id::text, d.payload::text, d.status,
		       d.attempts, d.last_error, d.created_at, d.updated_at, e.url
		FROM deliveries d
		JOIN endpoints e ON e.id = d.endpoint_id
		WHERE d.id = $1
	`, id).Scan(
		&target.ID,
		&target.EndpointID,
		&target.Payload,
		&target.Status,
		&target.Attempts,
		&target.LastError,
		&target.CreatedAt,
		&target.UpdatedAt,
		&target.URL,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return DeliveryTarget{}, pgx.ErrNoRows
	}
	if err != nil {
		return DeliveryTarget{}, fmt.Errorf("get delivery target: %w", err)
	}
	return target, nil
}

func (s *Store) ListDeliveries(ctx context.Context, endpointID string, limit int32) ([]Delivery, error) {
	if limit < 1 || limit > 100 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id::text, endpoint_id::text, payload::text, status, attempts,
		       last_error, created_at, updated_at
		FROM deliveries
		WHERE NULLIF($1::text, '') IS NULL
		   OR endpoint_id = NULLIF($1::text, '')::uuid
		ORDER BY created_at DESC
		LIMIT $2
	`, endpointID, limit)
	if err != nil {
		return nil, fmt.Errorf("list deliveries: %w", err)
	}
	defer rows.Close()

	deliveries := make([]Delivery, 0)
	for rows.Next() {
		var delivery Delivery
		if err := rows.Scan(
			&delivery.ID,
			&delivery.EndpointID,
			&delivery.Payload,
			&delivery.Status,
			&delivery.Attempts,
			&delivery.LastError,
			&delivery.CreatedAt,
			&delivery.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan delivery: %w", err)
		}
		deliveries = append(deliveries, delivery)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate deliveries: %w", err)
	}
	return deliveries, nil
}

func (s *Store) MarkAttemptStarted(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE deliveries
		SET status = 'delivering', attempts = attempts + 1, updated_at = now()
		WHERE id = $1
	`, id)
	if err != nil {
		return fmt.Errorf("mark delivery attempt started: %w", err)
	}
	return nil
}

func (s *Store) MarkAttemptFailed(ctx context.Context, id, message string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE deliveries
		SET status = 'retrying', last_error = $2, updated_at = now()
		WHERE id = $1
	`, id, message)
	if err != nil {
		return fmt.Errorf("record delivery attempt failure: %w", err)
	}
	return nil
}

func (s *Store) MarkDelivered(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE deliveries
		SET status = 'delivered', last_error = NULL, updated_at = now()
		WHERE id = $1
	`, id)
	if err != nil {
		return fmt.Errorf("mark delivery complete: %w", err)
	}
	return nil
}

func (s *Store) MarkDeliveryFailed(ctx context.Context, id, message string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE deliveries
		SET status = 'failed', last_error = $2, updated_at = now()
		WHERE id = $1
	`, id, message)
	if err != nil {
		return fmt.Errorf("mark delivery failed: %w", err)
	}
	return nil
}

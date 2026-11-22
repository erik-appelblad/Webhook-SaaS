# Webhook delivery SaaS

A learning-oriented MVP for managing webhook endpoints and reliably delivering
events. The first backend slice uses a GraphQL API, a separate gRPC delivery
service, Temporal workflows, and PostgreSQL.

## Local development

Requirements: Go 1.27.3+, Node.js/npm, Docker Compose, and `protoc`.

Quick start (starts PostgreSQL, Temporal, the chaos webhook sink, the delivery service, the GraphQL API, and the React dashboard in one terminal):

```sh
./scripts/dev.sh
```

The script installs `web/` dependencies automatically when `web/node_modules` is missing (`./scripts/dev.sh --install` forces a clean `npm ci`) and sets `DEV_ALLOWED_WEBHOOK_HOSTS` automatically for the local chaos sink (`http://localhost:9099/webhook`). Pressing `Ctrl+C` stops the local processes while keeping PostgreSQL and Temporal running; run `./scripts/dev.sh --down` (or `docker compose down`) to stop the Docker services.

Or start each service manually:

Start PostgreSQL and Temporal (add `--profile sink` to also run the chaos sink in Docker):

```sh
docker compose up -d
# Optional: run the chaos sink container instead of `go run ./cmd/sink`
docker compose --profile sink up -d
```

The Temporal UI is available at <http://localhost:8080>.

Run the chaos sink, delivery service, and API in separate terminals from the repository root:

```sh
go run ./cmd/sink
DEV_ALLOWED_WEBHOOK_HOSTS=localhost:9099,127.0.0.1:9099 go run ./cmd/delivery
go run ./cmd/api
```

The chaos sink UI is available at <http://localhost:9099> and receives webhooks at `POST http://localhost:9099/webhook` (or `/webhook/<bucket>`). You can toggle failure modes (`fail_first_n`, `status`, `flaky`), `Retry-After` headers, artificial delays, and optional HMAC-SHA256 verification directly from its UI or `/api/config`.

In another terminal, start the React dashboard:

```sh
cd web
npm ci
npm run dev
```

Open the Vite URL printed in the terminal (usually <http://localhost:5173>).
Its `/query` requests are proxied to the GraphQL API. The services use these
local defaults:

| Variable | Default |
| --- | --- |
| `DATABASE_URL` | `postgres://saas:saas@localhost:5433/saas?sslmode=disable` |
| `TEMPORAL_ADDRESS` | `localhost:7233` |
| `DELIVERY_GRPC_ADDRESS` | `localhost:9097` |
| `GRAPHQL_ADDRESS` | `localhost:8081` |
| `SINK_ADDRESS` | `localhost:9099` |
| `DEV_ALLOWED_WEBHOOK_HOSTS` | empty (`./scripts/dev.sh` sets it for the local sink; **do not use in production**) |

## Testing and migrations

Run the Go test suite (including Temporal workflow tests, chaos sink tests, and migration runner tests):

```sh
go test -race ./...
```

SQL migrations live in `internal/store/migrations/` (`<number>_<name>.sql`), run in numeric order inside a transaction under a PostgreSQL advisory lock, and are tracked by SHA-256 checksum in `schema_migrations`. Set `TEST_DATABASE_URL` to also run the live PostgreSQL migration test.

The delivery worker rejects private, loopback, link-local, and reserved IP
destinations by default (unless explicitly allowlisted via `DEV_ALLOWED_WEBHOOK_HOSTS`
for local development) and does not follow redirects. This MVP is still not
production-ready: it does not yet include authentication, tenant isolation, or
rate limits, and its outbound network policy needs a production security review.

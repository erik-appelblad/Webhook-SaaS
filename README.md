# Webhook delivery SaaS

A learning-oriented MVP for managing webhook endpoints and reliably delivering
events. The first backend slice uses a GraphQL API, a separate gRPC delivery
service, Temporal workflows, and PostgreSQL.

## Local development

Requirements: Go 1.27.3+, Node.js/npm, Docker Compose, and `protoc`.

Quick start (starts PostgreSQL, Temporal, the delivery service, the GraphQL API, and the React dashboard in one terminal):

```sh
./scripts/dev.sh
```

The script installs `web/` dependencies automatically when `web/node_modules` is missing (`./scripts/dev.sh --install` forces a clean `npm ci`). Pressing `Ctrl+C` stops the local processes while keeping PostgreSQL and Temporal running; run `./scripts/dev.sh --down` (or `docker compose down`) to stop the Docker services.

Or start each service manually:

Start PostgreSQL and Temporal:

```sh
docker compose up -d
```

The Temporal UI is available at <http://localhost:8080>.

Run the API and delivery service in separate terminals from the repository root:

```sh
go run ./cmd/delivery
go run ./cmd/api
```

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

The delivery worker rejects private, loopback, link-local, and reserved IP
destinations and does not follow redirects. This MVP is still not
production-ready: it does not yet include authentication, tenant isolation, or
rate limits, and its outbound network policy needs a production security review.

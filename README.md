# EventPulse

EventPulse is an asynchronous event-processing system built with Go, PostgreSQL, Redis Streams, and TypeScript.

## Architecture

- **API:** Accepts events and persists them in PostgreSQL.
- **Redis Streams:** Queues events for asynchronous processing.
- **Go Workers:** Process events concurrently, with retries and dead-letter handling.
- **PostgreSQL:** Stores event data and processing status.
- **Monitoring:** Prometheus metrics and Grafana dashboards for observability.

## Tech Stack

- TypeScript, Node.js
- Go
- PostgreSQL
- Redis Streams
- Prometheus & Grafana

## Status

Core event processing is implemented. Monitoring and Documentation are in progress.
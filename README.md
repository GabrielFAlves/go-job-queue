# Go Job Queue

Asynchronous job queue built in Go using Redis Streams, focused on hands-on learning of concurrency, reliability, and observability.

## Features

- Producer for creating and enqueueing jobs
- Worker with multiple registerable handlers
- Persistent queue using Redis Streams
- Retry with exponential backoff
- Dead Letter Queue (DLQ) for exhausted retries
- Graceful shutdown with `context` + `sync.WaitGroup`
- `/metrics` endpoint with queue processing counters
- Demo script for end-to-end terminal run

## Architecture

```text
Producer (Go)
  -> Redis Stream (jobs)
      -> Worker Consumer Group (workers)
          -> Handler Registry (by job type)
              -> Success: XACK + completed metric
              -> Failure: retry with backoff
                  -> attempts left: requeue to jobs
                  -> attempts exhausted: send to jobs.dlq

Worker also exposes:
  /metrics (pending, processing, completed, failed)
```

## Project Structure

```text
cmd/
  producer/        # producer binary entrypoint
  worker/          # worker binary entrypoint
internal/
  domain/          # core entities (Job)
  producer/        # job creation and publish service
  queue/           # Redis Streams adapter
  worker/          # worker engine and handler registry
  metrics/         # in-memory metrics store and HTTP handler
deployments/
  demo/            # terminal demo script + vhs tape
```

## Requirements

- Go 1.22+
- Redis 7+
- Docker (optional, easiest way to run Redis)

## Quick Start

### 1) Start Redis

```powershell
docker run --name gojq-redis -p 6379:6379 -d redis:7
```

### 2) Run Worker

```powershell
go run ./cmd/worker \
  -stream jobs \
  -dlq-stream jobs.dlq \
  -group workers \
  -consumer worker-1 \
  -concurrency 2 \
  -retry-base 1s \
  -retry-max 10s \
  -shutdown-timeout 10s \
  -metrics-addr :2112
```

### 3) Enqueue a Successful Job

```powershell
go run ./cmd/producer -type image.resize -payload "{}" -stream jobs
```

### 4) Enqueue a Failing Job (to test retry + DLQ)

```powershell
go run ./cmd/producer -type unknown.task -payload "{}" -max-attempts 3 -stream jobs
```

### 5) Check Metrics

```powershell
curl http://localhost:2112/metrics
```

Expected counters:

- `jobs_pending`
- `jobs_processing`
- `jobs_completed_total`
- `jobs_failed_total`

## Demo

Run full terminal demo:

```powershell
powershell -ExecutionPolicy Bypass -File ./deployments/demo/run-demo.ps1
```

Optional GIF generation (if installed):

- `vhs`
- `ffmpeg`

```powershell
vhs ./deployments/demo/demo.tape
```

## What This Project Practices

- Goroutines and worker pool concurrency
- Channels for dispatching stream messages to workers
- Context propagation for cancellation and shutdown
- Mutex-protected shared state (`handler registry`, `metrics store`)
- Fault handling patterns (retry, backoff, DLQ)

## Future Improvements

- Persistent/recoverable pending message re-claim strategy (`XPENDING`/`XCLAIM`)
- Structured logging and trace correlation IDs
- Prometheus-native instrumentation package
- Config file/env support
- Automated tests for worker retry and DLQ behavior

## License

MIT (or your preferred license)

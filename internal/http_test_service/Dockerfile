FROM golang:1.26 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY internal/http_test_service/ ./internal/http_test_service/
RUN go build -o /http-test-service ./internal/http_test_service

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*
COPY --from=builder /http-test-service /usr/local/bin/http-test-service
EXPOSE 17000
ENTRYPOINT ["http-test-service"]

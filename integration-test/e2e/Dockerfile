FROM golang:1.25 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_CFLAGS="-O1 -g" go build -tags=duckdb_arrow -o /dev-server ./cmd/dev-server

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*
COPY --from=builder /dev-server /usr/local/bin/dev-server
ENTRYPOINT ["dev-server"]

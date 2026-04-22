FROM golang:1.26 AS builder
WORKDIR /app
COPY go.mod go.sum ./
COPY types/go.mod types/go.sum ./types/
COPY client/go.mod client/go.sum ./client/
# Prefetch module dependencies without the workspace so the cache layer
# is stable across workspace edits.
RUN GOWORK=off go mod download
COPY . .
# The in-tree go.work (copied above) lets the build resolve
# github.com/hugr-lab/query-engine/types and /client against the
# sub-modules in the same checkout instead of the proxy-pinned
# versions in go.mod.
RUN CGO_CFLAGS="-O1 -g" go build -tags=duckdb_arrow -o /dev-server ./cmd/dev-server

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*
COPY --from=builder /dev-server /usr/local/bin/dev-server
ENTRYPOINT ["dev-server"]

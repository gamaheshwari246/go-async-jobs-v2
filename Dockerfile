FROM golang:1.23-bookworm AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -mod=readonly -trimpath -o /out/api ./cmd/api &&     go build -mod=readonly -trimpath -o /out/worker ./cmd/worker && \
    go build -mod=readonly -trimpath -o /out/healthcheck ./cmd/healthcheck && \
    go build -mod=readonly -trimpath -o /out/scheduler ./cmd/scheduler
FROM debian:bookworm-slim AS runtime
RUN useradd --uid 10001 -m appuser
USER appuser
COPY --from=builder /out/api /usr/local/bin/api
COPY --from=builder /out/worker /usr/local/bin/worker
COPY --from=builder /out/healthcheck /usr/local/bin/healthcheck
COPY --from=builder /out/scheduler /usr/local/bin/scheduler
EXPOSE 8080
CMD ["api"]

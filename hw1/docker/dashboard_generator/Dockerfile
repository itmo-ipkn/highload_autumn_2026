FROM golang:1.26-alpine AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY ./cmd/dashboard_generator ./cmd/dashboard_generator
COPY ./internal/dashboard ./internal/dashboard
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/generate_dashboard ./cmd/dashboard_generator

FROM alpine:3.19
RUN apk add --no-cache curl

WORKDIR /app
COPY --from=builder /out/generate_dashboard /app/generate_dashboard

ENTRYPOINT ["/app/generate_dashboard"]

FROM golang:1.26-alpine AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY ./cmd/load_tester ./cmd/load_tester
COPY ./internal/loadtester ./internal/loadtester
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/load_tester ./cmd/load_tester

FROM alpine:3.19
RUN apk add --no-cache curl

WORKDIR /app
COPY --from=builder /out/load_tester /app/load_tester

ENTRYPOINT ["/app/load_tester"]

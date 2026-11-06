FROM golang:1.27-alpine AS builder

WORKDIR /app

COPY . .

RUN CGO_ENABLED=0 GOOS=linux \
    go build -o /dashboard-generator .

FROM alpine:3.22

WORKDIR /app

COPY --from=builder /dashboard-generator ./dashboard-generator

ENTRYPOINT ["./dashboard-generator"]
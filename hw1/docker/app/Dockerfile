FROM golang:1.26-alpine AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY ./cmd/server ./cmd/server
COPY ./internal ./internal/
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server

FROM alpine:3.19
RUN apk add --no-cache curl

WORKDIR /app
COPY --from=builder /out/server /app/server

EXPOSE 8080

CMD ["./server"]

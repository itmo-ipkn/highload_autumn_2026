FROM golang:1.27.4 AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY *.go ./

RUN CGO_ENABLED=0 GOOS=linux go build -o producer .


FROM alpine:3.21

WORKDIR /app

COPY --from=builder /app/producer ./producer

ENTRYPOINT ["./producer"]


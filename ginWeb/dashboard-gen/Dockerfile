FROM golang:1.27.3 AS builder

WORKDIR /app

COPY go.mod ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /app/dashboard-gen .

FROM alpine:3.22

WORKDIR /app

COPY --from=builder /app/dashboard-gen .

CMD ["./dashboard-gen"]


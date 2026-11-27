# syntax=docker/dockerfile:1

FROM golang:1.26.8-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/consumer ./cmd/consumer && \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/verify ./cmd/verify

FROM alpine:3.22
RUN addgroup -S app && adduser -S -G app app
COPY --from=build /out/consumer /usr/local/bin/consumer
COPY --from=build /out/verify /usr/local/bin/verify
USER app
ENTRYPOINT ["/usr/local/bin/consumer"]

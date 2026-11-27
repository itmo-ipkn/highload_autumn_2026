# syntax=docker/dockerfile:1

FROM golang:1.26.8-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/producer ./cmd/producer

FROM alpine:3.22

RUN addgroup -S app && adduser -S -G app app
COPY --from=build /out/producer /usr/local/bin/producer

USER app
EXPOSE 2112
ENTRYPOINT ["/usr/local/bin/producer"]

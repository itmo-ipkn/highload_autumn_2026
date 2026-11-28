FROM golang:1.26 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=1 go build -trimpath -o /observer .

FROM debian:trixie-slim
COPY --from=builder /observer /observer
USER 1000:1000
ENTRYPOINT ["/observer"]

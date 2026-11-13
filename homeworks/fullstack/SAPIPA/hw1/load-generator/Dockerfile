FROM golang:1.25.14-alpine AS builder

WORKDIR /src

COPY go.mod ./
COPY main.go ./

RUN CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags="-s -w" \
    -o /load-generator .

FROM scratch

COPY --from=builder /load-generator /load-generator

USER 65532:65532

ENTRYPOINT ["/load-generator"]

FROM golang:1.26.8-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -o /dashboard-generator .

FROM scratch

COPY --from=builder /dashboard-generator /dashboard-generator

USER 65532:65532

ENTRYPOINT ["/dashboard-generator"]

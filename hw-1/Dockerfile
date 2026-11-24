FROM golang:1.27.3-alpine3.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM gcr.io/distroless/static-debian13:nonroot AS dashgen
COPY --from=build /out/dashgen /dashgen
ENTRYPOINT ["/dashgen"]

FROM gcr.io/distroless/static-debian13:nonroot AS app
COPY --from=build /out/app /app
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=3s --retries=5 CMD ["/app", "-healthcheck"]
ENTRYPOINT ["/app"]

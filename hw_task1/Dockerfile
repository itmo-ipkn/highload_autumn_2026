FROM golang:1.27.2-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY pkg ./pkg
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM gcr.io/distroless/static-debian12:nonroot AS dashboard-generator
COPY --from=build /out/dashboard-generator /dashboard-generator
USER nonroot:nonroot
ENTRYPOINT ["/dashboard-generator"]

FROM gcr.io/distroless/static-debian12:nonroot AS app
COPY --from=build /out/app /app
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/app"]

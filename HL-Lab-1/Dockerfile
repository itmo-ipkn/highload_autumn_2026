FROM golang:1.27.3-alpine3.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/items-service . \
    && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/dashboard-generator ./cmd/dashboard-generator \
    && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/load-generator ./cmd/load-generator

FROM scratch AS items
COPY --from=build /out/items-service /items-service
EXPOSE 8080
USER 65532:65532
HEALTHCHECK --interval=5s --timeout=2s --start-period=5s --retries=12 CMD ["/items-service", "healthcheck"]
ENTRYPOINT ["/items-service"]

FROM scratch AS dashboard-generator
COPY --from=build /out/dashboard-generator /dashboard-generator
USER 65532:65532
ENTRYPOINT ["/dashboard-generator"]

FROM scratch AS load-generator
COPY --from=build /out/load-generator /load-generator
USER 65532:65532
ENTRYPOINT ["/load-generator"]

FROM golang:1.27.3 AS source
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

FROM source AS test
RUN go test -race ./... && go vet ./...

FROM source AS build
RUN CGO_ENABLED=0 go build -trimpath -o /out/items-api ./cmd/app
RUN CGO_ENABLED=0 go build -trimpath -o /out/dashboard ./cmd/dashboard

RUN CGO_ENABLED=0 go build -trimpath -o /out/load ./cmd/load

FROM scratch AS load
COPY --from=build /out/load /load
USER 65532:65532
ENTRYPOINT ["/load"]

FROM scratch AS dashboard
COPY --from=build /out/dashboard /dashboard
USER 65532:65532
ENTRYPOINT ["/dashboard"]

FROM scratch AS runtime
COPY --from=build /out/items-api /items-api
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/items-api"]

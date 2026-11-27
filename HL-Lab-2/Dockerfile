FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGET=producer
RUN CGO_ENABLED=0 go build -o /app ./cmd/${TARGET}

FROM scratch
COPY --from=build /app /app
ENTRYPOINT ["/app"]

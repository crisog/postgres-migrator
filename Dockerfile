FROM golang:1.25.1-alpine AS builder

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o postgres-migrator ./cmd/postgres-migrator

FROM alpine:3.19

ARG PG_VERSION=16
RUN apk add --no-cache postgresql${PG_VERSION}-client

WORKDIR /app

COPY --from=builder /build/postgres-migrator .

RUN addgroup -g 1000 migrator && \
    adduser -D -u 1000 -G migrator migrator && \
    chown -R migrator:migrator /app

USER migrator

ENTRYPOINT ["/app/postgres-migrator"]

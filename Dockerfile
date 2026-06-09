FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /slotwatch ./cmd/slotwatch

FROM alpine:3.19
# ca-certificates needed for TLS connections to Kafka and Postgres
RUN apk --no-cache add ca-certificates
COPY --from=builder /slotwatch /usr/local/bin/slotwatch
ENTRYPOINT ["slotwatch"]
CMD ["-config", "/etc/slotwatch/config.yml"]

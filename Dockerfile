# =================== 1. Builder ===================
FROM golang:1.24-alpine AS builder

RUN apk add --no-cache gcc musl-dev git

WORKDIR /app

# Copy vendored dependencies and source code
COPY . .

# Build with vendored dependencies (no network access needed)
RUN CGO_ENABLED=1 go build -mod=vendor -o /dogecoin-relayer .

# =================== 2. Run ===================
FROM alpine:3.21

WORKDIR /app

RUN mkdir -p /app/data
RUN apk add --no-cache tzdata

COPY --from=builder /dogecoin-relayer /app/dogecoin-relayer

EXPOSE 8080 4001 5001 50051

CMD ["/app/dogecoin-relayer", "-config", "/app/config/config.yaml"]

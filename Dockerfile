FROM golang:1.24-alpine AS builder

RUN apk add --no-cache gcc musl-dev git

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 go build -o /dogecoin-relayer .

FROM alpine:3.21

WORKDIR /app

RUN mkdir -p /app/data
RUN apk add --no-cache tzdata

COPY --from=builder /dogecoin-relayer /app/dogecoin-relayer

EXPOSE 8080 4001

CMD ["/app/dogecoin-relayer", "-config", "/app/config/config.yaml"]

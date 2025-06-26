# =================== 1. Builder ===================
FROM golang:1.24-alpine AS builder

RUN apk add --no-cache gcc musl-dev git

WORKDIR /app

# 1. set github token for private repo
ARG GITHUB_TOKEN
# 2. set private repo domain
ENV GOPRIVATE=github.com/goatnetwork/tss

# 3. set env for private repo
RUN echo "machine github.com login ${GITHUB_TOKEN} password x-oauth-basic" > ~/.netrc && \
    chmod 600 ~/.netrc

# 4. go download
COPY go.mod go.sum ./
RUN go mod download

# 5. copy source code and build
COPY . .
RUN CGO_ENABLED=1 go build -o /dogecoin-relayer .

# 6. clean git config
RUN git config --global --remove-section url."https://${GITHUB_TOKEN}:x-oauth-basic@github.com/" || true

# =================== 2. Run ===================
FROM alpine:3.21

WORKDIR /app

RUN mkdir -p /app/data
RUN apk add --no-cache tzdata

COPY --from=builder /dogecoin-relayer /app/dogecoin-relayer

EXPOSE 8080 4001

CMD ["/app/dogecoin-relayer", "-config", "/app/config/config.yaml"]

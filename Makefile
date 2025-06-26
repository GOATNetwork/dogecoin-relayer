# Makefile for goat-bridge-assistant

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
BINARY_NAME=dogecoin-relayer
GITHUB_TOKEN=$(shell grep ^GITHUB_TOKEN .env | cut -d '=' -f2)
GOPRIVATE=$(shell grep ^GOPRIVATE .env | cut -d '=' -f2)

# Build binary
all: build

tidy:
	@if [ -z "$(GITHUB_TOKEN)" ]; then \
		echo "❌  GITHUB_TOKEN is not set"; exit 1; \
	fi
	@echo "machine github.com login ${GITHUB_TOKEN} password x-oauth-basic" > ~/.netrc && chmod 600 ~/.netrc
	GOPRIVATE=$(GOPRIVATE) $(GOCMD) mod tidy
	rm -f ~/.netrc

build:
	@if [ -z "$(GITHUB_TOKEN)" ]; then \
		echo "❌  GITHUB_TOKEN is not set"; exit 1; \
	fi
	@echo "machine github.com login ${GITHUB_TOKEN} password x-oauth-basic" > ~/.netrc && chmod 600 ~/.netrc
	GOPRIVATE=$(GOPRIVATE) $(GOBUILD) -o bin/$(BINARY_NAME) -v .
	rm -f ~/.netrc

clean:
	$(GOCLEAN)
	rm -f $(BINARY_NAME)

test:
	$(GOTEST) -v ./...

deps:
	$(GOGET) -u ./...

# Run the binary, copy the config.yaml file to ./data folder
run:
	$(GOBUILD) -o bin/$(BINARY_NAME) -v . && ./bin/$(BINARY_NAME) -config ./data/config.yaml

docker-build-all:
	@if [ -z "$(GITHUB_TOKEN)" ]; then \
		echo "❌  GITHUB_TOKEN is not set"; exit 1; \
	fi
	docker buildx build --platform linux/amd64,linux/arm64 --build-arg GITHUB_TOKEN=$(GITHUB_TOKEN) -t goat-network/dogecoin-relayer:latest --push .

docker-build:
	@if [ -z "$(GITHUB_TOKEN)" ]; then \
		echo "❌  GITHUB_TOKEN is not set"; exit 1; \
	fi
	docker buildx build --platform linux/amd64 --build-arg GITHUB_TOKEN=$(GITHUB_TOKEN) -t goat-network/dogecoin-relayer:latest --load .

docker-build-x:
	@if [ -z "$(GITHUB_TOKEN)" ]; then \
		echo "❌  GITHUB_TOKEN is not set"; exit 1; \
	fi
	docker buildx build --platform linux/arm64 --build-arg GITHUB_TOKEN=$(GITHUB_TOKEN) -t goat-network/dogecoin-relayer:latest --load .

.PHONY: all tidy build clean test deps run docker-build docker-build-all docker-build-x
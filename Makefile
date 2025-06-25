# Makefile for goat-bridge-assistant

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
BINARY_NAME=dogecoin-relayer

# Build binary
all: build

build:
	$(GOBUILD) -o bin/$(BINARY_NAME) -v .

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
	docker buildx build --platform linux/amd64,linux/arm64 -t goat-network/dogecoin-relayer:latest --push .

docker-build:
	docker buildx build --platform linux/amd64 -t goat-network/dogecoin-relayer:latest --load .

docker-build-x:
	docker buildx build --platform linux/arm64 -t goat-network/dogecoin-relayer:latest --no-cache --load .

.PHONY: all build clean test deps run docker-build docker-build-all docker-build-x
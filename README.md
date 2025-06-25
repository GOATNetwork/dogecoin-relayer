# Dogecoin Relayer

*please read the config.yaml file to set the correct parameters*

## Chapter 1: Local Test Run

```bash
go run main.go -config ./data/config.yaml
```

## Chapter 2: Run Unit Tests

```bash
make test
```

## Chapter 3: Local Build

```bash
make build
```

## Chapter 4: Docker Buildx

```bash
make docker-build-x
```

## Required Environment Variables

```bash
PROPOSER_PRIVATE_KEY
```

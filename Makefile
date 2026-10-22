SHELL := /bin/bash

.PHONY: all help test race cover fmt vet lint tidy generate clean

all: test ## Build and test

help: ## Show available make targets
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'

test: ## Run all tests
	go test ./...

race: ## Run tests with the race detector
	go test -race ./...

cover: ## Run tests and report coverage
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -n 1

fmt: ## Format Go sources
	go fmt ./...

vet: ## Run go vet
	go vet ./...

lint: vet fmt ## Run linting (golangci-lint if available)
	@which golangci-lint > /dev/null 2>&1 && golangci-lint run || echo "golangci-lint not installed, skipping"

tidy: ## Tidy go.mod
	go mod tidy

# Generated code is committed, so a build never depends on a contributor having
# protoc installed. buf is optional: protoc alone is enough for one file.
generate: ## Regenerate protobuf code under pb/
	@which protoc > /dev/null 2>&1 || { echo "protoc not installed"; exit 1; }
	protoc \
		--proto_path=proto \
		--go_out=. --go_opt=module=github.com/seanly/pulse-devkit \
		--go-grpc_out=. --go-grpc_opt=module=github.com/seanly/pulse-devkit \
		proto/pulse/v1/pulse.proto

clean: ## Remove build output
	rm -rf bin coverage.out

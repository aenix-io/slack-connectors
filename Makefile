export PATH := $(HOME)/sdk/go/bin:$(HOME)/go/bin:$(PATH)

.PHONY: test lint build tidy

test:
	go test ./... -race -count=1

lint:
	golangci-lint run

build:
	go build -ldflags "-X github.com/aenix-io/slack-connectors/internal/version.Version=$(shell git describe --tags --always --dirty)" ./cmd/bridge

tidy:
	go mod tidy

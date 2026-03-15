BINARY := steeplechase
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -ldflags "-X main.version=$(VERSION)"
GOFLAGS := -trimpath

.PHONY: build test vet clean

build:
	go build $(GOFLAGS) $(LDFLAGS) -o bin/$(BINARY) ./cmd/steeplechase

test:
	go test -race ./...

vet:
	go vet ./...

clean:
	rm -rf bin/

all: vet test build

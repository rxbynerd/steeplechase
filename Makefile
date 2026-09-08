BINARY := steeplechase
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -ldflags "-X main.version=$(VERSION)"
GOFLAGS := -trimpath
IMAGE ?= ghcr.io/rxbynerd/steeplechase
CONTAINER_TOOL ?= podman

.PHONY: build test vet clean image

build:
	go build $(GOFLAGS) $(LDFLAGS) -o bin/$(BINARY) ./cmd/steeplechase

test:
	go test -race ./...

vet:
	go vet ./...

image:
	$(CONTAINER_TOOL) build -f Containerfile --build-arg VERSION=$(VERSION) -t $(IMAGE):$(VERSION) .

clean:
	rm -rf bin/

all: vet test build

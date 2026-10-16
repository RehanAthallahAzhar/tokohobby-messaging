.PHONY: all test cover lint build verify fmt clean

all: fmt verify lint test build

fmt:
	gofmt -w -s .

verify:
	go mod verify

lint:
	golangci-lint run --config=golangci.yml

test:
	go test -v -race ./...

cover:
	go test -race -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

build:
	go build -v ./...

clean:
	rm -f coverage.out
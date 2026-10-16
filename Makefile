.PHONY: all test cover lint build verify fmt vuln clean

all: fmt verify lint test vuln build

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

vuln:
	govulncheck ./...

build:
	go build -v ./...

clean:
	rm -f coverage.out
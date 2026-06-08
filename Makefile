.PHONY: build test lint up down

build:
	go build -o bin/slotwatch ./cmd/slotwatch

test:
	go test ./... -v

lint:
	go vet ./...

up:
	docker compose up --build

down:
	docker compose down

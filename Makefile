.PHONY: up down test proto lint clean

up:
	docker-compose up --build

down:
	docker-compose down

test:
	cd sandbox && go test ./...
	cd botfleet && go test ./...
	cd telemetry && go test ./...

proto:
	protoc --go_out=. --go-grpc_out=. proto/*.proto

lint:
	golangci-lint run ./...

clean:
	docker-compose down --volumes --remove-orphans

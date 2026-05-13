.PHONY: up down test sandbox-deps sandbox-build sandbox-test proto lint clean

up:
	docker-compose up --build

down:
	docker-compose down

sandbox-deps:
	cd sandbox && go mod tidy && go mod download

sandbox-build:
	cd sandbox && CGO_ENABLED=0 go build -ldflags="-s -w" -o /dev/null .

sandbox-test:
	cd sandbox && go test -race -count=1 ./...

test:
	$(MAKE) sandbox-test
	cd botfleet && go test ./...
	cd telemetry && go test ./...

proto:
	protoc --go_out=. --go-grpc_out=. proto/*.proto

lint:
	golangci-lint run ./...

clean:
	docker-compose down --volumes --remove-orphans

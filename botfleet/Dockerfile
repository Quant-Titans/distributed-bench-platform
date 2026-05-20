FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o botfleet ./cmd/

FROM alpine:3.20
WORKDIR /app
COPY --from=builder /app/botfleet .
EXPOSE 9090
CMD ["./botfleet"]

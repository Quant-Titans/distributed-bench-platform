FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY main.go .
RUN go mod init dummy-engine && \
    CGO_ENABLED=0 go build -ldflags="-s -w" -o dummy-engine .

FROM alpine:3.20
WORKDIR /app
COPY --from=builder /app/dummy-engine .
EXPOSE 9000
CMD ["./dummy-engine"]

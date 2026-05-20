FROM golang:1.22-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git

COPY go.mod go.sum* ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /sandbox-manager .

# ---- runtime image ----
FROM alpine:3.19

RUN apk add --no-cache ca-certificates

COPY --from=builder /sandbox-manager /sandbox-manager
COPY seccomp/profile.json /etc/sandbox/seccomp.json

EXPOSE 8080

ENTRYPOINT ["/sandbox-manager"]

# Stage 1 — build React frontend
FROM node:22-alpine AS frontend
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY index.html vite.config.ts tsconfig.json tsconfig.node.json ./
COPY src ./src
RUN npm run build

# Stage 2 — build Go WebSocket server
FROM golang:1.26-alpine AS backend
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY server ./server
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o leaderboard-server ./server/

# Stage 3 — minimal runtime image
FROM alpine:3.20
WORKDIR /app
COPY --from=backend /app/leaderboard-server .
COPY --from=frontend /app/dist ./dist
EXPOSE 8082
ENV STATIC_DIR=/app/dist
CMD ["./leaderboard-server"]

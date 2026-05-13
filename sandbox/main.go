package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Quant-Titans/distributed-bench-platform/sandbox/internal/handler"
	"github.com/Quant-Titans/distributed-bench-platform/sandbox/internal/manager"
)

func main() {
	mgr, err := manager.New()
	if err != nil {
		log.Fatalf("sandbox manager init: %v", err)
	}
	defer mgr.Close()

	h := handler.New(mgr)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/sandbox/run", h.Run)
	mux.HandleFunc("GET /v1/sandbox/{id}/status", h.Status)
	mux.HandleFunc("DELETE /v1/sandbox/{id}", h.Stop)
	mux.HandleFunc("GET /healthz", h.Health)

	addr := envOr("LISTEN_ADDR", ":8080")
	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("sandbox manager listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Println("shutting down sandbox manager")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

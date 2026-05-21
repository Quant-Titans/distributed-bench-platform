package handler

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/Quant-Titans/distributed-bench-platform/sandbox/internal/manager"
)

type Handler struct {
	mgr          *manager.Manager
	botFleetAddr string // e.g. "http://botfleet:9091" — empty disables auto-spawn
}

func New(mgr *manager.Manager, botFleetAddr string) *Handler {
	return &Handler{mgr: mgr, botFleetAddr: botFleetAddr}
}

type runRequest struct {
	SessionID string   `json:"session_id"`
	TeamName  string   `json:"team_name,omitempty"`
	Image     string   `json:"image"`
	CPUCores  string   `json:"cpu_cores"`
	MemoryMB  int64    `json:"memory_mb"`
	TimeoutS  int      `json:"timeout_s"`
	Env       []string `json:"env"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func (h *Handler) Run(w http.ResponseWriter, r *http.Request) {
	var req runRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Image == "" {
		writeError(w, http.StatusBadRequest, "image is required")
		return
	}

	info, err := h.mgr.Run(r.Context(), manager.Config{
		SessionID: req.SessionID,
		TeamName:  req.TeamName,
		Image:     req.Image,
		CPUCores:  req.CPUCores,
		MemoryMB:  req.MemoryMB,
		TimeoutS:  req.TimeoutS,
		Env:       req.Env,
	})
	if err != nil {
		log.Printf("run sandbox %s: %v", req.Image, err)
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, info)
}

func (h *Handler) Status(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "id is required")
		return
	}

	info, err := h.mgr.Status(id)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, info)
}

func (h *Handler) Stop(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "id is required")
		return
	}

	if err := h.mgr.Stop(id); err != nil {
		log.Printf("stop sandbox %s: %v", id, err)
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, errorResponse{Error: msg})
}

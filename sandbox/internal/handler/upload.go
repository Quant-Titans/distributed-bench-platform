package handler

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	dockertypes "github.com/docker/docker/api/types"
	"github.com/docker/docker/client"

	"github.com/Quant-Titans/distributed-bench-platform/sandbox/internal/manager"
)

const (
	maxUploadSize = 128 << 20 // 128 MiB
	enginePort    = 8080
)

// dockerfile wraps a contestant binary in a minimal alpine image.
// The binary is expected to listen on :8080 and expose:
//
//	GET  /healthz            → {"status":"ok"}
//	POST /v1/order           → order submission
//	GET  /stats              → throughput + fill stats
var contestantDockerfile = `FROM alpine:3.20
RUN apk add --no-cache ca-certificates libstdc++ libc6-compat
COPY engine /engine
RUN chmod +x /engine
EXPOSE 8080
ENTRYPOINT ["/engine"]
`

// UploadResponse extends manager.Info with the upload outcome.
type UploadResponse struct {
	*manager.Info
	ImageTag   string `json:"image_tag"`
	BuildMS    int64  `json:"build_ms"`
}

// Upload accepts a compiled trading-engine binary (multipart/form-data),
// wraps it in a Docker image, and immediately starts a sandboxed benchmark run.
//
// Form fields:
//
//	session_id  string  — optional; auto-generated if absent
//	team_name   string  — required for leaderboard display
//	binary      file    — compiled Linux/amd64 trading engine binary
//	cpu_cores   string  — cpuset for pinning, default "0,1"
//	memory_mb   int     — container memory limit MiB, default 512
//	timeout_s   int     — benchmark duration seconds, default 120
func (h *Handler) Upload(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(maxUploadSize); err != nil {
		writeError(w, http.StatusBadRequest, "parse form: "+err.Error())
		return
	}

	sessionID := r.FormValue("session_id")
	if sessionID == "" {
		sessionID = fmt.Sprintf("s-%d", time.Now().UnixNano())
	}
	teamName := r.FormValue("team_name")
	if teamName == "" {
		teamName = "team-" + sessionID[len(sessionID)-6:]
	}

	cpuCores := r.FormValue("cpu_cores")
	if cpuCores == "" {
		cpuCores = "0,1"
	}
	memoryMB := int64(512)
	timeoutS := 120
	if v := r.FormValue("memory_mb"); v != "" {
		fmt.Sscanf(v, "%d", &memoryMB)
	}
	if v := r.FormValue("timeout_s"); v != "" {
		fmt.Sscanf(v, "%d", &timeoutS)
	}

	file, header, err := r.FormFile("binary")
	if err != nil {
		writeError(w, http.StatusBadRequest, "binary field required: "+err.Error())
		return
	}
	defer file.Close()

	log.Printf("upload: session=%s team=%q file=%s size=%d", sessionID, teamName, header.Filename, header.Size)

	// Build image tag from session — lowercase, no underscores (Docker tag rules).
	slug := strings.ToLower(strings.NewReplacer("_", "-", " ", "-").Replace(sessionID))
	imageTag := "contestant/" + slug + ":latest"

	buildStart := time.Now()
	if err := buildContestantImage(r.Context(), imageTag, file); err != nil {
		log.Printf("upload: docker build %s: %v", imageTag, err)
		writeError(w, http.StatusInternalServerError, "docker build: "+err.Error())
		return
	}
	buildMS := time.Since(buildStart).Milliseconds()

	info, err := h.mgr.Run(r.Context(), manager.Config{
		SessionID: sessionID,
		TeamName:  teamName,
		Image:     imageTag,
		CPUCores:  cpuCores,
		MemoryMB:  memoryMB,
		TimeoutS:  timeoutS,
		Env:       []string{"TEAM_NAME=" + teamName, "SESSION_ID=" + sessionID},
	})
	if err != nil {
		log.Printf("upload: run sandbox %s: %v", imageTag, err)
		writeError(w, http.StatusInternalServerError, "start sandbox: "+err.Error())
		return
	}

	log.Printf("upload: sandbox started id=%s endpoint=%s build_ms=%d", info.ID, info.Endpoint, buildMS)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(UploadResponse{
		Info:     info,
		ImageTag: imageTag,
		BuildMS:  buildMS,
	})
}

// buildContestantImage writes a minimal tar build context (Dockerfile + binary)
// and calls docker build. It blocks until the build completes or ctx is cancelled.
func buildContestantImage(ctx context.Context, tag string, binary io.Reader) error {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return fmt.Errorf("docker client: %w", err)
	}
	defer cli.Close()

	engineBytes, err := io.ReadAll(io.LimitReader(binary, maxUploadSize))
	if err != nil {
		return fmt.Errorf("read binary: %w", err)
	}
	if len(engineBytes) == 0 {
		return fmt.Errorf("binary is empty")
	}

	buildCtx, err := makeBuildContext(engineBytes)
	if err != nil {
		return err
	}

	resp, err := cli.ImageBuild(ctx, buildCtx, dockertypes.ImageBuildOptions{
		Tags:        []string{tag},
		Remove:      true,
		ForceRemove: true,
	})
	if err != nil {
		return fmt.Errorf("image build API: %w", err)
	}
	defer resp.Body.Close()

	// Scan JSON stream for build errors — must consume to completion.
	dec := json.NewDecoder(resp.Body)
	for {
		var msg struct {
			Error  string `json:"error"`
			Stream string `json:"stream"`
		}
		if err := dec.Decode(&msg); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("decode build output: %w", err)
		}
		if msg.Error != "" {
			return fmt.Errorf("build error: %s", msg.Error)
		}
	}
	return nil
}

// makeBuildContext returns an in-memory tar archive containing Dockerfile + binary.
func makeBuildContext(engineBytes []byte) (*bytes.Reader, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	dfBytes := []byte(contestantDockerfile)
	for _, entry := range []struct {
		name string
		mode int64
		data []byte
	}{
		{"Dockerfile", 0644, dfBytes},
		{"engine", 0755, engineBytes},
	} {
		if err := tw.WriteHeader(&tar.Header{
			Name: entry.name,
			Mode: entry.mode,
			Size: int64(len(entry.data)),
		}); err != nil {
			return nil, err
		}
		if _, err := tw.Write(entry.data); err != nil {
			return nil, err
		}
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	return bytes.NewReader(buf.Bytes()), nil
}

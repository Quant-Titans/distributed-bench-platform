#!/usr/bin/env bash
# ============================================================
#  Quant Titans — Demo Startup Script
#  Run this after every computer restart to get everything live.
#  Usage: ./start-demo.sh
# ============================================================
set -e

LEADERBOARD_DOMAIN="leaderboard.quanttitans.win"
GRAFANA_DOMAIN="grafana.quanttitans.win"
NAMESPACE="quant-titans"

echo "=== Quant Titans Demo Startup ==="
echo ""

# ── Step 1: Wait for Docker ──────────────────────────────────
echo "[1/5] Waiting for Docker engine..."
for i in $(seq 1 30); do
  if docker info > /dev/null 2>&1; then
    echo "      Docker ready."
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "      ERROR: Docker not ready after 60s. Open Docker Desktop and try again."
    exit 1
  fi
  sleep 2
done

# ── Step 2: Wait for kind cluster ───────────────────────────
echo "[2/5] Waiting for kind cluster to restore..."
for i in $(seq 1 30); do
  if kubectl get nodes --request-timeout=3s > /dev/null 2>&1; then
    echo "      Cluster ready."
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "      Cluster not found. Run: make up"
    exit 1
  fi
  sleep 2
done

# ── Step 3: Wait for pods ────────────────────────────────────
echo "[3/5] Waiting for pods to be Running..."
kubectl wait --for=condition=Ready pod -l app=leaderboard -n "$NAMESPACE" --timeout=120s > /dev/null 2>&1 && \
kubectl wait --for=condition=Ready pod -l app=grafana    -n "$NAMESPACE" --timeout=120s > /dev/null 2>&1 && \
kubectl wait --for=condition=Ready pod -l app=botfleet   -n "$NAMESPACE" --timeout=120s > /dev/null 2>&1
echo "      Pods ready."

# ── Step 4: Start tunnels ────────────────────────────────────
echo "[4/5] Starting public tunnels..."

pkill -f "ngrok" 2>/dev/null || true
pkill -f "cloudflared" 2>/dev/null || true
sleep 1

cloudflared tunnel --config ~/.cloudflared/leaderboard.yml run > /tmp/cloudflared-leaderboard.log 2>&1 &
cloudflared tunnel --config ~/.cloudflared/grafana.yml run > /tmp/cloudflared-grafana.log 2>&1 &

# Wait for tunnels to establish
sleep 6

echo "      Leaderboard: https://$LEADERBOARD_DOMAIN  (permanent)"
echo "      Grafana:     https://$GRAFANA_DOMAIN  (permanent)"

# ── Step 5: Spawn bot fleets ─────────────────────────────────
echo "[5/6] Spawning 600 bots across 3 teams (3 hour session)..."

curl -sf -X POST http://localhost:9091/v1/spawn \
  -H "Content-Type: application/json" \
  -d '{"session_id":"demo-qt","team_name":"Quant Titans","bot_count":200,"target_tps":20,"duration_secs":10800,"endpoint_url":"http://dummy-engine:9000"}' > /dev/null

curl -sf -X POST http://localhost:9091/v1/spawn \
  -H "Content-Type: application/json" \
  -d '{"session_id":"demo-as","team_name":"Alpha Strike","bot_count":200,"target_tps":3,"duration_secs":10800,"endpoint_url":"http://dummy-engine:9000"}' > /dev/null

curl -sf -X POST http://localhost:9091/v1/spawn \
  -H "Content-Type: application/json" \
  -d '{"session_id":"demo-iq","team_name":"Iron Quant","bot_count":200,"target_tps":10,"duration_secs":10800,"endpoint_url":"http://dummy-engine:9000"}' > /dev/null

echo "      Bots spawned. Scores appear on leaderboard in ~15s."

# ── Step 6: Start chaos loop ─────────────────────────────────
echo "[6/6] Starting continuous chaos loop (live racing competition)..."
pkill -f "chaos-loop.sh" 2>/dev/null || true
sleep 1
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
"$SCRIPT_DIR/chaos-loop.sh" > /tmp/chaos-loop.log 2>&1 &
echo "      Chaos loop running. Watch: tail -f /tmp/chaos-loop.log"

echo ""
echo "=== All systems live ==="
echo "   Leaderboard : https://$LEADERBOARD_DOMAIN"
echo "   Grafana     : https://$GRAFANA_DOMAIN"
echo "   Grafana     : http://localhost:3000  (local)"
echo ""
echo "   Grafana login: admin / quanttitans"
echo "   Chaos loop  : tail -f /tmp/chaos-loop.log"
echo ""

#!/usr/bin/env bash
# ============================================================
#  Quant Titans — Continuous Chaos Loop
#  Rotates CPU-stress chaos through all 3 sessions forever.
#  Each cycle: target session gets ⚡ on leaderboard + real
#  p99 spike (dummy-engine slowed) → scores drop → positions swap.
# ============================================================

NAMESPACE="quant-titans"
SESSIONS=("demo-qt"      "demo-as"       "demo-iq")
NAMES=(   "Quant Titans" "Alpha Strike"  "Iron Quant")

CHAOS_SECS=18    # how long chaos is active per session
CLEAN_SECS=22    # clean racing between chaos bursts
IDX=0

publish_event() {
  local session_id=$1 event_type=$2
  printf '{"session_id":"%s","event_type":"%s","ts":%s}' \
    "$session_id" "$event_type" "$(date +%s%N)" | \
  kubectl exec -n "$NAMESPACE" quant-titans-0 -- \
    rpk topic produce bench.events \
    --brokers localhost:9093 \
    --key "$session_id" 2>/dev/null
}

inject_chaos() {
  # Saturate dummy-engine CPU for $CHAOS_SECS seconds.
  # This drives p99 latency up → tail_latency_score drops → score falls.
  kubectl exec -n "$NAMESPACE" deployment/dummy-engine -- \
    sh -c "timeout ${CHAOS_SECS} yes > /dev/null
           timeout ${CHAOS_SECS} yes > /dev/null" 2>/dev/null &
}

echo "[chaos-loop] Live racing started. Ctrl+C to stop."
echo "[chaos-loop] Cycle: ${CHAOS_SECS}s chaos → ${CLEAN_SECS}s clean racing, rotating through all 3 teams."
echo ""

while true; do
  SESSION="${SESSIONS[$((IDX % 3))]}"
  NAME="${NAMES[$((IDX % 3))]}"

  echo "[chaos-loop] $(date '+%H:%M:%S') ⚡ CHAOS → $NAME ($SESSION)"
  publish_event "$SESSION" "chaos_start"
  inject_chaos

  sleep "$CHAOS_SECS"

  echo "[chaos-loop] $(date '+%H:%M:%S') ✓ RECOVERY → $NAME — clean racing for ${CLEAN_SECS}s"
  publish_event "$SESSION" "chaos_end"

  sleep "$CLEAN_SECS"
  IDX=$((IDX + 1))
done

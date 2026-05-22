import { useEffect, useRef, useState, useCallback } from 'react'

export interface LeaderboardEntry {
  rank: number
  team_name: string
  session_id: string
  p50_ns: number
  p90_ns: number
  p99_ns: number
  p999_ns: number
  tps: number
  peak_tps: number
  fill_accuracy: number
  price_time_violations: number
  recovery_time_ms: number
  degradation_ratio: number
  throughput_score: number
  tail_latency_score: number
  correctness_score: number
  resilience_score: number
  total_score: number
  active_bots: number
  score_history: number[]
  computed_at_ns: number
}

interface LeaderboardSnapshot {
  entries: LeaderboardEntry[]
  snapshot_ns: number
}

type ConnectionState = 'connecting' | 'connected' | 'reconnecting' | 'error'

export function useWebSocket(url: string) {
  const [entries, setEntries] = useState<LeaderboardEntry[]>([])
  const [connState, setConnState] = useState<ConnectionState>('connecting')
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)
  const wsRef = useRef<WebSocket | null>(null)
  const reconnectTimer = useRef<ReturnType<typeof setTimeout> | null>(null)
  const attempt = useRef(0)

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return

    setConnState(attempt.current === 0 ? 'connecting' : 'reconnecting')
    const ws = new WebSocket(url)
    wsRef.current = ws

    ws.onopen = () => {
      setConnState('connected')
      attempt.current = 0
    }

    ws.onmessage = (ev) => {
      try {
        const snap: LeaderboardSnapshot = JSON.parse(ev.data)
        // Sort by total_score descending and assign rank
        const sorted = [...snap.entries].sort((a, b) => b.total_score - a.total_score)
        sorted.forEach((e, i) => { e.rank = i + 1 })
        setEntries(sorted)
        setLastUpdated(new Date())
      } catch {
        // malformed message — ignore
      }
    }

    ws.onerror = () => setConnState('error')

    ws.onclose = () => {
      attempt.current++
      const delay = Math.min(1000 * 2 ** attempt.current, 30000)
      reconnectTimer.current = setTimeout(connect, delay)
    }
  }, [url])

  useEffect(() => {
    connect()
    return () => {
      reconnectTimer.current && clearTimeout(reconnectTimer.current)
      wsRef.current?.close()
    }
  }, [connect])

  return { entries, connState, lastUpdated }
}

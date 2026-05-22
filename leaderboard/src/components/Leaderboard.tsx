import React, { useState } from 'react'
import { LeaderboardEntry } from '../hooks/useWebSocket'
import { ScoreBar } from './ScoreBar'

// Inline SVG sparkline from an array of values (last N total scores).
const Sparkline: React.FC<{ data: number[]; width?: number; height?: number }> = ({
  data, width = 80, height = 28,
}) => {
  if (!data || data.length < 2) return <span style={{ color: '#333', fontSize: 11 }}>—</span>
  const min = Math.min(...data)
  const max = Math.max(...data)
  const range = max - min || 1
  const pts = data.map((v, i) => {
    const x = (i / (data.length - 1)) * width
    const y = height - ((v - min) / range) * (height - 4) - 2
    return `${x.toFixed(1)},${y.toFixed(1)}`
  }).join(' ')
  return (
    <svg width={width} height={height} style={{ display: 'block' }}>
      <polyline points={pts} fill="none" stroke="#00d4ff" strokeWidth={1.5} strokeLinejoin="round" />
      {/* last point dot */}
      {(() => {
        const last = data[data.length - 1]
        const x = width
        const y = height - ((last - min) / range) * (height - 4) - 2
        return <circle cx={x} cy={y} r={2} fill="#00d4ff" />
      })()}
    </svg>
  )
}

interface Props {
  entries: LeaderboardEntry[]
  lastUpdated: Date | null
}

const fmtNS = (ns: number): string => {
  if (ns >= 1_000_000) return `${(ns / 1_000_000).toFixed(2)}ms`
  if (ns >= 1_000)     return `${(ns / 1_000).toFixed(1)}µs`
  return `${ns.toFixed(0)}ns`
}

const fmtTPS = (tps: number): string =>
  tps >= 1000 ? `${(tps / 1000).toFixed(1)}k` : tps.toFixed(0)

const MEDAL = ['🥇', '🥈', '🥉']

export const Leaderboard: React.FC<Props> = ({ entries, lastUpdated }) => {
  const [expanded, setExpanded] = useState<string | null>(null)

  return (
    <div style={{ fontFamily: "'JetBrains Mono', 'Fira Code', monospace", color: '#e0e0e0', padding: '24px 32px', maxWidth: 1100, margin: '0 auto' }}>
      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-end', marginBottom: 24 }}>
        <div>
          <h1 style={{ margin: 0, fontSize: 22, letterSpacing: 2, color: '#00d4ff' }}>
            QUANT TITANS — LIVE LEADERBOARD
          </h1>
          <div style={{ fontSize: 11, color: '#555', marginTop: 4 }}>
            IICPC Summer Hackathon 2026 · Distributed Bench Platform
          </div>
        </div>
        <div style={{ fontSize: 11, color: '#444' }}>
          {lastUpdated ? `Updated ${lastUpdated.toLocaleTimeString()}` : 'Waiting for data…'}
        </div>
      </div>

      {/* Score legend */}
      <div style={{ display: 'flex', gap: 24, marginBottom: 20, padding: '10px 16px', background: '#0d0d1a', borderRadius: 6, fontSize: 11 }}>
        {[
          { label: 'Throughput',   color: '#00d4ff', weight: '30%' },
          { label: 'Tail Latency', color: '#7c3aed', weight: '30%' },
          { label: 'Correctness',  color: '#16a34a', weight: '25%' },
          { label: 'Resilience',   color: '#ea580c', weight: '15%' },
        ].map(d => (
          <span key={d.label} style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <span style={{ width: 10, height: 10, background: d.color, borderRadius: 2, display: 'inline-block' }} />
            <span style={{ color: '#888' }}>{d.label}</span>
            <span style={{ color: '#555' }}>{d.weight}</span>
          </span>
        ))}
      </div>

      {/* Rows */}
      {entries.length === 0 && (
        <div style={{ textAlign: 'center', color: '#333', padding: 48, fontSize: 14 }}>
          Awaiting benchmark runs…
        </div>
      )}

      {entries.map(e => (
        <div key={e.session_id}
          onClick={() => setExpanded(expanded === e.session_id ? null : e.session_id)}
          style={{
            background: '#0a0a18',
            border: `1px solid ${expanded === e.session_id ? '#00d4ff33' : '#1e1e3a'}`,
            borderRadius: 8,
            padding: '14px 20px',
            marginBottom: 8,
            cursor: 'pointer',
            transition: 'border-color 0.2s'
          }}>
          {/* Row summary */}
          <div style={{ display: 'grid', gridTemplateColumns: '40px 1fr 90px 80px 80px 90px 90px 80px', gap: 12, alignItems: 'center' }}>
            <span style={{ fontSize: 20 }}>{MEDAL[e.rank - 1] ?? `#${e.rank}`}</span>
            <div>
              <div style={{ fontWeight: 700, fontSize: 15 }}>{e.team_name || e.session_id.slice(0, 12)}</div>
              {e.active_bots > 0 && (
                <div style={{ fontSize: 10, color: '#00d4ff88', marginTop: 2 }}>
                  🤖 {e.active_bots} bots active
                </div>
              )}
            </div>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end' }}>
              <Sparkline data={e.score_history ?? []} />
            </div>
            <div style={{ textAlign: 'right' }}>
              <div style={{ fontSize: 11, color: '#555' }}>p99</div>
              <div style={{ color: '#7c3aed', fontSize: 13 }}>{fmtNS(e.p99_ns)}</div>
            </div>
            <div style={{ textAlign: 'right' }}>
              <div style={{ fontSize: 11, color: '#555' }}>TPS</div>
              <div style={{ color: '#00d4ff', fontSize: 13 }}>{fmtTPS(e.tps)}</div>
            </div>
            <div style={{ textAlign: 'right' }}>
              <div style={{ fontSize: 11, color: '#555' }}>Accuracy</div>
              <div style={{ color: e.fill_accuracy >= 0.99 ? '#16a34a' : '#ea580c', fontSize: 13 }}>
                {(e.fill_accuracy * 100).toFixed(2)}%
              </div>
            </div>
            <div style={{ textAlign: 'right' }}>
              <div style={{ fontSize: 11, color: '#555' }}>Violations</div>
              <div style={{ color: e.price_time_violations === 0 ? '#16a34a' : '#ef4444', fontSize: 13 }}>
                {e.price_time_violations}
              </div>
            </div>
            <div style={{ textAlign: 'right' }}>
              <div style={{ fontSize: 11, color: '#555' }}>Score</div>
              <div style={{ fontSize: 18, fontWeight: 800, color: '#f0f0f0' }}>
                {e.total_score.toFixed(1)}
              </div>
            </div>
          </div>

          {/* Expanded detail */}
          {expanded === e.session_id && (
            <div style={{ marginTop: 16, paddingTop: 16, borderTop: '1px solid #1e1e3a' }}>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 24 }}>
                {/* Score breakdown */}
                <div>
                  <div style={{ fontSize: 11, color: '#555', marginBottom: 10, letterSpacing: 1 }}>SCORE BREAKDOWN</div>
                  <ScoreBar label="Throughput"   value={e.throughput_score}   color="#00d4ff" weight="30%" />
                  <ScoreBar label="Tail Latency" value={e.tail_latency_score} color="#7c3aed" weight="30%" />
                  <ScoreBar label="Correctness"  value={e.correctness_score}  color="#16a34a" weight="25%" />
                  <ScoreBar label="Resilience"   value={e.resilience_score}   color="#ea580c" weight="15%" />
                </div>
                {/* Latency details */}
                <div>
                  <div style={{ fontSize: 11, color: '#555', marginBottom: 10, letterSpacing: 1 }}>
                    LATENCY (eBPF KERNEL-LEVEL)
                  </div>
                  {[
                    { label: 'p50', value: e.p50_ns },
                    { label: 'p90', value: e.p90_ns },
                    { label: 'p99', value: e.p99_ns },
                    { label: 'p99.9', value: e.p999_ns },
                  ].map(({ label, value }) => (
                    <div key={label} style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, marginBottom: 4 }}>
                      <span style={{ color: '#555' }}>{label}</span>
                      <span style={{ color: '#c0c0c0' }}>{fmtNS(value)}</span>
                    </div>
                  ))}
                  <div style={{ marginTop: 10, paddingTop: 10, borderTop: '1px solid #1e1e3a' }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, marginBottom: 4 }}>
                      <span style={{ color: '#555' }}>Peak TPS</span>
                      <span style={{ color: '#00d4ff' }}>{fmtTPS(e.peak_tps)}/s</span>
                    </div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, marginBottom: 4 }}>
                      <span style={{ color: '#555' }}>Recovery time</span>
                      <span style={{ color: '#ea580c' }}>{e.recovery_time_ms.toFixed(0)}ms</span>
                    </div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12 }}>
                      <span style={{ color: '#555' }}>Chaos degradation</span>
                      <span style={{ color: '#ea580c' }}>{e.degradation_ratio.toFixed(2)}×</span>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>
      ))}
    </div>
  )
}

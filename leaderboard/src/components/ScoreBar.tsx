import React from 'react'

interface Props {
  label: string
  value: number      // 0–100
  color: string
  weight: string     // e.g. "30%"
}

export const ScoreBar: React.FC<Props> = ({ label, value, color, weight }) => (
  <div style={{ marginBottom: 6 }}>
    <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11, color: '#aaa', marginBottom: 2 }}>
      <span>{label} <span style={{ color: '#555' }}>({weight})</span></span>
      <span style={{ color, fontWeight: 700 }}>{value.toFixed(1)}</span>
    </div>
    <div style={{ background: '#1a1a2e', borderRadius: 3, height: 6, overflow: 'hidden' }}>
      <div style={{
        width: `${Math.min(100, value)}%`,
        height: '100%',
        background: color,
        borderRadius: 3,
        transition: 'width 0.4s ease'
      }} />
    </div>
  </div>
)

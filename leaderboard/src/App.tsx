import { useWebSocket } from './hooks/useWebSocket'
import { Leaderboard } from './components/Leaderboard'

const WS_URL = (import.meta as { env?: { VITE_WS_URL?: string } }).env?.VITE_WS_URL ?? 'ws://localhost:8082/ws/leaderboard'

export default function App() {
  const { entries, connState, lastUpdated } = useWebSocket(WS_URL)

  return (
    <div style={{
      minHeight: '100vh',
      background: 'linear-gradient(135deg, #020209 0%, #06061a 100%)',
      paddingTop: 16,
      paddingBottom: 48
    }}>
      {/* Connection status banner */}
      {connState !== 'connected' && (
        <div style={{
          background: connState === 'error' ? '#7f1d1d' : '#1c1c0a',
          color: connState === 'error' ? '#fca5a5' : '#fde68a',
          textAlign: 'center',
          padding: '6px 0',
          fontSize: 12,
          letterSpacing: 1
        }}>
          {connState === 'reconnecting' ? '⟳ RECONNECTING…' : connState === 'error' ? '✗ CONNECTION ERROR' : '◌ CONNECTING…'}
        </div>
      )}
      <Leaderboard entries={entries} lastUpdated={lastUpdated} />
    </div>
  )
}

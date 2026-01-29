/**
 * Connection status indicator.
 */

interface ConnectionStatusProps {
  isConnected: boolean;
}

export function ConnectionStatus({ isConnected }: ConnectionStatusProps) {
  return (
    <div
      className="connection-status"
      style={{
        display: 'flex',
        alignItems: 'center',
        gap: '8px',
      }}
    >
      <div
        className="status-dot"
        style={{
          width: '10px',
          height: '10px',
          borderRadius: '50%',
          backgroundColor: isConnected ? '#22c55e' : '#ef4444',
        }}
      />
      <span style={{ color: isConnected ? '#22c55e' : '#ef4444' }}>
        {isConnected ? 'Connected' : 'Disconnected'}
      </span>
    </div>
  );
}

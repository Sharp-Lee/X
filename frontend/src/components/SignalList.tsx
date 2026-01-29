/**
 * Signal list component showing recent trading signals.
 */

import type { Signal } from '../services/api';

interface SignalListProps {
  signals: Signal[];
  title?: string;
}

function formatPrice(price: number): string {
  return price.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

function formatTime(isoString: string): string {
  return new Date(isoString).toLocaleTimeString();
}

function getOutcomeColor(outcome: Signal['outcome']): string {
  switch (outcome) {
    case 'tp':
      return '#22c55e'; // green
    case 'sl':
      return '#ef4444'; // red
    default:
      return '#3b82f6'; // blue
  }
}

function getDirectionColor(direction: Signal['direction']): string {
  return direction === 'LONG' ? '#22c55e' : '#ef4444';
}

export function SignalList({ signals, title = 'Recent Signals' }: SignalListProps) {
  if (signals.length === 0) {
    return (
      <div className="signal-list">
        <h3>{title}</h3>
        <p className="no-signals">No signals yet</p>
      </div>
    );
  }

  return (
    <div className="signal-list">
      <h3>{title}</h3>
      <table>
        <thead>
          <tr>
            <th>Time</th>
            <th>Symbol</th>
            <th>Direction</th>
            <th>Entry</th>
            <th>TP</th>
            <th>SL</th>
            <th>MAE</th>
            <th>Outcome</th>
          </tr>
        </thead>
        <tbody>
          {signals.map((signal) => (
            <tr key={signal.id}>
              <td>{formatTime(signal.signal_time)}</td>
              <td>{signal.symbol}</td>
              <td style={{ color: getDirectionColor(signal.direction) }}>
                {signal.direction}
              </td>
              <td>{formatPrice(signal.entry_price)}</td>
              <td>{formatPrice(signal.tp_price)}</td>
              <td>{formatPrice(signal.sl_price)}</td>
              <td>{(signal.mae_ratio * 100).toFixed(1)}%</td>
              <td style={{ color: getOutcomeColor(signal.outcome) }}>
                {signal.outcome.toUpperCase()}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

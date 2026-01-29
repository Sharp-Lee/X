/**
 * Active position component showing current open positions with real-time MAE.
 */

import type { Signal } from '../services/api';

interface ActivePositionProps {
  signals: Signal[];
}

function formatPrice(price: number): string {
  return price.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

function getMaeColor(maeRatio: number): string {
  if (maeRatio < 0.3) return '#22c55e'; // green
  if (maeRatio < 0.6) return '#eab308'; // yellow
  return '#ef4444'; // red
}

export function ActivePosition({ signals }: ActivePositionProps) {
  if (signals.length === 0) {
    return (
      <div className="active-position">
        <h3>Active Positions</h3>
        <p className="no-positions">No active positions</p>
      </div>
    );
  }

  return (
    <div className="active-position">
      <h3>Active Positions ({signals.length})</h3>
      {signals.map((signal) => {
        const maePercent = signal.mae_ratio * 100;
        const mfePercent = signal.mfe_ratio * 100;
        const isLong = signal.direction === 'LONG';

        return (
          <div key={signal.id} className="position-card">
            <div className="position-header">
              <span className="symbol">{signal.symbol}</span>
              <span
                className="direction"
                style={{ color: isLong ? '#22c55e' : '#ef4444' }}
              >
                {signal.direction}
              </span>
            </div>

            <div className="position-prices">
              <div className="price-row">
                <span>Entry:</span>
                <span>{formatPrice(signal.entry_price)}</span>
              </div>
              <div className="price-row tp">
                <span>TP:</span>
                <span>{formatPrice(signal.tp_price)}</span>
              </div>
              <div className="price-row sl">
                <span>SL:</span>
                <span>{formatPrice(signal.sl_price)}</span>
              </div>
            </div>

            <div className="position-metrics">
              <div className="metric">
                <span>MAE:</span>
                <span style={{ color: getMaeColor(signal.mae_ratio) }}>
                  {maePercent.toFixed(1)}%
                </span>
              </div>
              <div className="metric">
                <span>MFE:</span>
                <span style={{ color: '#22c55e' }}>{mfePercent.toFixed(1)}%</span>
              </div>
            </div>

            {/* Progress bar showing position vs TP/SL */}
            <div className="progress-bar">
              <div
                className="progress-fill"
                style={{
                  width: `${Math.min(100, mfePercent * 2)}%`,
                  backgroundColor: mfePercent > 50 ? '#22c55e' : '#3b82f6',
                }}
              />
            </div>
          </div>
        );
      })}
    </div>
  );
}

/**
 * Streak indicator showing win/loss streak status.
 */

import { useEffect, useState } from 'react';
import { api } from '../services/api';
import type { Stats } from '../services/api';

interface StreakIndicatorProps {
  symbol?: string;
}

export function StreakIndicator({ symbol }: StreakIndicatorProps) {
  const [stats, setStats] = useState<Stats | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    const fetchStats = async () => {
      try {
        const data = await api.getStats({ symbol, days: 7 });
        setStats(data);
      } catch (e) {
        console.error('Failed to fetch stats:', e);
      } finally {
        setIsLoading(false);
      }
    };

    fetchStats();
    // Refresh every 30 seconds
    const interval = setInterval(fetchStats, 30000);
    return () => clearInterval(interval);
  }, [symbol]);

  if (isLoading || !stats) {
    return (
      <div className="streak-indicator">
        <h3>Statistics</h3>
        <p>Loading...</p>
      </div>
    );
  }

  const isWinning = stats.win_rate >= stats.breakeven_win_rate;

  return (
    <div className="streak-indicator">
      <h3>Statistics (7 Days)</h3>

      <div className="stat-grid">
        <div className="stat">
          <span className="stat-label">Total Signals</span>
          <span className="stat-value">{stats.total_signals}</span>
        </div>

        <div className="stat">
          <span className="stat-label">Win Rate</span>
          <span
            className="stat-value"
            style={{ color: isWinning ? '#22c55e' : '#ef4444' }}
          >
            {stats.win_rate.toFixed(1)}%
          </span>
        </div>

        <div className="stat">
          <span className="stat-label">Wins</span>
          <span className="stat-value" style={{ color: '#22c55e' }}>
            {stats.wins}
          </span>
        </div>

        <div className="stat">
          <span className="stat-label">Losses</span>
          <span className="stat-value" style={{ color: '#ef4444' }}>
            {stats.losses}
          </span>
        </div>

        <div className="stat">
          <span className="stat-label">Active</span>
          <span className="stat-value" style={{ color: '#3b82f6' }}>
            {stats.active}
          </span>
        </div>

        <div className="stat">
          <span className="stat-label">Breakeven</span>
          <span className="stat-value">{stats.breakeven_win_rate}%</span>
        </div>
      </div>

      {/* Win rate progress bar */}
      <div className="win-rate-bar">
        <div className="bar-container">
          <div
            className="bar-fill"
            style={{
              width: `${Math.min(100, stats.win_rate)}%`,
              backgroundColor: isWinning ? '#22c55e' : '#ef4444',
            }}
          />
          <div
            className="breakeven-marker"
            style={{ left: `${stats.breakeven_win_rate}%` }}
          />
        </div>
        <div className="bar-labels">
          <span>0%</span>
          <span>Breakeven: {stats.breakeven_win_rate}%</span>
          <span>100%</span>
        </div>
      </div>
    </div>
  );
}

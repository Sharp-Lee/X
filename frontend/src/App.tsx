/**
 * MSR Retest Capture - Trading Signal Dashboard
 */

import { useState } from 'react';
import { useSignals } from './hooks/useSignals';
import { SignalList } from './components/SignalList';
import { ActivePosition } from './components/ActivePosition';
import { StreakIndicator } from './components/StreakIndicator';
import { ConnectionStatus } from './components/ConnectionStatus';
import './App.css';

function App() {
  const [selectedSymbol, setSelectedSymbol] = useState<string | undefined>(undefined);
  const { signals, activeSignals, isLoading, error, isConnected } = useSignals(selectedSymbol);

  const symbols = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'XRPUSDT'];

  return (
    <div className="app">
      <header className="app-header">
        <h1>MSR Retest Capture</h1>
        <div className="header-right">
          <select
            value={selectedSymbol || 'all'}
            onChange={(e) =>
              setSelectedSymbol(e.target.value === 'all' ? undefined : e.target.value)
            }
            className="symbol-selector"
          >
            <option value="all">All Symbols</option>
            {symbols.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
          <ConnectionStatus isConnected={isConnected} />
        </div>
      </header>

      {error && <div className="error-banner">{error}</div>}
      {isLoading && <div className="loading-banner">Loading signals...</div>}

      <main className="app-main">
        <div className="left-panel">
          <ActivePosition signals={activeSignals} />
          <StreakIndicator symbol={selectedSymbol} />
        </div>

        <div className="right-panel">
          <SignalList
            signals={activeSignals}
            title="Active Signals"
          />
          <SignalList
            signals={signals.filter((s) => s.outcome !== 'active')}
            title="Recent Closed Signals"
          />
        </div>
      </main>

      <footer className="app-footer">
        <p>
          Strategy: Retest Capture | R:R = 1:4.42 | Required Win Rate: 81.5%
        </p>
      </footer>
    </div>
  );
}

export default App;

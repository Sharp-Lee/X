import { useState, useEffect } from 'react'
import { Providers } from '@/app/providers'
import { Header } from '@/components/layout/Header'
import { Footer } from '@/components/layout/Footer'
import { SignalTable } from '@/components/signals/SignalTable'
import { ActivePositions } from '@/components/signals/ActivePositions'
import { StatsGrid } from '@/components/stats/StatsGrid'
import { TradingPanel } from '@/components/trading/TradingPanel'
import { TradingChart } from '@/components/chart/TradingChart'
import { TimeframeTabs } from '@/features/multi-timeframe/TimeframeTabs'
import { TimeframeGrid } from '@/features/multi-timeframe/TimeframeGrid'
import { AnalyticsPage } from '@/features/analytics/AnalyticsPage'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { useSignals } from '@/hooks/useSignals'
import { useSystemStatus } from '@/hooks/useSystemStatus'
import { api } from '@/services/api'
import type { Signal } from '@/services/api'
import '@/styles/globals.css'

function Dashboard() {
  const [selectedSymbol, setSelectedSymbol] = useState<string | undefined>()
  const [selectedTimeframe, setSelectedTimeframe] = useState<string | undefined>()
  const [selectedStrategy, setSelectedStrategy] = useState<string | undefined>()
  const [strategies, setStrategies] = useState<string[]>([])
  const [selectedSignal, setSelectedSignal] = useState<Signal | null>(null)
  const [view, setView] = useState('dashboard')
  const { signals, activeSignals, isLoading, error, isConnected } = useSignals(selectedSymbol)
  const { isStarting, phaseLabel } = useSystemStatus()

  // Fetch strategy list once on mount
  useEffect(() => {
    api.getStrategies().then(setStrategies).catch(() => {})
  }, [])

  // Client-side strategy + timeframe filtering
  const applyFilters = (list: Signal[]) => {
    let result = list
    if (selectedTimeframe) {
      result = result.filter((s) => s.timeframe === selectedTimeframe)
    }
    if (selectedStrategy) {
      result = result.filter((s) => (s.strategy || 'msr_retest_capture') === selectedStrategy)
    }
    return result
  }

  const filteredSignals = applyFilters(signals)
  const filteredActive = applyFilters(activeSignals)

  const handleSignalClick = (signal: Signal) => {
    setSelectedSignal(signal)
  }

  return (
    <div className="flex min-h-screen flex-col bg-background text-foreground">
      <Header
        selectedSymbol={selectedSymbol}
        onSymbolChange={setSelectedSymbol}
        selectedTimeframe={selectedTimeframe}
        onTimeframeChange={setSelectedTimeframe}
        selectedStrategy={selectedStrategy}
        onStrategyChange={setSelectedStrategy}
        strategies={strategies}
        isConnected={isConnected}
      />

      {isStarting && (
        <div className="border-b border-warning bg-warning/20 px-6 py-2 text-warning text-sm flex items-center gap-2">
          <span className="h-2 w-2 rounded-full bg-warning animate-pulse" />
          Starting up: {phaseLabel}
        </div>
      )}

      {error && (
        <div className="border-b border-destructive bg-destructive/20 px-6 py-3 text-destructive">
          {error}
        </div>
      )}

      {isLoading && !isStarting && view === 'dashboard' && (
        <div className="border-b border-primary bg-primary/20 px-6 py-3 text-primary">
          Loading signals...
        </div>
      )}

      <Tabs value={view} onValueChange={setView} className="flex-1 flex flex-col">
        <div className="border-b bg-card px-6">
          <TabsList className="h-10">
            <TabsTrigger value="dashboard">Dashboard</TabsTrigger>
            <TabsTrigger value="analytics">Analytics</TabsTrigger>
          </TabsList>
        </div>

        <TabsContent value="dashboard" className="flex-1 mt-0">
          <main className="flex-1 p-6">
            <div className="grid gap-6 lg:grid-cols-[350px_1fr]">
              {/* Left Panel */}
              <div className="space-y-6 order-2 lg:order-1">
                <TradingPanel />
                <ActivePositions
                  signals={filteredActive}
                  onSelect={handleSignalClick}
                />
                <StatsGrid symbol={selectedSymbol} />
              </div>

              {/* Right Panel */}
              <div className="space-y-6 order-1 lg:order-2">
                {/* Chart */}
                {selectedSignal && (
                  <TradingChart
                    symbol={selectedSignal.symbol}
                    timeframe={selectedSignal.timeframe}
                    selectedSignal={selectedSignal}
                  />
                )}

                {/* Timeframe Overview */}
                <TimeframeGrid
                  signals={signals}
                  selectedTimeframe={selectedTimeframe}
                  onTimeframeSelect={(tf) => {
                    setSelectedTimeframe(selectedTimeframe === tf ? undefined : tf)
                  }}
                />

                {/* Signal Tables with Tabs */}
                <TimeframeTabs
                  signals={filteredActive}
                  onSignalClick={handleSignalClick}
                />

                {/* Recent Closed Signals */}
                <SignalTable
                  signals={filteredSignals.filter((s) => s.outcome !== 'active').slice(0, 20)}
                  title="Recent Closed Signals"
                  onSignalClick={handleSignalClick}
                />
              </div>
            </div>
          </main>
        </TabsContent>

        <TabsContent value="analytics" className="flex-1 mt-0">
          <AnalyticsPage />
        </TabsContent>
      </Tabs>

      <Footer />
    </div>
  )
}

function App() {
  return (
    <Providers>
      <Dashboard />
    </Providers>
  )
}

export default App

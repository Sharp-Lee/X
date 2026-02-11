import { useState } from 'react'
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
import type { Signal } from '@/services/api'
import '@/styles/globals.css'

function Dashboard() {
  const [selectedSymbol, setSelectedSymbol] = useState<string | undefined>()
  const [selectedSignal, setSelectedSignal] = useState<Signal | null>(null)
  const [view, setView] = useState('dashboard')
  const { signals, activeSignals, isLoading, error, isConnected } = useSignals(selectedSymbol)

  const handleSignalClick = (signal: Signal) => {
    setSelectedSignal(signal)
  }

  return (
    <div className="flex min-h-screen flex-col bg-background text-foreground">
      <Header
        selectedSymbol={selectedSymbol}
        onSymbolChange={setSelectedSymbol}
        isConnected={isConnected}
      />

      {error && (
        <div className="border-b border-destructive bg-destructive/20 px-6 py-3 text-destructive">
          {error}
        </div>
      )}

      {isLoading && view === 'dashboard' && (
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
                  signals={activeSignals}
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
                  onTimeframeSelect={(tf) => {
                    console.log('Selected timeframe:', tf)
                  }}
                />

                {/* Signal Tables with Tabs */}
                <TimeframeTabs
                  signals={activeSignals}
                  onSignalClick={handleSignalClick}
                />

                {/* Recent Closed Signals */}
                <SignalTable
                  signals={signals.filter((s) => s.outcome !== 'active').slice(0, 20)}
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

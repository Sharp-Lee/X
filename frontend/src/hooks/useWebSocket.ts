/**
 * WebSocket hook for real-time updates.
 */

import { useEffect, useRef, useState, useCallback } from 'react';

const WS_URL = import.meta.env.VITE_WS_URL || 'ws://localhost:8000/ws';

export interface WSMessage {
  type: 'signal' | 'mae_update' | 'outcome' | 'status' | 'connected' | 'ping' | 'pong';
  data: Record<string, unknown>;
  timestamp: string;
}

export interface UseWebSocketOptions {
  onSignal?: (data: Record<string, unknown>) => void;
  onMaeUpdate?: (data: { signal_id: string; mae_ratio: number; mfe_ratio: number }) => void;
  onOutcome?: (data: { signal_id: string; outcome: string; exit_price: number }) => void;
  onStatus?: (data: Record<string, unknown>) => void;
}

export function useWebSocket(options: UseWebSocketOptions = {}) {
  const [isConnected, setIsConnected] = useState(false);
  const [lastMessage, setLastMessage] = useState<WSMessage | null>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimeoutRef = useRef<number | null>(null);

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      return;
    }

    const ws = new WebSocket(WS_URL);

    ws.onopen = () => {
      console.log('WebSocket connected');
      setIsConnected(true);
    };

    ws.onclose = () => {
      console.log('WebSocket disconnected');
      setIsConnected(false);

      // Reconnect after delay
      reconnectTimeoutRef.current = window.setTimeout(() => {
        connect();
      }, 3000);
    };

    ws.onerror = (error) => {
      console.error('WebSocket error:', error);
    };

    ws.onmessage = (event) => {
      try {
        const message: WSMessage = JSON.parse(event.data);
        setLastMessage(message);

        switch (message.type) {
          case 'signal':
            options.onSignal?.(message.data);
            break;
          case 'mae_update':
            options.onMaeUpdate?.(message.data as { signal_id: string; mae_ratio: number; mfe_ratio: number });
            break;
          case 'outcome':
            options.onOutcome?.(message.data as { signal_id: string; outcome: string; exit_price: number });
            break;
          case 'status':
            options.onStatus?.(message.data);
            break;
          case 'ping':
            ws.send(JSON.stringify({ type: 'pong', data: {} }));
            break;
        }
      } catch (e) {
        console.error('Failed to parse WebSocket message:', e);
      }
    };

    wsRef.current = ws;
  }, [options]);

  useEffect(() => {
    connect();

    return () => {
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
      wsRef.current?.close();
    };
  }, [connect]);

  const send = useCallback((data: object) => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify(data));
    }
  }, []);

  return {
    isConnected,
    lastMessage,
    send,
  };
}

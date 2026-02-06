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
  const isUnmountingRef = useRef(false);
  // Store options in ref to avoid re-creating connect callback
  const optionsRef = useRef(options);
  optionsRef.current = options;

  const connect = useCallback(() => {
    // Don't connect if unmounting
    if (isUnmountingRef.current) {
      return;
    }

    // Prevent multiple connections
    if (wsRef.current?.readyState === WebSocket.OPEN ||
        wsRef.current?.readyState === WebSocket.CONNECTING) {
      return;
    }

    const ws = new WebSocket(WS_URL);

    ws.onopen = () => {
      console.log('WebSocket connected');
      setIsConnected(true);
    };

    ws.onclose = (event) => {
      console.log('WebSocket disconnected', event.code);
      setIsConnected(false);
      wsRef.current = null;

      // Only reconnect if not intentionally closed and not unmounting
      if (!isUnmountingRef.current && event.code !== 1000) {
        reconnectTimeoutRef.current = window.setTimeout(() => {
          connect();
        }, 3000);
      }
    };

    ws.onerror = () => {
      // Don't log error if we're unmounting (expected during StrictMode)
      if (!isUnmountingRef.current) {
        console.warn('WebSocket connection error');
      }
    };

    ws.onmessage = (event) => {
      try {
        const message: WSMessage = JSON.parse(event.data);
        setLastMessage(message);

        // Use ref to get current options without re-creating callback
        const opts = optionsRef.current;
        switch (message.type) {
          case 'signal':
            opts.onSignal?.(message.data);
            break;
          case 'mae_update':
            opts.onMaeUpdate?.(message.data as { signal_id: string; mae_ratio: number; mfe_ratio: number });
            break;
          case 'outcome':
            opts.onOutcome?.(message.data as { signal_id: string; outcome: string; exit_price: number });
            break;
          case 'status':
            opts.onStatus?.(message.data);
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
  }, []); // No dependencies - stable callback

  useEffect(() => {
    isUnmountingRef.current = false;
    connect();

    return () => {
      isUnmountingRef.current = true;
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
        reconnectTimeoutRef.current = null;
      }
      if (wsRef.current) {
        wsRef.current.close(1000, 'Component unmounting');
        wsRef.current = null;
      }
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

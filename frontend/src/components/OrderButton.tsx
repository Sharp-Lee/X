/**
 * Order button component for manual trading.
 */

import { useState } from 'react';
import { api } from '../services/api';
import type { OrderRequest } from '../services/api';

interface OrderButtonProps {
  symbol: string;
  direction: 'LONG' | 'SHORT';
  onOrderPlaced?: (success: boolean, message: string) => void;
}

export function OrderButton({
  symbol,
  direction,
  onOrderPlaced,
}: OrderButtonProps) {
  const [isLoading, setIsLoading] = useState(false);
  const [quantity, setQuantity] = useState('0.01');

  const handleOrder = async () => {
    if (isLoading) return;

    setIsLoading(true);
    try {
      const order: OrderRequest = {
        symbol,
        side: direction === 'LONG' ? 'buy' : 'sell',
        quantity: parseFloat(quantity),
      };

      const response = await api.placeOrder(order);
      onOrderPlaced?.(response.success, response.message);
    } catch (e) {
      onOrderPlaced?.(false, e instanceof Error ? e.message : 'Order failed');
    } finally {
      setIsLoading(false);
    }
  };

  const isLong = direction === 'LONG';
  const buttonColor = isLong ? '#22c55e' : '#ef4444';

  return (
    <div className="order-button">
      <input
        type="number"
        value={quantity}
        onChange={(e) => setQuantity(e.target.value)}
        step="0.01"
        min="0.001"
        className="quantity-input"
        disabled={isLoading}
      />
      <button
        onClick={handleOrder}
        disabled={isLoading}
        style={{
          backgroundColor: buttonColor,
          opacity: isLoading ? 0.5 : 1,
        }}
      >
        {isLoading ? 'Placing...' : `${direction} ${symbol}`}
      </button>
    </div>
  );
}

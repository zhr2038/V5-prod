import { useEffect, useMemo, useRef, useState } from 'react';

export type DataPulseDirection = 'up' | 'down' | 'changed' | null;

function stableKey(value: unknown) {
  if (value === null || value === undefined) return '';
  if (typeof value === 'number') return Number.isFinite(value) ? String(value) : '';
  if (typeof value === 'string' || typeof value === 'boolean') return String(value);
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function pulseDirection(previous: unknown, current: unknown): DataPulseDirection {
  const prevNumber = Number(previous);
  const nextNumber = Number(current);
  if (Number.isFinite(prevNumber) && Number.isFinite(nextNumber) && prevNumber !== nextNumber) {
    return nextNumber > prevNumber ? 'up' : 'down';
  }
  return 'changed';
}

export function usePreviousValue<T>(value: T) {
  const [previous, setPrevious] = useState<T>(value);
  useEffect(() => {
    setPrevious(value);
  }, [value]);
  return previous;
}

export function useDataPulse(
  value: unknown,
  options: { durationMs?: number; disabled?: boolean } = {}
) {
  const { durationMs = 560, disabled = false } = options;
  const [state, setState] = useState<{
    active: boolean;
    direction: DataPulseDirection;
    key: number;
  }>({ active: false, direction: null, key: 0 });
  const initializedRef = useRef(false);
  const previousValueRef = useRef<unknown>(value);
  const previousKeyRef = useRef(stableKey(value));
  const currentKey = useMemo(() => stableKey(value), [value]);

  useEffect(() => {
    if (disabled) {
      previousValueRef.current = value;
      previousKeyRef.current = currentKey;
      return undefined;
    }

    if (!initializedRef.current) {
      initializedRef.current = true;
      previousValueRef.current = value;
      previousKeyRef.current = currentKey;
      return undefined;
    }

    if (previousKeyRef.current === currentKey) return undefined;

    const direction = pulseDirection(previousValueRef.current, value);
    previousValueRef.current = value;
    previousKeyRef.current = currentKey;
    setState((prev) => ({ active: true, direction, key: prev.key + 1 }));
    const timer = window.setTimeout(() => {
      setState((prev) => ({ ...prev, active: false, direction: null }));
    }, durationMs);
    return () => window.clearTimeout(timer);
  }, [currentKey, disabled, durationMs, value]);

  return {
    ...state,
    className: state.active ? 'data-pulse-active' : '',
    dataPulse: state.active ? state.direction || 'changed' : undefined,
  };
}

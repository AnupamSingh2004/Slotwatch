type Props = { walLagBytes: number; thresholdBytes: number };

// Only renders when lag exceeds the configured threshold.
export function WalLagBanner({ walLagBytes, thresholdBytes }: Props) {
  if (walLagBytes <= thresholdBytes) return null;
  return (
    <div className="bg-yellow-900/40 border border-yellow-600 rounded px-4 py-2 text-yellow-300 text-sm">
      ⚠ WAL lag growing — {walLagBytes.toLocaleString()} bytes behind
    </div>
  );
}

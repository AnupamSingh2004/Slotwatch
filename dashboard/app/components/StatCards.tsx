type Props = {
  eventsTotal: number;
  walLagBytes: number;
  currentLSN: string;
};

function Card({ label, value }: { label: string; value: string }) {
  return (
    <div className="bg-gray-900 rounded-lg p-4 border border-gray-800">
      <div className="text-2xl font-bold">{value}</div>
      <div className="text-sm text-gray-400 mt-1">{label}</div>
    </div>
  );
}

export function StatCards({ eventsTotal, walLagBytes, currentLSN }: Props) {
  return (
    <div className="grid grid-cols-3 gap-4">
      <Card label="events published" value={eventsTotal.toLocaleString()} />
      <Card label="WAL lag" value={`${walLagBytes.toLocaleString()} bytes`} />
      <Card label="current LSN" value={currentLSN || "—"} />
    </div>
  );
}

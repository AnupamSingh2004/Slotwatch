type Event = {
  op: string;
  table: string;
  lsn: number;
  ts: string;
};

type Props = { events: Event[] };

// Colour-code operations so INSERT/UPDATE/DELETE are scannable at a glance.
const opColor: Record<string, string> = {
  INSERT: "text-green-400",
  UPDATE: "text-blue-400",
  DELETE: "text-red-400",
};

export function EventsTable({ events }: Props) {
  if (events.length === 0) {
    return <p className="text-gray-500 text-sm">No events yet.</p>;
  }
  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="text-gray-500 text-left border-b border-gray-800">
          <th className="pb-2 pr-6">Op</th>
          <th className="pb-2 pr-6">Table</th>
          <th className="pb-2 pr-6">LSN</th>
          <th className="pb-2">Time</th>
        </tr>
      </thead>
      <tbody>
        {events.map((ev, i) => (
          <tr key={i} className="border-b border-gray-900 hover:bg-gray-900/40">
            <td className={`py-2 pr-6 font-medium ${opColor[ev.op] ?? ""}`}>{ev.op}</td>
            <td className="py-2 pr-6 text-gray-300">{ev.table}</td>
            <td className="py-2 pr-6 text-gray-500">{ev.lsn}</td>
            <td className="py-2 text-gray-500">{new Date(ev.ts).toLocaleTimeString()}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

"use client";

import { useEffect, useState } from "react";
import { PipelineHeader } from "./components/PipelineHeader";
import { StatCards } from "./components/StatCards";
import { WalLagBanner } from "./components/WalLagBanner";
import { ConnectionErrorBanner } from "./components/ConnectionErrorBanner";
import { EventsTable } from "./components/EventsTable";

const POLL_INTERVAL_MS = 5000;
// Warn when WAL lag exceeds 10MB — matches the default in slotwatch.yml.example.
const WAL_LAG_WARN_BYTES = 10 * 1024 * 1024;

type Status = {
  pipeline: string;
  current_lsn: string;
  wal_lag_bytes: number;
  kafka_connected: boolean;
  postgres_connected: boolean;
};

type Event = { op: string; table: string; lsn: number; ts: string };

export default function Home() {
  const [status, setStatus] = useState<Status | null>(null);
  const [events, setEvents] = useState<Event[]>([]);
  const [failCount, setFailCount] = useState(0);
  // Track total events seen across polls — the ring buffer only holds the last 50,
  // so we accumulate a running total separately.
  const [totalEvents, setTotalEvents] = useState(0);

  useEffect(() => {
    let cancelled = false;

    // Recursive setTimeout instead of setInterval: next poll only starts after
    // the current fetch completes, preventing overlapping requests if the API is slow.
    const poll = async () => {
      try {
        const [statusRes, eventsRes] = await Promise.all([
          fetch("/api/status"),
          fetch("/api/events"),
        ]);
        if (!statusRes.ok || !eventsRes.ok) throw new Error("non-ok response");

        const s: Status = await statusRes.json();
        const e: { events: Event[] } = await eventsRes.json();

        if (!cancelled) {
          setStatus(s);
          setEvents(e.events ?? []);
          setTotalEvents((prev) => prev + (e.events?.length ?? 0));
          setFailCount(0);
        }
      } catch {
        if (!cancelled) setFailCount((n) => n + 1);
      } finally {
        if (!cancelled) setTimeout(poll, POLL_INTERVAL_MS);
      }
    };

    poll();
    return () => {
      cancelled = true;
    };
  }, []);

  const handlePause = () => fetch("/api/pipeline/pause", { method: "POST" });
  const handleResume = () => fetch("/api/pipeline/resume", { method: "POST" });

  return (
    <main>
      <PipelineHeader
        status={status?.pipeline ?? "connecting..."}
        onPause={handlePause}
        onResume={handleResume}
      />
      <div className="p-6 space-y-4">
        <ConnectionErrorBanner failCount={failCount} />
        {status && (
          <>
            <StatCards
              eventsTotal={totalEvents}
              walLagBytes={status.wal_lag_bytes}
              currentLSN={status.current_lsn}
            />
            <WalLagBanner
              walLagBytes={status.wal_lag_bytes}
              thresholdBytes={WAL_LAG_WARN_BYTES}
            />
          </>
        )}
        <div>
          <h2 className="text-sm font-semibold text-gray-400 mb-3">
            Recent Events (last 50)
          </h2>
          <EventsTable events={events} />
        </div>
      </div>
    </main>
  );
}

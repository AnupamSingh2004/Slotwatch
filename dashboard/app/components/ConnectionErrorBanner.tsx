type Props = { failCount: number };

// Shows a red banner after 3 consecutive poll failures so stale data is never shown silently.
export function ConnectionErrorBanner({ failCount }: Props) {
  if (failCount < 3) return null;
  return (
    <div className="bg-red-900/40 border border-red-600 rounded px-4 py-3 text-red-300 text-sm">
      Cannot reach Slotwatch API — is the service running?
    </div>
  );
}

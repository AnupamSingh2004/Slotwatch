"use client";

type Props = {
  status: string;
  onPause: () => void;
  onResume: () => void;
};

export function PipelineHeader({ status, onPause, onResume }: Props) {
  const isPaused = status === "paused";
  // Status dot colour: green = running, yellow = paused, red = anything else (error/connecting)
  const dot =
    status === "running"
      ? "bg-green-400"
      : status === "paused"
      ? "bg-yellow-400"
      : "bg-red-500";

  return (
    <header className="flex items-center justify-between px-6 py-4 border-b border-gray-800">
      <h1 className="text-xl font-bold tracking-tight">Slotwatch</h1>
      <div className="flex items-center gap-4">
        <span className="flex items-center gap-2 text-sm">
          <span className={`w-2 h-2 rounded-full ${dot}`} />
          {status}
        </span>
        <button
          onClick={isPaused ? onResume : onPause}
          className="px-3 py-1 text-sm rounded border border-gray-600 hover:border-gray-400 transition-colors"
        >
          {isPaused ? "Resume" : "Pause"}
        </button>
      </div>
    </header>
  );
}

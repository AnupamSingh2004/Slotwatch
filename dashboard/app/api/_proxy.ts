// Base URL of the Go API — server-side only, never sent to the browser.
const API_BASE = process.env.SLOTWATCH_API_URL ?? "http://localhost:8080";

export async function proxyGet(path: string): Promise<Response> {
  const res = await fetch(`${API_BASE}${path}`, { cache: "no-store" });
  const body = await res.text();
  return new Response(body, {
    status: res.status,
    headers: { "Content-Type": res.headers.get("Content-Type") ?? "application/json" },
  });
}

export async function proxyPost(path: string): Promise<Response> {
  const res = await fetch(`${API_BASE}${path}`, { method: "POST", cache: "no-store" });
  const body = await res.text();
  return new Response(body, {
    status: res.status,
    headers: { "Content-Type": "application/json" },
  });
}

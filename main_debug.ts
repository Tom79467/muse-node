// DEBUG VARIANT: VLESS-WS + self-diagnostics via /selftest and /lastws
const uuidStr = Deno.env.get("UUID") || "49269dfb-53e6-4748-b3a1-3f18378d38eb";
const uuidBytes = (() => {
  const hex = uuidStr.replace(/-/g, "");
  const b = new Uint8Array(16);
  for (let i = 0; i < 16; i++) b[i] = parseInt(hex.substr(i * 2, 2), 16);
  return b;
})();
let lastWs = "no ws attempt yet";

function eqUUID(a: Uint8Array): boolean {
  if (a.length !== 16) return false;
  for (let i = 0; i < 16; i++) if (a[i] !== uuidBytes[i]) return false;
  return true;
}
function b64urlDecode(s: string): Uint8Array {
  s = s.replace(/-/g, "+").replace(/_/g, "/");
  while (s.length % 4) s += "=";
  const bin = atob(s);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

function handleWs(req: Request): Response {
  const { socket, response } = Deno.upgradeWebSocket(req);
  let remote: Deno.TcpConn | null = null;
  let writer: WritableStreamDefaultWriter<Uint8Array> | null = null;
  let headerSent = false;
  let version = 0;
  let connecting = false;
  const pending: Uint8Array[] = [];

  const processChunk = async (chunk: Uint8Array) => {
    if (writer) { try { await writer.write(chunk); } catch { /* closed */ } return; }
    if (connecting) { pending.push(chunk); return; }
    try {
      version = chunk[0];
      if (!eqUUID(chunk.subarray(1, 17))) { lastWs = "uuid mismatch"; socket.close(); return; }
      let idx = 17;
      const optLen = chunk[idx]; idx += 1 + optLen;
      const cmd = chunk[idx]; idx += 1;
      if (cmd !== 1) { lastWs = "cmd not tcp: " + cmd; socket.close(); return; }
      const port = (chunk[idx] << 8) | chunk[idx + 1]; idx += 2;
      const atyp = chunk[idx]; idx += 1;
      let host = "";
      if (atyp === 1) { host = Array.from(chunk.subarray(idx, idx + 4)).join("."); idx += 4; }
      else if (atyp === 2) { const len = chunk[idx]; idx += 1; host = new TextDecoder().decode(chunk.subarray(idx, idx + len)); idx += len; }
      else if (atyp === 3) { const parts: string[] = []; for (let i = 0; i < 8; i++) { parts.push(((chunk[idx] << 8) | chunk[idx + 1]).toString(16)); idx += 2; } host = parts.join(":"); }
      else { lastWs = "bad atyp " + atyp; socket.close(); return; }
      lastWs = `parsed host=${host} port=${port}, connecting...`;
      connecting = true;
      remote = await Deno.connect({ hostname: host, port });
      lastWs = `connected to ${host}:${port}`;
      writer = remote.writable.getWriter();
      const rest = chunk.subarray(idx);
      if (rest.length) await writer.write(rest);
      for (const p of pending) await writer.write(p);
      pending.length = 0;
      (async () => {
        const reader = remote!.readable.getReader();
        try {
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            if (!headerSent) {
              headerSent = true;
              const out = new Uint8Array(value.length + 2);
              out[0] = version; out[1] = 0; out.set(value, 2);
              socket.send(out);
            } else socket.send(value);
          }
        } catch (e) { lastWs = "read loop err: " + (e instanceof Error ? e.message : String(e)); }
        try { socket.close(); } catch { /* ignore */ }
      })();
    } catch (e) {
      lastWs = "processChunk err: " + (e instanceof Error ? e.message : String(e));
      try { socket.close(); } catch { /* ignore */ }
    }
  };

  socket.onopen = () => {
    const early = req.headers.get("sec-websocket-protocol");
    if (early) { try { processChunk(b64urlDecode(early)); } catch { /* ignore */ } }
  };
  socket.onmessage = (e) => {
    if (e.data instanceof ArrayBuffer) processChunk(new Uint8Array(e.data));
    else if (e.data instanceof Blob) e.data.arrayBuffer().then((ab) => processChunk(new Uint8Array(ab)));
    else if (typeof e.data === "string") processChunk(new TextEncoder().encode(e.data));
  };
  socket.onclose = () => { try { remote?.close(); } catch { /* ignore */ } };
  socket.onerror = () => { try { remote?.close(); } catch { /* ignore */ } };
  return response;
}

async function selftest(): Promise<string> {
  const lines: string[] = [];
  try {
    const c = await Deno.connect({ hostname: "ipinfo.io", port: 80 });
    const w = c.writable.getWriter();
    await w.write(new TextEncoder().encode("GET /json HTTP/1.0\r\nHost: ipinfo.io\r\n\r\n"));
    const r = c.readable.getReader();
    const { value } = await r.read();
    lines.push("connect ipinfo.io:80 OK, first bytes: " + new TextDecoder().decode(value?.subarray(0, 60) || new Uint8Array()));
    c.close();
  } catch (e) { lines.push("connect ipinfo.io:80 FAIL: " + (e instanceof Error ? e.message : String(e))); }
  try {
    const c2 = await Deno.connect({ hostname: "example.com", port: 443 });
    lines.push("connect example.com:443 OK");
    c2.close();
  } catch (e) { lines.push("connect example.com:443 FAIL: " + (e instanceof Error ? e.message : String(e))); }
  try {
    const c3 = await Deno.connect({ hostname: "1.1.1.1", port: 443 });
    lines.push("connect 1.1.1.1:443 OK");
    c3.close();
  } catch (e) { lines.push("connect 1.1.1.1:443 FAIL: " + (e instanceof Error ? e.message : String(e))); }
  return lines.join("\n");
}

Deno.serve(async (req: Request) => {
  const url = new URL(req.url);
  if (url.pathname === "/selftest") return new Response(await selftest(), { headers: { "content-type": "text/plain" } });
  if (url.pathname === "/lastws") return new Response(lastWs, { headers: { "content-type": "text/plain" } });
  if (req.headers.get("upgrade")?.toLowerCase() === "websocket") return handleWs(req);
  return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
});

// VLESS over WebSocket on Deno Deploy (TCP only). Production build.
// NOTE: never touch the original Request object after upgrade (throws "Request closed" on this runtime).
const uuidStr = Deno.env.get("UUID") || "49269dfb-53e6-4748-b3a1-3f18378d38eb";
const uuidBytes = (() => {
  const hex = uuidStr.replace(/-/g, "");
  const b = new Uint8Array(16);
  for (let i = 0; i < 16; i++) b[i] = parseInt(hex.substr(i * 2, 2), 16);
  return b;
})();

function eqUUID(a: Uint8Array): boolean {
  if (a.length !== 16) return false;
  for (let i = 0; i < 16; i++) if (a[i] !== uuidBytes[i]) return false;
  return true;
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
    if (writer) {
      try { await writer.write(chunk); } catch { /* closed */ }
      return;
    }
    if (connecting) { pending.push(chunk); return; }
    try {
      version = chunk[0];
      if (!eqUUID(chunk.subarray(1, 17))) { socket.close(); return; }
      let idx = 17;
      const optLen = chunk[idx]; idx += 1 + optLen;
      const cmd = chunk[idx]; idx += 1;
      if (cmd !== 1) { socket.close(); return; } // TCP only
      const port = (chunk[idx] << 8) | chunk[idx + 1]; idx += 2;
      const atyp = chunk[idx]; idx += 1;
      let host = "";
      if (atyp === 1) {
        host = Array.from(chunk.subarray(idx, idx + 4)).join("."); idx += 4;
      } else if (atyp === 2) {
        const len = chunk[idx]; idx += 1;
        host = new TextDecoder().decode(chunk.subarray(idx, idx + len)); idx += len;
      } else if (atyp === 3) {
        const parts: string[] = [];
        for (let i = 0; i < 8; i++) { parts.push(((chunk[idx] << 8) | chunk[idx + 1]).toString(16)); idx += 2; }
        host = parts.join(":");
      } else { socket.close(); return; }

      connecting = true;
      remote = await Deno.connect({ hostname: host, port });
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
            } else {
              socket.send(value);
            }
          }
        } catch { /* ignore */ }
        try { socket.close(); } catch { /* ignore */ }
      })();
    } catch {
      try { socket.close(); } catch { /* ignore */ }
    }
  };

  socket.addEventListener("message", (e) => {
    const d = (e as MessageEvent).data;
    if (d instanceof ArrayBuffer) processChunk(new Uint8Array(d));
    else if (d instanceof Blob) d.arrayBuffer().then((ab) => processChunk(new Uint8Array(ab)));
    else if (typeof d === "string") processChunk(new TextEncoder().encode(d));
  });
  socket.addEventListener("close", () => { try { remote?.close(); } catch { /* ignore */ } });
  socket.addEventListener("error", () => { try { remote?.close(); } catch { /* ignore */ } });
  return response;
}

Deno.serve((req: Request) => {
  if (req.headers.get("upgrade")?.toLowerCase() === "websocket") return handleWs(req);
  return new Response("ok", { status: 200, headers: { "content-type": "text/plain" } });
});

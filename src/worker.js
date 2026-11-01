// Billiard online server: a Cloudflare Worker that serves the game (public/)
// and relays online-play state through one Durable Object per room.
//
// Protocol (JSON over WebSocket at /ws?room=<name>&peer=<id>):
//   server -> client  {t:"welcome", peer, peers:[{peer, p}]}   on connect: everyone already here
//   client -> server  {t:"u", d:{...}}                          merge d into my presence (null deletes a key)
//   server -> client  {t:"u", peer, d:{...}}                    someone's presence changed (or they joined)
//   server -> client  {t:"leave", peer}                         someone left
//   "ping" -> "pong"                                             keep-alive, answered without waking the object
import { DurableObject } from "cloudflare:workers";

const ROOM_RE = /^[a-z0-9][a-z0-9_.-]{0,47}$/;
const PEER_RE = /^[a-z0-9]{8,32}$/;
const MAX_MSG = 8192;   // bytes of one message / one presence object
const MAX_PEERS = 64;   // connections per room

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname !== "/ws") return env.ASSETS.fetch(request);

    if (request.method !== "GET" || request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected a WebSocket upgrade", { status: 426 });
    }
    // Only pages served from this same site may connect.
    const origin = request.headers.get("Origin");
    if (origin) {
      let host = "";
      try { host = new URL(origin).host; } catch { /* bad origin */ }
      if (host !== url.host) return new Response("Forbidden", { status: 403 });
    }
    const room = url.searchParams.get("room") || "";
    const peer = url.searchParams.get("peer") || "";
    if (!ROOM_RE.test(room) || !PEER_RE.test(peer)) return new Response("Bad request", { status: 400 });

    return env.ROOMS.getByName(room).fetch(request);
  },
};

export class Room extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
  }

  peerOf(ws) {
    const a = ws.deserializeAttachment();
    if (a && a.peer) return a.peer;
    try { return this.ctx.getTags(ws)[0] || null; } catch { return null; }
  }

  // Sockets that are still open, except `skip`.
  live(skip) {
    return this.ctx.getWebSockets().filter((ws) => ws !== skip && ws.readyState === 1);
  }

  async fetch(request) {
    const peer = new URL(request.url).searchParams.get("peer");

    // A page that reconnects replaces its own old socket (no "leave" is sent for it).
    for (const old of this.ctx.getWebSockets(peer)) {
      try { old.close(4000, "replaced"); } catch { /* already closed */ }
    }
    if (this.live().filter((ws) => this.peerOf(ws) !== peer).length >= MAX_PEERS) {
      return new Response("Room is full", { status: 429 });
    }

    const [client, server] = Object.values(new WebSocketPair());
    this.ctx.acceptWebSocket(server, [peer]);
    server.serializeAttachment({ peer, p: {} });

    const peers = [];
    for (const ws of this.live(server)) {
      const a = ws.deserializeAttachment();
      if (a && a.peer && a.peer !== peer) peers.push({ peer: a.peer, p: a.p || {} });
    }
    server.send(JSON.stringify({ t: "welcome", peer, peers }));
    this.broadcast({ t: "u", peer, d: {} }, server);
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, msg) {
    if (typeof msg !== "string" || msg.length > MAX_MSG) return;
    let m;
    try { m = JSON.parse(msg); } catch { return; }
    if (!m || m.t !== "u" || !m.d || typeof m.d !== "object" || Array.isArray(m.d)) return;

    const a = ws.deserializeAttachment();
    if (!a || !a.peer) return;
    const p = { ...(a.p || {}) };
    for (const [k, v] of Object.entries(m.d)) {
      if (v === null) delete p[k];
      else p[k] = v;
    }
    if (JSON.stringify(p).length > MAX_MSG) return;
    a.p = p;
    ws.serializeAttachment(a);
    this.broadcast({ t: "u", peer: a.peer, d: m.d }, ws);
  }

  async webSocketClose(ws) { this.gone(ws); }
  async webSocketError(ws) { this.gone(ws); }

  gone(ws) {
    const peer = this.peerOf(ws);
    if (!peer) return;
    // Still connected through a newer socket (a reconnect)? Then nobody left.
    if (this.live(ws).some((w) => this.peerOf(w) === peer)) return;
    this.broadcast({ t: "leave", peer }, ws);
  }

  broadcast(obj, skip) {
    const s = JSON.stringify(obj);
    for (const ws of this.live(skip)) {
      try { ws.send(s); } catch { /* closing */ }
    }
  }
}

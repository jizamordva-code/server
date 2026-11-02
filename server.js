'use strict';
/**
 * КИБЕР_Догонялки — сервер (WebSocket + раздача веб-версии для Telegram).
 * Один сервер обслуживает и Android-клиент, и Telegram Mini App.
 * Режимы: classic (догонялки), ctf (захват флага), team (догоня⚚ки).
 */
const http = require('http');
const { WebSocketServer } = require('ws');
const CFG = require('./lib/config');
const { createCore } = require('./core');
const { serveStatic, warmCache } = require('./lib/static');

const PORT = process.env.PORT || 10000;
const core = createCore();

setInterval(() => core.tick(), CFG.TICK_MS);
setInterval(() => core.cleanup(), 5000);

const server = http.createServer(async (req, res) => {
  if (req.url === '/health') {
    console.log('[ANDROID_DIAG_V3_HTTP_HEALTH]', {
      method: req.method,
      userAgent: req.headers['user-agent'] || 'none'
    });
  }
  try {
    if (req.url === '/health') { res.writeHead(200); return res.end('ok'); }
    if (req.url === '/api/stats') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ name: 'cyber-chase', sessions: core.sessions.size, players: core.players.size }));
    }
    if (await serveStatic(req, res)) return;
  } catch (e) { console.error('http error', e); }
  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Not found. Экспортируйте веб-версию Godot в server/public/');
});

server.on('upgrade', (req) => {
    console.log('[WS UPGRADE IN]', {
        url: req.url,
        origin: req.headers.origin || 'none',
        userAgent: req.headers['user-agent'] || 'none'
    });
});
const wss = new WebSocketServer({ server, maxPayload: 4096 });
wss.on('connection', (ws) => {
  console.log('[WS CONNECTED]');
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  const out = (obj) => { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); };
  let p = null;

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw.toString()); } catch (_) { return; }
    if (!m || typeof m.t !== 'string') return;
    try {
      if (m.t === 'hello') {
        const r = core.hello(m, out, () => ws.terminate());
        if (r.error) return out({ t: 'error', code: r.error[0], msg: r.error[1] });
        p = r.player;
      } else if (p) core.handle(p, m);
      else out({ t: 'error', code: 'NO_HELLO', msg: 'Нет рукопожатия' });
    } catch (e) { console.error('handler error', e); out({ t: 'error', code: 'SERVER', msg: 'Внутренняя ошибка' }); }
  });
  ws.on('close', () => { if (p) core.disconnect(p, out); });
});

setInterval(() => {
  wss.clients.forEach((ws) => { if (!ws.isAlive) return ws.terminate(); ws.isAlive = false; ws.ping(); });
}, 25000);

server.listen(PORT, () => {
  console.log('[ANDROID_DIAG_V3_SERVER_ACTIVE]');
  console.log(`КИБЕР_Догонялки server on :${PORT}`);
  warmCache();
});

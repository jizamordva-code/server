'use strict';
/**
 * КИБЕР_Догонялки — игровой сервер.
 * WebSocket + JSON. Всё состояние хранится в памяти (одна инстанция).
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 10000;
const num = (v, d) => (v !== undefined && v !== '' && !Number.isNaN(+v)) ? +v : d;

const CFG = {
  TICK_MS: 500,
  TAG_RADIUS_M: num(process.env.TAG_RADIUS_M, 2),     // дистанция "поймал"
  MAX_ACC_M: num(process.env.MAX_ACC_M, 25),          // худшая допустимая точность GPS для поимки
  CHASER_RATIO: 0.4,                                  // < 40% догоняющих -> новый игрок догоняющий
  MATCH_RADIUS_KM: 100,
  MAX_PLAYERS: 40,
  BOTS: num(process.env.BOTS, 4),
  BOT_MIN_M: 400, BOT_MAX_M: 900,                     // боты появляются "далеко"
  BOT_WALK_MS: 1.6,
  BOT_FLEE_MS: num(process.env.BOT_FLEE_MS, 2.4),     // чуть медленнее бегуна -> можно догнать
  BOT_TRIGGER_M: 90,                                  // с какого расстояния бот начинает убегать
  ROUTER: (process.env.ROUTER_URL || 'https://routing.openstreetmap.de/routed-foot').replace(/\/$/, ''),
  PROFILE: process.env.ROUTE_PROFILE || 'driving',    // для OSRM имя профиля в URL игнорируется
  OFFLINE_GRACE_MS: 60000,
  STALE_POS_MS: 15000,
  TAGGER_BECOMES_RUNNER: process.env.TAGGER_BECOMES_RUNNER === '1',
  ROUND_RESET_MS: 8000,
};

// ───────────────────────── утилиты ─────────────────────────
const R = 6371000;
const rad = (d) => d * Math.PI / 180;
const deg = (r) => r * 180 / Math.PI;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const rid = (n) => crypto.randomBytes(n).toString('hex');

function dist(aLat, aLon, bLat, bLon) {
  const dLat = rad(bLat - aLat), dLon = rad(bLon - aLon);
  const h = Math.sin(dLat / 2) ** 2 + Math.cos(rad(aLat)) * Math.cos(rad(bLat)) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.min(1, Math.sqrt(h)));
}
function bearing(aLat, aLon, bLat, bLon) {
  const y = Math.sin(rad(bLon - aLon)) * Math.cos(rad(bLat));
  const x = Math.cos(rad(aLat)) * Math.sin(rad(bLat)) - Math.sin(rad(aLat)) * Math.cos(rad(bLat)) * Math.cos(rad(bLon - aLon));
  return (deg(Math.atan2(y, x)) + 360) % 360;
}
function dest(lat, lon, brg, m) {
  const d = m / R, b = rad(brg), la = rad(lat), lo = rad(lon);
  const la2 = Math.asin(Math.sin(la) * Math.cos(d) + Math.cos(la) * Math.sin(d) * Math.cos(b));
  const lo2 = lo + Math.atan2(Math.sin(b) * Math.sin(d) * Math.cos(la), Math.cos(d) - Math.sin(la) * Math.sin(la2));
  return { lat: deg(la2), lon: ((deg(lo2) + 540) % 360) - 180 };
}
const validPos = (lat, lon) => Number.isFinite(lat) && Number.isFinite(lon) && Math.abs(lat) <= 90 && Math.abs(lon) <= 180 && !(lat === 0 && lon === 0);
const monthKey = () => new Date().toISOString().slice(0, 7);

// ───────────────────── бесплатный лимит (раз в месяц) ─────────────────────
// ВНИМАНИЕ: на бесплатном Render файловая система временная. Для продакшена
// храните квоты в Render Postgres/Redis и привязывайте к аккаунту, а не к device id.
const DATA_FILE = process.env.DATA_FILE || './data.json';
let quota = {};
try { quota = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); } catch (_) { quota = {}; }
let saveTimer = null;
function saveQuota() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => fs.writeFile(DATA_FILE, JSON.stringify(quota), () => {}), 500);
}

// ───────────────────── маршрутизация для ботов (по дорогам) ─────────────────────
const routerQ = [];
let routerBusy = false;
function routerFetch(path) {
  return new Promise((resolve, reject) => {
    if (routerQ.length > 30) return reject(new Error('router queue full'));
    routerQ.push({ path, resolve, reject });
    pumpRouter();
  });
}
async function pumpRouter() {
  if (routerBusy) return;
  routerBusy = true;
  while (routerQ.length) {
    const job = routerQ.shift();
    const ctl = new AbortController();
    const to = setTimeout(() => ctl.abort(), 8000);
    try {
      const r = await fetch(CFG.ROUTER + job.path, { signal: ctl.signal, headers: { 'User-Agent': 'CyberChase/1.0' } });
      if (!r.ok) throw new Error('HTTP ' + r.status);
      job.resolve(await r.json());
    } catch (e) { job.reject(e); }
    clearTimeout(to);
    await sleep(250); // вежливый интервал для публичных роутеров
  }
  routerBusy = false;
}

// ───────────────────── авторизация Telegram Mini App ─────────────────────
// Android-клиент входит по случайному device id, Telegram-клиент — по подписанным initData.
// Оба попадают в ОДНИ И ТЕ ЖЕ сессии: протокол и сервер общие.
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const TG_APP_LINK = process.env.TG_APP_LINK || '';

function verifyInitData(initData) {
  try {
    const p = new URLSearchParams(initData);
    const hash = p.get('hash');
    if (!hash) return null;
    p.delete('hash');
    const check = [...p.entries()]
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      .map(([k, v]) => `${k}=${v}`).join('\n');
    if (BOT_TOKEN) {
      const secret = crypto.createHmac('sha256', 'WebAppData').update(BOT_TOKEN).digest();
      const calc = Buffer.from(crypto.createHmac('sha256', secret).update(check).digest('hex'));
      const got = Buffer.from(hash);
      if (calc.length !== got.length || !crypto.timingSafeEqual(calc, got)) return null;
    } else if (process.env.TG_INSECURE !== '1') {
      return null; // без BOT_TOKEN подпись проверить нельзя (для локальных тестов: TG_INSECURE=1)
    }
    const age = Date.now() / 1000 - Number(p.get('auth_date') || 0);
    if (!(age < 86400)) return null;
    const u = JSON.parse(p.get('user') || 'null');
    if (!u || !Number.isFinite(+u.id)) return null;
    return u;
  } catch (_) { return null; }
}

// ───────────────────── раздача веб-версии игры (папка public/) ─────────────────────
const PUBLIC_DIR = path.join(__dirname, 'public');
const MIME = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.mjs': 'text/javascript; charset=utf-8',
  '.wasm': 'application/wasm', '.pck': 'application/octet-stream', '.png': 'image/png', '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon', '.json': 'application/json', '.css': 'text/css; charset=utf-8', '.txt': 'text/plain; charset=utf-8',
  '.webmanifest': 'application/manifest+json', '.ttf': 'font/ttf', '.woff2': 'font/woff2',
};
const COMPRESSIBLE = new Set(['.html', '.js', '.mjs', '.wasm', '.pck', '.json', '.css', '.svg', '.txt']);
const bufCache = new Map(); // "file|etag|enc" -> Promise<Buffer>

function encodeBuf(raw, enc) {
  return new Promise((resolve, reject) => {
    const cb = (e, out) => (e ? reject(e) : resolve(out));
    if (enc === 'br') {
      zlib.brotliCompress(raw, { params: { [zlib.constants.BROTLI_PARAM_QUALITY]: 5, [zlib.constants.BROTLI_PARAM_SIZE_HINT]: raw.length } }, cb);
    } else zlib.gzip(raw, { level: 6 }, cb);
  });
}
function getBuf(file, etag, enc) {
  const key = `${file}|${etag}|${enc}`;
  if (!bufCache.has(key)) {
    const p = enc === 'raw'
      ? fs.promises.readFile(file)
      : getBuf(file, etag, 'raw').then((raw) => encodeBuf(raw, enc));
    bufCache.set(key, p);
    p.catch(() => bufCache.delete(key));
  }
  return bufCache.get(key);
}

async function serveStatic(req, res) {
  if (req.method !== 'GET' && req.method !== 'HEAD') return false;
  let urlPath;
  try { urlPath = decodeURIComponent(new URL(req.url, 'http://x').pathname); } catch (_) { return false; }
  if (urlPath.endsWith('/')) urlPath += 'index.html';
  const file = path.normalize(path.join(PUBLIC_DIR, urlPath));
  if (!file.startsWith(PUBLIC_DIR + path.sep)) return false;
  let st;
  try { st = await fs.promises.stat(file); } catch (_) { return false; }
  if (!st.isFile()) return false;

  const ext = path.extname(file).toLowerCase();
  const etag = `"${st.size.toString(16)}-${Math.floor(st.mtimeMs).toString(16)}"`;
  const headers = {
    'Content-Type': MIME[ext] || 'application/octet-stream',
    ETag: etag,
    'Cache-Control': ext === '.html' ? 'no-cache' : 'public, max-age=300',
    'X-Content-Type-Options': 'nosniff',
    Vary: 'Accept-Encoding',
  };
  if (req.headers['if-none-match'] === etag) { res.writeHead(304, headers); res.end(); return true; }

  const ae = String(req.headers['accept-encoding'] || '');
  let enc = 'raw';
  if (COMPRESSIBLE.has(ext)) enc = /\bbr\b/.test(ae) ? 'br' : (/\bgzip\b/.test(ae) ? 'gzip' : 'raw');
  const body = await getBuf(file, etag, enc);
  if (enc !== 'raw') headers['Content-Encoding'] = enc;
  headers['Content-Length'] = body.length;
  res.writeHead(200, headers);
  res.end(req.method === 'HEAD' ? undefined : body);
  return true;
}

// Прогрев кэша сжатия, чтобы первый игрок не ждал
async function warmCache() {
  try {
    for (const name of await fs.promises.readdir(PUBLIC_DIR)) {
      const file = path.join(PUBLIC_DIR, name);
      const ext = path.extname(name).toLowerCase();
      const st = await fs.promises.stat(file);
      if (!st.isFile() || !COMPRESSIBLE.has(ext)) continue;
      const etag = `"${st.size.toString(16)}-${Math.floor(st.mtimeMs).toString(16)}"`;
      await getBuf(file, etag, 'br');
    }
    console.log('static cache warmed');
  } catch (_) { /* папки public может не быть */ }
}

// ───────────────────────── состояние ─────────────────────────
const sessions = new Map(); // sid -> session
const players = new Map();  // deviceId -> player
const CODE_ALPHABET = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';

function uniqueCode() {
  for (;;) {
    let c = '';
    for (let i = 0; i < 6; i++) c += CODE_ALPHABET[crypto.randomInt(CODE_ALPHABET.length)];
    if (![...sessions.values()].some((s) => s.code === c)) return c;
  }
}
const online = (p) => p.ws && p.ws.readyState === 1;
const realPlayers = (s) => [...s.players.values()];
function send(ws, obj) { if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj)); }
const errMsg = (ws, code, msg) => send(ws, { t: 'error', code, msg });

function newSession(mode, lat, lon) {
  const s = {
    id: rid(4), code: null, mode, players: new Map(), bots: [], spawning: 0,
    createdAt: Date.now(), emptySince: null, allChasersSince: null, ref: { lat, lon },
  };
  if (mode === 'private') s.code = uniqueCode();
  sessions.set(s.id, s);
  return s;
}

function findPublicSession(lat, lon) {
  let best = null, bd = CFG.MATCH_RADIUS_KM * 1000;
  for (const s of sessions.values()) {
    if (s.mode !== 'public' || s.players.size >= CFG.MAX_PLAYERS) continue;
    for (const q of s.players.values()) {
      if (q.lat == null) continue;
      const d = dist(lat, lon, q.lat, q.lon);
      if (d < bd) { bd = d; best = s; }
    }
  }
  return best;
}

function sessionInfo(s) { return { id: s.id, code: s.code, mode: s.mode }; }

function joinSession(p, s, created = false) {
  const list = realPlayers(s);
  const ch = list.filter((x) => x.role === 'chaser').length;
  // Если догоняющих меньше 40% — игрок становится догоняющим, иначе убегающим
  p.role = (list.length === 0 || ch / list.length < CFG.CHASER_RATIO) ? 'chaser' : 'runner';
  p.sid = s.id;
  s.players.set(p.id, p);
  s.emptySince = null;
  send(p.ws, { t: 'joined', created, session: sessionInfo(s), you: { id: p.id, role: p.role, name: p.name } });
  manageBots(s);
  broadcastState(s);
}

function leaveSession(p) {
  const s = sessions.get(p.sid);
  p.sid = null;
  if (!s) return;
  s.players.delete(p.id);
  if (s.players.size === 0) s.emptySince = Date.now();
  manageBots(s);
}

function feed(s, text) {
  for (const p of s.players.values()) send(p.ws, { t: 'feed', text });
}

// ───────────────────────── боты ─────────────────────────
function manageBots(s) {
  const real = realPlayers(s);
  // Боты нужны только публичной сессии с ОДНИМ живым игроком
  if (s.mode !== 'public' || real.length !== 1) { s.bots = []; return; }
  const need = CFG.BOTS - s.bots.length - s.spawning;
  for (let i = 0; i < need; i++) spawnBot(s).catch(() => {});
}

function makeBot(pos) {
  return {
    id: 'bot-' + rid(3), name: 'AI-' + (10 + Math.floor(Math.random() * 90)),
    lat: pos.lat, lon: pos.lon, role: 'runner', bot: true,
    path: [], pi: 0, mode: 'wander', planning: false, lastPlan: 0,
  };
}

async function spawnBot(s) {
  s.spawning++;
  try {
    const me = realPlayers(s).find((p) => p.lat != null);
    if (!me) return;
    let pos = null, raw = null;
    for (let i = 0; i < 3 && !pos; i++) {
      raw = dest(me.lat, me.lon, Math.random() * 360, CFG.BOT_MIN_M + Math.random() * (CFG.BOT_MAX_M - CFG.BOT_MIN_M));
      try {
        const j = await routerFetch(`/nearest/v1/${CFG.PROFILE}/${raw.lon},${raw.lat}`);
        const loc = j.waypoints && j.waypoints[0] && j.waypoints[0].location;
        if (loc) {
          const d = dist(me.lat, me.lon, loc[1], loc[0]);
          if (d >= 150 && d <= 1500) pos = { lat: loc[1], lon: loc[0] }; // точка на дороге
        }
      } catch (_) { /* роутер недоступен */ }
    }
    if (!pos) pos = raw; // запасной вариант — без привязки к дороге
    if (!sessions.has(s.id) || realPlayers(s).length !== 1) return;
    s.bots.push(makeBot(pos));
  } finally { s.spawning--; }
}

async function planBot(b, threat, now) {
  b.planning = true; b.lastPlan = now;
  let brg, d;
  if (threat) { // убегаем: направление ОТ догоняющего ±35°
    brg = bearing(threat.lat, threat.lon, b.lat, b.lon) + (Math.random() * 70 - 35);
    d = 500 + Math.random() * 300;
  } else { brg = Math.random() * 360; d = 300 + Math.random() * 600; }
  const t = dest(b.lat, b.lon, brg, d);
  try {
    const j = await routerFetch(`/route/v1/${CFG.PROFILE}/${b.lon},${b.lat};${t.lon},${t.lat}?overview=full&geometries=geojson`);
    const c = j.routes && j.routes[0] && j.routes[0].geometry.coordinates;
    if (!c || c.length < 2) throw new Error('no route');
    b.path = c.map((x) => [x[1], x[0]]); b.pi = 0;
  } catch (_) {
    if (threat) { b.path = [[t.lat, t.lon]]; b.pi = 0; } // при бегстве — хоть по прямой
    else { b.path = []; b.pi = 0; }
  }
  b.planning = false;
}

function moveBot(b, meters) {
  while (meters > 0 && b.pi < b.path.length) {
    const [tl, tn] = b.path[b.pi];
    const d = dist(b.lat, b.lon, tl, tn);
    if (d <= meters) { b.lat = tl; b.lon = tn; meters -= d; b.pi++; }
    else { const f = meters / d; b.lat += (tl - b.lat) * f; b.lon += (tn - b.lon) * f; meters = 0; }
  }
}

function botStep(s, b, dt, now) {
  let threat = null, td = Infinity;
  for (const c of realPlayers(s)) {
    if (c.role !== 'chaser' || c.lat == null || now - c.lastPos > CFG.STALE_POS_MS) continue;
    const d = dist(b.lat, b.lon, c.lat, c.lon);
    if (d < td) { td = d; threat = c; }
  }
  if (threat && td < CFG.BOT_TRIGGER_M) {
    if (b.mode !== 'flee') { b.mode = 'flee'; b.path = []; b.pi = 0; b.lastPlan = 0; }
    if (!b.planning && (b.pi >= b.path.length || now - b.lastPlan > 3000)) planBot(b, threat, now);
  } else {
    if (b.mode === 'flee' && (!threat || td > CFG.BOT_TRIGGER_M * 1.5)) { b.mode = 'wander'; b.path = []; b.pi = 0; }
    if (!b.planning && b.pi >= b.path.length && now - b.lastPlan > 2000) planBot(b, null, now);
  }
  moveBot(b, (b.mode === 'flee' ? CFG.BOT_FLEE_MS : CFG.BOT_WALK_MS) * dt);
}

// ───────────────────────── игровой цикл ─────────────────────────
function processTags(s, now) {
  const live = realPlayers(s).filter((p) => online(p) && p.lat != null && now - p.lastPos < CFG.STALE_POS_MS && p.acc <= CFG.MAX_ACC_M);
  const chasers = live.filter((p) => p.role === 'chaser');

  for (const r of live) {
    if (r.role !== 'runner' || (r.immuneUntil || 0) > now) continue;
    for (const c of chasers) {
      if (c.role !== 'chaser') continue;
      if (dist(r.lat, r.lon, c.lat, c.lon) <= CFG.TAG_RADIUS_M) {
        r.role = 'chaser';
        send(r.ws, { t: 'tagged', by: c.name, role: 'chaser' });
        feed(s, `${c.name} ДОГНАЛ ${r.name}`);
        if (CFG.TAGGER_BECOMES_RUNNER) {
          c.role = 'runner'; c.immuneUntil = now + 8000;
          send(c.ws, { t: 'role', role: 'runner' });
        }
        break;
      }
    }
  }
  // боты
  for (const c of chasers) {
    if (c.role !== 'chaser') continue;
    for (const b of [...s.bots]) {
      if (dist(b.lat, b.lon, c.lat, c.lon) <= CFG.TAG_RADIUS_M) {
        s.bots = s.bots.filter((x) => x !== b);
        send(c.ws, { t: 'bot_caught', name: b.name });
        manageBots(s);
      }
    }
  }
}

function maybeResetRound(s, now) {
  const real = realPlayers(s).filter(online);
  if (real.length < 2 || real.some((p) => p.role === 'runner')) { s.allChasersSince = null; return; }
  if (!s.allChasersSince) { s.allChasersSince = now; return; }
  if (now - s.allChasersSince < CFG.ROUND_RESET_MS) return;
  const shuffled = real.sort(() => Math.random() - 0.5);
  const nCh = Math.max(1, Math.floor(real.length * CFG.CHASER_RATIO));
  shuffled.forEach((p, i) => {
    p.role = i < nCh ? 'chaser' : 'runner';
    send(p.ws, { t: 'role', role: p.role });
  });
  feed(s, 'НОВЫЙ РАУНД — РОЛИ ПЕРЕРАСПРЕДЕЛЕНЫ');
  s.allChasersSince = null;
}

const r6 = (v) => Math.round(v * 1e6) / 1e6;
function broadcastState(s) {
  const list = [];
  for (const p of s.players.values()) {
    if (!online(p) || p.lat == null) continue;
    list.push({ id: p.id, name: p.name, lat: r6(p.lat), lon: r6(p.lon), role: p.role, bot: false });
  }
  for (const b of s.bots) list.push({ id: b.id, name: b.name, lat: r6(b.lat), lon: r6(b.lon), role: 'runner', bot: true });
  const chasers = list.filter((x) => x.role === 'chaser').length;
  const msg = JSON.stringify({ t: 'state', sid: s.id, players: list, chasers, total: list.length, ts: Date.now() });
  for (const p of s.players.values()) if (online(p)) p.ws.send(msg);
}

setInterval(() => {
  const now = Date.now();
  for (const s of sessions.values()) {
    for (const b of s.bots) botStep(s, b, CFG.TICK_MS / 1000, now);
    processTags(s, now);
    maybeResetRound(s, now);
    broadcastState(s);
  }
}, CFG.TICK_MS);

setInterval(() => {
  const now = Date.now();
  for (const [id, p] of players) {
    if (!online(p) && p.offlineSince && now - p.offlineSince > CFG.OFFLINE_GRACE_MS) {
      leaveSession(p); players.delete(id);
    }
  }
  for (const [sid, s] of sessions) {
    if (s.players.size === 0) {
      s.emptySince = s.emptySince || now;
      const ttl = s.mode === 'private' ? 15 * 60e3 : 30e3;
      if (now - s.emptySince > ttl) sessions.delete(sid);
    }
  }
}, 5000);

// ───────────────────────── сеть ─────────────────────────
const server = http.createServer(async (req, res) => {
  try {
    if (req.url === '/health') { res.writeHead(200); return res.end('ok'); }
    if (req.url === '/api/stats') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ name: 'cyber-chase', sessions: sessions.size, players: players.size }));
    }
    if (await serveStatic(req, res)) return;
  } catch (e) { console.error('http error', e); }
  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Not found. Экспортируйте веб-версию Godot в server/public/');
});
const wss = new WebSocketServer({ server, maxPayload: 4096 });

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  let p = null;

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw.toString()); } catch (_) { return; }
    if (!m || typeof m.t !== 'string') return;
    try { handle(m); } catch (e) { console.error('handler error', e); errMsg(ws, 'SERVER', 'Внутренняя ошибка'); }
  });

  ws.on('close', () => { if (p && p.ws === ws) { p.ws = null; p.offlineSince = Date.now(); } });

  function setPos(m) {
    const lat = +m.lat, lon = +m.lon;
    if (!validPos(lat, lon)) return false;
    p.lat = lat; p.lon = lon; p.acc = Number.isFinite(+m.acc) ? +m.acc : 999;
    p.hdg = Number.isFinite(+m.hdg) ? +m.hdg : 0; p.lastPos = Date.now();
    return true;
  }

  function handle(m) {
    if (m.t === 'hello') {
      let id, nm = m.name;
      if (typeof m.init_data === 'string' && m.init_data.length > 0) {
        // Telegram Mini App: личность подтверждена подписью Telegram
        const u = verifyInitData(m.init_data);
        if (!u) return errMsg(ws, 'BAD_AUTH', 'ОШИБКА АВТОРИЗАЦИИ TELEGRAM');
        id = 'tg' + u.id;
        nm = u.username || u.first_name || 'TG';
      } else {
        // Android / браузерный гость: случайный device id (hex), префикс tg зарезервирован
        id = String(m.id || '').replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 64);
        if (id.length < 8 || /^tg/i.test(id)) return errMsg(ws, 'BAD_ID', 'Некорректный идентификатор');
      }
      p = players.get(id);
      if (!p) {
        p = { id, name: '', ws: null, lat: null, lon: null, acc: 999, hdg: 0, role: 'runner', sid: null, lastPos: 0, offlineSince: null };
        players.set(id, p);
      } else if (p.ws && p.ws !== ws) { try { p.ws.terminate(); } catch (_) {} }
      p.ws = ws; p.offlineSince = null;
      p.name = String(nm || 'RUNNER').replace(/[^\w\-А-Яа-яЁё ]/g, '').slice(0, 16) || 'RUNNER';
      send(ws, { t: 'welcome', free_available: quota[id] !== monthKey(), month: monthKey(), tg_app_link: TG_APP_LINK });
      const s = p.sid && sessions.get(p.sid);
      if (m.resume && s) send(ws, { t: 'joined', created: false, session: sessionInfo(s), you: { id: p.id, role: p.role, name: p.name } });
      else if (s) leaveSession(p);
      return;
    }
    if (!p) return errMsg(ws, 'NO_HELLO', 'Нет рукопожатия');

    switch (m.t) {
      case 'pos': setPos(m); break;

      case 'join_public': {
        if (!setPos(m)) return errMsg(ws, 'BAD_POS', 'GPS: некорректные координаты');
        if (p.sid) leaveSession(p);
        const s = findPublicSession(p.lat, p.lon) || newSession('public', p.lat, p.lon);
        joinSession(p, s);
        break;
      }
      case 'create': {
        if (!setPos(m)) return errMsg(ws, 'BAD_POS', 'GPS: некорректные координаты');
        if (quota[p.id] === monthKey()) return errMsg(ws, 'LIMIT', 'БЕСПЛАТНАЯ СЕССИЯ ЭТОГО МЕСЯЦА УЖЕ ИСПОЛЬЗОВАНА');
        if (p.sid) leaveSession(p);
        quota[p.id] = monthKey(); saveQuota();
        const s = newSession(m.mode === 'private' ? 'private' : 'public', p.lat, p.lon);
        joinSession(p, s, true);
        break;
      }
      case 'join_code': {
        if (!setPos(m)) return errMsg(ws, 'BAD_POS', 'GPS: некорректные координаты');
        const code = String(m.code || '').toUpperCase().trim();
        const s = [...sessions.values()].find((x) => x.mode === 'private' && x.code === code);
        if (!s) return errMsg(ws, 'NOT_FOUND', 'СЕССИЯ С ТАКИМ КОДОМ НЕ НАЙДЕНА');
        if (s.players.size >= CFG.MAX_PLAYERS) return errMsg(ws, 'FULL', 'СЕССИЯ ЗАПОЛНЕНА');
        if (p.sid && p.sid !== s.id) leaveSession(p);
        if (p.sid !== s.id) joinSession(p, s);
        break;
      }
      case 'leave': leaveSession(p); break;
      default: break;
    }
  }
});

setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false; ws.ping();
  });
}, 25000);

server.listen(PORT, () => { console.log(`КИБЕР_Догонялки server on :${PORT}`); warmCache(); });

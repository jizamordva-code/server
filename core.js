'use strict';
/**
 * Ядро игры без сетевого кода: игроки, сессии, лобби, подбор и слияние комнат, результаты.
 * Сетевой слой (server.js) подключает игрока через core.hello(...), у игрока есть функция p.out(obj).
 */
const crypto = require('crypto');
const CFG = require('./lib/config');
const geo = require('./lib/geo');
const store = require('./lib/store');
const router = require('./lib/router');
const { verifyInitData } = require('./lib/auth');
const { send, online } = require('./lib/util');

const MODES = { classic: require('./modes/classic'), ctf: require('./modes/ctf'), team: require('./modes/team') };
const MODE_RU = { classic: 'ДОГОНЯЛКИ', ctf: 'ЗАХВАТ ФЛАГА', team: 'ДОГОНЯ⚚КИ' };
const CODE_ALPHABET = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';

function createCore(opts = {}) {
  const nowFn = opts.now || Date.now;
  const players = new Map();
  const sessions = new Map();

  // ───────── контекст для режимов ─────────
  const ctx = () => ({ now: nowFn(), send, feed, broadcast, finish: finishGame, nearest: router.nearest, route: router.route });

  function feed(s, text, kind = 'info') { for (const p of s.players.values()) send(p, { t: 'feed', text, kind }); }
  function broadcast(s, msg) { for (const p of s.players.values()) send(p, msg); }
  const err = (p, code, msg) => send(p, { t: 'error', code, msg });

  // ───────── сессии ─────────
  function uniqueCode() {
    for (;;) {
      let c = '';
      for (let i = 0; i < 6; i++) c += CODE_ALPHABET[crypto.randomInt(CODE_ALPHABET.length)];
      if (![...sessions.values()].some((s) => s.code === c)) return c;
    }
  }
  function newSession(kind, mode, owner) {
    const s = {
      id: crypto.randomBytes(4).toString('hex'), code: uniqueCode(), kind, mode, owner: kind === 'private' ? owner : null,
      players: new Map(), bots: [], g: {}, state: mode === 'classic' ? 'running' : 'lobby',
      createdAt: nowFn(), emptySince: null, lobbyAt: null, startedAt: mode === 'classic' ? nowFn() : null, endedAt: null, lastTick: nowFn(),
    };
    sessions.set(s.id, s);
    return s;
  }
  const minPlayers = (s) => (s.kind === 'private' ? CFG.MIN_PRIVATE : CFG.MIN_PUBLIC[s.mode]);
  const sessionInfo = (s) => ({ id: s.id, code: s.code, kind: s.kind, mode: s.mode, state: s.state });

  // mode = null -> подойдёт комната любого режима (кнопка «ИГРАТЬ» берёт самую ближнюю)
  function joinable(s, mode) {
    return s.kind === 'public' && (!mode || s.mode === mode) && s.state !== 'over' && s.players.size < CFG.MAX_PLAYERS;
  }
  function findPublic(mode, lat, lon) {
    let best = null, bd = CFG.MATCH_RADIUS_KM * 1000;
    for (const s of sessions.values()) {
      if (!joinable(s, mode)) continue;
      for (const q of s.players.values()) {
        if (q.lat == null) continue;
        const d = geo.dist(lat, lon, q.lat, q.lon);
        if (d < bd) { bd = d; best = s; }
      }
    }
    return best;
  }

  function joinSession(p, s, flags = {}) {
    if (p.sid && p.sid !== s.id) leaveSession(p);
    const now = nowFn();
    p.sid = s.id; s.players.set(p.id, p); s.emptySince = null;
    p.st = { tags: 0, bots: 0, pct: 0, joinedAt: now };
    p.team = null; p.cap = false; p.role = 'runner'; p.shieldAt = 0; p.immuneUntil = 0;
    MODES[s.mode].join(s, p, ctx());
    send(p, { t: 'joined', created: !!flags.created, merged: !!flags.merged, session: sessionInfo(s),
      you: { id: p.id, role: p.role, team: p.team, name: p.name } });
    feed(s, `${p.name} подключился к игре`, 'join');
    broadcastState(s);
  }

  function entryFor(s, p, result, points) {
    return { id: `${s.id}:${p.id}:${p.st.joinedAt}`, ts: nowFn(), mode: s.mode, mode_name: MODE_RU[s.mode], result,
      points, team: p.team || null, dur: Math.round((nowFn() - p.st.joinedAt) / 1000), tags: p.st.tags };
  }
  /** Запись в историю при выходе из незавершённой игры. */
  function recordLeave(s, p) {
    if (!p.st || s.state === 'over' || s.state === 'lobby') return null;
    const personal = MODES[s.mode].personalPoints(p);
    if (s.mode === 'classic') {
      if (nowFn() - p.st.joinedAt < 30000 && personal === 0) return null;
      const e = entryFor(s, p, 'free', personal);
      store.addHistory(p.id, e);
      return e;
    }
    const e = entryFor(s, p, 'left', personal);
    store.addHistory(p.id, e);
    return e;
  }
  function leaveSession(p) {
    const s = sessions.get(p.sid);
    p.sid = null;
    if (!s) return null;
    const entry = recordLeave(s, p);
    s.players.delete(p.id);
    MODES[s.mode].leave(s, p, ctx());
    if (s.players.size === 0) s.emptySince = nowFn();
    return entry;
  }

  function startGame(s) {
    s.state = 'running'; s.startedAt = nowFn(); s.lobbyAt = null;
    MODES[s.mode].start(s, ctx());
    feed(s, `ИГРА НАЧАЛАСЬ · ${MODE_RU[s.mode]}`, 'round');
  }

  function finishGame(s, outcome) {
    if (s.state === 'over') return;
    s.state = 'over'; s.endedAt = nowFn();
    for (const p of s.players.values()) {
      const gone = !online(p);                       // ушёл из игры и не вернулся до конца матча
      const result = gone ? 'left' : outcome.winner === 'draw' ? 'draw' : (p.team === outcome.winner ? 'win' : 'loss');
      const base = gone ? 0 : result === 'win' ? CFG.POINTS.WIN : result === 'draw' ? CFG.POINTS.DRAW : CFG.POINTS.LOSS;
      const e = entryFor(s, p, result, base + MODES[s.mode].personalPoints(p));
      store.addHistory(p.id, e);
      send(p, { t: 'match_end', ...e, winner: outcome.winner, summary: outcome.summary });
    }
    const w = outcome.winner === 'draw' ? 'НИЧЬЯ' : `ПОБЕДИЛИ ${outcome.winner === 'red' ? 'КРАСНЫЕ' : 'СИНИЕ'}`;
    feed(s, `ИГРА ОКОНЧЕНА · ${w} (${outcome.summary.red}:${outcome.summary.blue})`, 'round');
  }

  // ───────── объединение комнат с одинаковыми настройками ─────────
  const mergeable = (a, b) => a.mode === b.mode && (a.mode === 'classic' || (a.state === 'lobby' && b.state === 'lobby'));
  function sessionDist(a, b) {
    let m = Infinity;
    for (const p of a.players.values()) for (const q of b.players.values()) {
      if (p.lat == null || q.lat == null) continue;
      m = Math.min(m, geo.dist(p.lat, p.lon, q.lat, q.lon));
    }
    return m;
  }
  function mergeInto(target, src) {
    for (const p of [...src.players.values()]) {
      src.players.delete(p.id); p.sid = null;
      joinSession(p, target, { merged: true });
    }
    sessions.delete(src.id);
  }
  function tryMerge() {
    const list = [...sessions.values()].filter((s) => s.kind === 'public' && s.state !== 'over' && s.players.size > 0);
    for (let i = 0; i < list.length; i++) for (let j = i + 1; j < list.length; j++) {
      const a = list[i], b = list[j];
      if (!mergeable(a, b) || a.players.size + b.players.size > CFG.MAX_PLAYERS) continue;
      if (sessionDist(a, b) > CFG.MATCH_RADIUS_KM * 1000) continue;
      const [t, s] = a.players.size >= b.players.size ? [a, b] : [b, a];
      mergeInto(t, s);
      return true;
    }
    return false;
  }

  // ───────── состояние ─────────
  function stateMsg(s) {
    const c = ctx();
    const mode = MODES[s.mode];
    const list = [];
    for (const p of s.players.values()) {
      if (!online(p) || p.lat == null) continue;
      list.push({ id: p.id, name: p.name, lat: +p.lat.toFixed(6), lon: +p.lon.toFixed(6), bot: false, ...mode.playerView(s, p) });
    }
    for (const b of s.bots) list.push({ id: b.id, name: b.name, lat: +b.lat.toFixed(6), lon: +b.lon.toFixed(6), role: 'runner', bot: true });
    const msg = { t: 'state', sid: s.id, mode: s.mode, phase: s.state, players: list,
      chasers: list.filter((x) => x.role === 'chaser').length, total: list.length, ts: c.now };
    if (s.state === 'lobby') {
      msg.lobby = { min: minPlayers(s), have: s.players.size, kind: s.kind, owner: s.owner,
        start_in_ms: s.lobbyAt ? Math.max(0, s.lobbyAt - c.now) : null };
    } else Object.assign(msg, mode.stateExtra(s, c));
    return msg;
  }
  function broadcastState(s) { const m = stateMsg(s); for (const p of s.players.values()) send(p, m); }

  function tickSession(s, now) {
    const dt = Math.min(2, (now - s.lastTick) / 1000);
    s.lastTick = now;
    if (s.state === 'lobby' && s.kind === 'public') {
      if (s.players.size >= minPlayers(s)) {
        if (!s.lobbyAt) s.lobbyAt = now + CFG.LOBBY_MS;
        if (now >= s.lobbyAt) startGame(s);
      } else s.lobbyAt = null;
    }
    if (s.state === 'running') MODES[s.mode].tick(s, ctx(), dt);
    broadcastState(s);
  }
  function tick() {
    const now = nowFn();
    for (const s of sessions.values()) tickSession(s, now);
  }
  function cleanup() {
    const now = nowFn();
    for (const [id, p] of players) {
      if (!online(p) && p.offlineSince && now - p.offlineSince > CFG.OFFLINE_GRACE_MS) { leaveSession(p); players.delete(id); }
    }
    for (const [sid, s] of sessions) {
      if (s.players.size === 0) {
        s.emptySince = s.emptySince || now;
        const ttl = s.kind === 'private' ? 15 * 60e3 : 30e3;
        if (now - s.emptySince > ttl) sessions.delete(sid);
      } else if (s.state === 'over' && now - s.endedAt > 10 * 60e3) {
        for (const p of s.players.values()) p.sid = null;
        sessions.delete(sid);
      }
    }
    tryMerge();
  }

  // ───────── подключение и сообщения ─────────
  function hello(m, out, terminate) {
    let id, nm = m.name;
    if (typeof m.init_data === 'string' && m.init_data.length > 0) {
      const u = verifyInitData(m.init_data);
      if (!u) return { error: ['BAD_AUTH', 'ОШИБКА АВТОРИЗАЦИИ TELEGRAM'] };
      id = 'tg' + u.id; nm = u.username || u.first_name || 'TG';
    } else {
      id = String(m.id || '').replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 64);
      if (id.length < 8 || /^tg/i.test(id)) return { error: ['BAD_ID', 'Некорректный идентификатор'] };
    }
    let p = players.get(id);
    if (!p) {
      p = { id, name: '', out: null, terminate: null, lat: null, lon: null, acc: 999, hdg: 0, role: 'runner', team: null, cap: false,
        sid: null, lastPos: 0, offlineSince: null, st: { tags: 0, bots: 0, pct: 0, joinedAt: nowFn() } };
      players.set(id, p);
    } else if (p.out && p.out !== out && p.terminate) { try { p.terminate(); } catch (_) { /* уже закрыт */ } }
    p.out = out; p.terminate = terminate || null; p.offlineSince = null;
    p.name = String(nm || 'RUNNER').replace(/[^\w\-А-Яа-яЁё ]/g, '').slice(0, 16) || 'RUNNER';
    send(p, { t: 'welcome', id: p.id, free_available: store.hasFree(id), month: store.monthKey(), tg_app_link: process.env.TG_APP_LINK || '' });
    const s = p.sid && sessions.get(p.sid);
    if (m.resume && s) send(p, { t: 'joined', created: false, session: sessionInfo(s), you: { id: p.id, role: p.role, team: p.team, name: p.name } });
    else if (s) leaveSession(p);
    return { player: p };
  }
  function disconnect(p, out) {
    if (p.out === out) { p.out = null; p.offlineSince = nowFn(); }
  }

  function setPos(p, m) {
    const lat = +m.lat, lon = +m.lon;
    if (!geo.validPos(lat, lon)) return false;
    p.lat = lat; p.lon = lon; p.acc = Number.isFinite(+m.acc) ? +m.acc : 999;
    p.hdg = Number.isFinite(+m.hdg) ? +m.hdg : 0; p.lastPos = nowFn();
    return true;
  }
  const parseMode = (v) => (MODES[v] ? v : 'classic');

  function handle(p, m) {
    switch (m.t) {
      case 'pos': setPos(p, m); break;

      case 'join_public': {
        if (!setPos(p, m)) return err(p, 'BAD_POS', 'GPS: некорректные координаты');
        const mode = MODES[m.mode] ? m.mode : null;           // без режима — ближайшая игра любого режима
        if (p.sid) leaveSession(p);
        const s = findPublic(mode, p.lat, p.lon) || newSession('public', mode || 'classic', null);
        joinSession(p, s);
        break;
      }
      case 'create': {
        if (!setPos(p, m)) return err(p, 'BAD_POS', 'GPS: некорректные координаты');
        if (!store.hasFree(p.id)) return err(p, 'LIMIT', 'БЕСПЛАТНАЯ СЕССИЯ ЭТОГО МЕСЯЦА УЖЕ ИСПОЛЬЗОВАНА');
        // совместимость со старым клиентом: mode = public/private
        let kind = m.kind, mode = m.mode;
        if (m.mode === 'public' || m.mode === 'private') { kind = m.mode; mode = 'classic'; }
        kind = kind === 'private' ? 'private' : 'public';
        mode = parseMode(mode);
        if (p.sid) leaveSession(p);
        store.useFree(p.id);
        if (kind === 'public') {
          // одинаковые настройки рядом -> комнаты объединяются
          const ex = findPublic(mode, p.lat, p.lon);
          if (ex) { joinSession(p, ex, { created: true, merged: true }); return send(p, { t: 'free_update', free_available: store.hasFree(p.id) }); }
        }
        joinSession(p, newSession(kind, mode, p.id), { created: true });
        send(p, { t: 'free_update', free_available: store.hasFree(p.id) });
        break;
      }
      case 'join_code': {
        if (!setPos(p, m)) return err(p, 'BAD_POS', 'GPS: некорректные координаты');
        const code = String(m.code || '').toUpperCase().trim();
        const s = [...sessions.values()].find((x) => x.code === code);   // код есть у любой комнаты
        if (!s || s.state === 'over') return err(p, 'NOT_FOUND', 'СЕССИЯ С ТАКИМ КОДОМ НЕ НАЙДЕНА');
        if (s.players.size >= CFG.MAX_PLAYERS) return err(p, 'FULL', 'СЕССИЯ ЗАПОЛНЕНА');
        if (p.sid !== s.id) joinSession(p, s);
        break;
      }
      case 'start': { // владелец личной сессии запускает игру вручную
        const s = sessions.get(p.sid);
        if (!s || s.state !== 'lobby' || s.owner !== p.id) return;
        if (s.players.size < minPlayers(s)) return err(p, 'FEW', `НУЖНО МИНИМУМ ${minPlayers(s)} ИГРОКА`);
        startGame(s);
        break;
      }
      case 'leave': {
        const e = leaveSession(p);
        send(p, { t: 'match_end', ...(e || { result: 'none' }) });
        break;
      }
      case 'stats': send(p, { t: 'stats', history: store.getHistory(p.id) }); break;
      case 'ad_reward': { // просмотрена реклама с наградой: +1 сессия (лимит в месяц)
        const granted = store.grantExtra(p.id);
        send(p, { t: 'free_update', free_available: store.hasFree(p.id), granted });
        break;
      }
      default: break;
    }
  }

  return { players, sessions, hello, disconnect, handle, tick, cleanup, tryMerge, startGame, _ctx: ctx, MODE_RU };
}
module.exports = { createCore };

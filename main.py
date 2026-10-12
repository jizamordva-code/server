
"""
Сервер игры (FastAPI + SQLAlchemy).  Запуск:  uvicorn main:app --host 0.0.0.0 --port $PORT

Что изменилось по сравнению со старым файлом:
  * есть ВСЕ эндпоинты, которые вызывает клиент (добавлены 10 недостающих);
  * маршруты доступны и с префиксом /api (как зовёт клиент), и без него (как в старом файле);
  * состояние матчей лежит в нормальных таблицах (матч + клетки + игроки), а не JSON-строкой;
  * «висячие» комнаты убирает один фоновый уборщик по полям created_at/last_seen
    (никакого разбора времени из room_id и никаких `except: pass`);
  * сезонный режим (2 на 2): настоящее лобби на сервере, реалистичное появление ботов;
  * БД берётся из переменной DATABASE_URL (Postgres на бесплатном хостинге),
    по умолчанию — SQLite-файл game.db (таблица players совместима со старой).
"""
import asyncio
import json
import logging
import math
import os
import random
import string
import threading
import time
from collections import defaultdict
from contextlib import asynccontextmanager, contextmanager

from fastapi import APIRouter, Body, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import (BigInteger, Boolean, Column, Float, Integer, String,
                        Text, UniqueConstraint, case, create_engine, event,
                        update)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import declarative_base, sessionmaker

log = logging.getLogger("game")
logging.basicConfig(level=logging.INFO)

# ───────────────────────────── БАЗА ДАННЫХ ─────────────────────────────

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./game.db")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = "postgresql+psycopg2://" + DATABASE_URL[len("postgres://"):]
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = "postgresql+psycopg2://" + DATABASE_URL[len("postgresql://"):]
IS_SQLITE = DATABASE_URL.startswith("sqlite")

if IS_SQLITE:
    engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})

    @event.listens_for(engine, "connect")
    def _sqlite_connect(dbapi_conn, _rec):
        dbapi_conn.isolation_level = None  # транзакциями управляем сами (см. ниже)
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA busy_timeout=10000")
        cur.execute("PRAGMA synchronous=NORMAL")
        cur.close()

    @event.listens_for(engine, "begin")
    def _sqlite_begin(conn):
        # сразу берём блокировку записи: нет «database is locked» при апгрейде чтения до записи
        conn.exec_driver_sql("BEGIN IMMEDIATE")
else:
    engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=300)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


@contextmanager
def session():
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


# Игроки: таблица ОСТАЛАСЬ как в старом сервере (рейтинги переезжают без миграции)
class Player(Base):
    __tablename__ = "players"
    id = Column(Integer, primary_key=True, index=True)
    player_id = Column(String, unique=True)
    total_ex = Column(Integer, default=0)


class Kv(Base):  # произвольные пары для /store_data (кроме рейтинга)
    __tablename__ = "kv_store"
    tag = Column(String(200), primary_key=True)
    value = Column(Text, default="")
    updated_at = Column(Float, default=0)


# ---- обычный матч 1 на 1 (поле 5x5) ----
class Room(Base):
    __tablename__ = "rooms_v2"
    id = Column(Integer, primary_key=True)
    room_id = Column(String(64), unique=True, index=True)
    host_id = Column(String, default="")
    guest_id = Column(String, default="")
    status = Column(String(16), default="WAITING")  # WAITING / READY / FINISHED
    created_at = Column(Float, default=0)
    last_seen = Column(Float, default=0)


class Match(Base):
    __tablename__ = "matches_v2"
    id = Column(Integer, primary_key=True)
    room_id = Column(String(64), unique=True, index=True)
    status = Column(String(16), default="HOST_PICK")  # HOST_PICK / GUEST_PICK / RUNNING
    host_id = Column(String, default="")
    guest_id = Column(String, default="")
    host_start = Column(Integer, default=-1)
    guest_start = Column(Integer, default=-1)
    winner = Column(String, default="")
    host_glitch_until = Column(BigInteger, default=0)
    guest_glitch_until = Column(BigInteger, default=0)
    host_protection_until = Column(BigInteger, default=0)
    guest_protection_until = Column(BigInteger, default=0)
    host_hidden_until = Column(BigInteger, default=0)
    guest_hidden_until = Column(BigInteger, default=0)
    host_skulls = Column(Integer, default=50)
    host_energy = Column(Integer, default=0)
    host_rating = Column(Integer, default=0)
    guest_skulls = Column(Integer, default=50)
    guest_energy = Column(Integer, default=0)
    guest_rating = Column(Integer, default=0)
    rewarded = Column(Boolean, default=False)
    created_at = Column(Float, default=0)


# клетки обоих режимов: scope "m" = матч 1x1, "e" = сезонный матч 2x2
class Cell(Base):
    __tablename__ = "cells_v2"
    id = Column(Integer, primary_key=True)
    scope = Column(String(1), index=True)
    room_id = Column(String(64), index=True)
    idx = Column(Integer)
    type = Column(String(32))
    owner = Column(Integer, default=0)
    ability_spent = Column(Boolean, default=False)
    cooldown_end = Column(BigInteger, default=0)
    rev_a = Column(Boolean, default=False)  # 1x1: видна хосту      | 2x2: видна команде 1
    rev_b = Column(Boolean, default=False)  # 1x1: видна гостю      | 2x2: видна команде 2
    blocked = Column(Text, default="[]")    # только 2x2 (blocked_directions)
    __table_args__ = (UniqueConstraint("scope", "room_id", "idx", name="uq_cell"),)


# ---- сезонный режим 2 на 2 (поле 7x7) ----
class EventRoom(Base):
    __tablename__ = "event_rooms_v2"
    id = Column(Integer, primary_key=True)
    room_id = Column(String(64), unique=True, index=True)
    room_code = Column(String(8), unique=True, index=True)
    status = Column(String(16), default="WAITING")  # WAITING / RUNNING / FINISHED
    host_name = Column(String, default="")
    created_at = Column(Float, default=0)
    start_at = Column(Float, default=0)
    started_at = Column(Float, default=0)
    finished_at = Column(Float, default=0)
    last_seen = Column(Float, default=0)


class EventSlot(Base):  # 4 места в лобби: 0,1 — команда 1; 2,3 — команда 2
    __tablename__ = "event_slots_v2"
    id = Column(Integer, primary_key=True)
    room_id = Column(String(64), index=True)
    slot = Column(Integer)
    name = Column(String)
    rating = Column(Integer, default=0)
    is_bot = Column(Boolean, default=False)
    join_at = Column(Float, default=0)    # когда игрок «появился» в лобби (у бота — в будущем)
    last_seen = Column(Float, default=0)
    __table_args__ = (UniqueConstraint("room_id", "slot", name="uq_event_slot"),)


class EventMatch(Base):
    __tablename__ = "event_matches_v2"
    id = Column(Integer, primary_key=True)
    room_id = Column(String(64), unique=True, index=True)
    status = Column(String(16), default="NOT_READY")
    winner = Column(String, default="")
    turn = Column(Integer, default=0)
    turn_end_time = Column(BigInteger, default=0)
    team1_glitch_until = Column(BigInteger, default=0)
    team2_glitch_until = Column(BigInteger, default=0)
    team1_protection_until = Column(BigInteger, default=0)
    team2_protection_until = Column(BigInteger, default=0)
    team1_hidden_until = Column(BigInteger, default=0)
    team2_hidden_until = Column(BigInteger, default=0)
    created_at = Column(Float, default=0)
    last_seen = Column(Float, default=0)


class EventStat(Base):  # ресурсы каждого игрока матча
    __tablename__ = "event_stats_v2"
    id = Column(Integer, primary_key=True)
    room_id = Column(String(64), index=True)
    team = Column(Integer)
    idx = Column(Integer)
    name = Column(String)
    skulls = Column(Integer, default=50)
    energy = Column(Integer, default=0)
    rating = Column(Integer, default=0)
    __table_args__ = (UniqueConstraint("room_id", "team", "idx", name="uq_event_stat"),)


Base.metadata.create_all(bind=engine)

# ───────────────────────────── ОБЩЕЕ ─────────────────────────────

# тайминги (секунды), всё в одном месте
WAITING_HOST_FRESH = 30      # 1x1: хост считается «живым», если опрашивал комнату не позже
WAITING_ROOM_TTL = 60        # 1x1: комната без опроса дольше этого удаляется
READY_ROOM_IDLE = 240        # 1x1: матч, в который никто не заглядывал, удаляется
FINISHED_KEEP = 120          # завершённые матчи живут ещё столько (чтобы оба увидели победителя)
EVENT_RUNNING_IDLE = 600     # 2x2: матч без опросов удаляется
EVENT_HUMAN_TTL = 20         # 2x2 лобби: человек, не опрашивающий лобби, выпадает из комнаты
JANITOR_INTERVAL = 30

# сезонное лобби
LOBBY_SECONDS = int(os.getenv("EVENT_LOBBY_SECONDS", "30"))
BOTS_NOT_BEFORE = 0.25       # доля окна ожидания, ДО которой ботов нет вообще (людям — приоритет)
BOTS_GAP = (1.5, 4.0)        # пауза между появлениями ботов, сек
FULL_ROOM_START_DELAY = 4    # если собрались 4 живых игрока — старт через столько секунд
HUMAN_FILL_ORDER = [0, 2, 1, 3]       # люди расходятся по разным командам
SLOT_TEAM = {0: 1, 1: 1, 2: 2, 3: 2}

_locks = defaultdict(threading.RLock)
_locks_guard = threading.Lock()
MM_LOCK = threading.RLock()  # матчмейкинг (поиск/создание комнат)


def room_lock(key: str):
    with _locks_guard:
        return _locks[key]


def drop_lock(key: str):
    with _locks_guard:
        _locks.pop(key, None)


def now() -> float:
    return time.time()


def touch(obj, t=None, every=5.0):
    """Обновить last_seen не чаще раза в `every` секунд — чтобы опросы не забивали БД записями."""
    t = t or now()
    if t - (obj.last_seen or 0) >= every:
        obj.last_seen = t


def ensure_player(db, player_id: str):
    p = db.query(Player).filter(Player.player_id == player_id).first()
    if p:
        return p
    try:
        with db.begin_nested():
            p = Player(player_id=player_id, total_ex=0)
            db.add(p)
            db.flush()
    except IntegrityError:
        p = db.query(Player).filter(Player.player_id == player_id).first()
    return p


# ───────────────────────────── 1 НА 1: ПОЛЕ 5x5 ─────────────────────────────

CELL_TYPES = [
    "blue_gravity", "blue_hydralic", "blue_light", "brown_passcode",
    "brown_storage", "green_vacant", "realise", "red_base", "red_exit",
    "red_flamew", "red_heal", "red_occupied", "yellow_atomic",
    "yellow_energy", "yellow_thri_line", "yellow_two_line",
]


def generate_map():
    pool = [t for t in CELL_TYPES for _ in range(2)]
    random.shuffle(pool)
    return [pool[i] if i < len(pool) else random.choice(CELL_TYPES) for i in range(25)]


def get_neighbors(idx, size=5):
    row, col = divmod(idx, size)
    res = []
    if row > 0:
        res.append(idx - size)
    if row < size - 1:
        res.append(idx + size)
    if col > 0:
        res.append(idx - 1)
    if col < size - 1:
        res.append(idx + 1)
    return res


def get_match_row(db, room_id):
    return db.query(Match).filter(Match.room_id == room_id).first()


def load_cells(db, scope, room_id):
    rows = (db.query(Cell).filter(Cell.scope == scope, Cell.room_id == room_id)
            .order_by(Cell.idx).all())
    return rows


def delete_cells(db, scope, room_id):
    db.query(Cell).filter(Cell.scope == scope, Cell.room_id == room_id).delete()


def new_match(db, room):
    m = Match(room_id=room.room_id, host_id=room.host_id, guest_id=room.guest_id,
              status="HOST_PICK", created_at=now())
    db.add(m)
    for i, t in enumerate(generate_map()):
        db.add(Cell(scope="m", room_id=room.room_id, idx=i, type=t))
    return m


def match_dict(m, cells):
    return {
        "status": m.status, "host_id": m.host_id, "guest_id": m.guest_id,
        "host_start": m.host_start, "guest_start": m.guest_start, "winner": m.winner,
        "host_glitch_until": m.host_glitch_until, "guest_glitch_until": m.guest_glitch_until,
        "host_protection_until": m.host_protection_until,
        "guest_protection_until": m.guest_protection_until,
        "host_hidden_until": m.host_hidden_until, "guest_hidden_until": m.guest_hidden_until,
        "host_data": {"skulls": m.host_skulls, "energy": m.host_energy, "rating": m.host_rating},
        "guest_data": {"skulls": m.guest_skulls, "energy": m.guest_energy, "rating": m.guest_rating},
        "cells": [{"type": c.type, "owner": c.owner, "ability_spent": bool(c.ability_spent),
                   "cooldown_end": c.cooldown_end, "revealed_host": bool(c.rev_a),
                   "revealed_guest": bool(c.rev_b)} for c in cells],
    }


def delete_room(db, room):
    delete_cells(db, "m", room.room_id)
    db.query(Match).filter(Match.room_id == room.room_id).delete()
    db.delete(room)
    drop_lock(room.room_id)


# ───────────────────────────── СЕЗОННЫЙ РЕЖИМ 2x2 ─────────────────────────────

ABILITY_STATS_2V2 = {  # [энергия, перезарядка (-1 = одноразовая, постоянный эффект)]
    "yellow_two_line": [0, 5], "brown_storage": [0, 5], "realise": [0, 5],
    "red_heal": [60, 10], "blue_hydralic": [10, 4], "green_vacant": [10, 5],
    "red_base": [10, 20], "red_exit": [10, 6], "red_flamew": [20, 20],
    "red_occupied": [20, -1], "yellow_thri_line": [15, 10], "yellow_energy": [25, 5],
    "blue_gravity": [20, 15], "blue_light": [20, -1], "brown_passcode": [25, -1],
    "yellow_atomic": [25, 20], "brown_useless": [0, 0], "blue_block": [0, 0],
}
EVENT_SIZE = 7
EVENT_CELLS = 49
EVENT_STARTS = {1: [0, 6], 2: [42, 48]}


def event_neighbors(idx):
    return get_neighbors(idx, EVENT_SIZE)


def ev_room(db, room_id):
    return db.query(EventRoom).filter(EventRoom.room_id == room_id).first()


def ev_match(db, room_id):
    return db.query(EventMatch).filter(EventMatch.room_id == room_id).first()


def cell2_dict(c):
    return {"type": c.type, "owner": c.owner, "ability_spent": bool(c.ability_spent),
            "cooldown_end": c.cooldown_end, "revealed_team1": bool(c.rev_a),
            "revealed_team2": bool(c.rev_b), "blocked_directions": json.loads(c.blocked or "[]")}


def event_state_dict(db, em):
    stats = (db.query(EventStat).filter(EventStat.room_id == em.room_id)
             .order_by(EventStat.team, EventStat.idx).all())
    st = {
        "status": em.status, "winner": em.winner, "turn": em.turn, "turn_end_time": em.turn_end_time,
        "team1_glitch_until": em.team1_glitch_until, "team2_glitch_until": em.team2_glitch_until,
        "team1_protection_until": em.team1_protection_until,
        "team2_protection_until": em.team2_protection_until,
        "team1_hidden_until": em.team1_hidden_until, "team2_hidden_until": em.team2_hidden_until,
    }
    for team in (1, 2):
        mine = [s for s in stats if s.team == team]
        st[f"team{team}_players"] = [s.name for s in mine]
        st[f"team{team}_skulls"] = [s.skulls for s in mine]
        st[f"team{team}_energy"] = [s.energy for s in mine]
        st[f"team{team}_rating"] = [s.rating for s in mine]
    st["cells"] = [cell2_dict(c) for c in load_cells(db, "e", em.room_id)]
    return st


def _num(v, default=0):
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def set_event_state(db, em, state: dict):
    """Полная замена состояния матча тем, что прислал клиент-хост (разложено по таблицам)."""
    room = ev_room(db, em.room_id)
    if room:  # состав команд — истина с сервера, а не то, что насчитал клиент
        roster = event_roster(db, room.room_id)
        for team in (1, 2):
            if roster[team]:
                state[f"team{team}_players"] = [s.name for s in roster[team]]
    em.status = str(state.get("status", "RUNNING"))
    em.winner = str(state.get("winner", "") or "")
    em.turn = _num(state.get("turn"))
    em.turn_end_time = _num(state.get("turn_end_time"))
    for team in (1, 2):
        for k in ("glitch", "protection", "hidden"):
            setattr(em, f"team{team}_{k}_until", _num(state.get(f"team{team}_{k}_until")))
    db.query(EventStat).filter(EventStat.room_id == em.room_id).delete()
    for team in (1, 2):
        names = state.get(f"team{team}_players", []) or []
        for i, name in enumerate(names):
            def pick(key, default):
                arr = state.get(f"team{team}_{key}", []) or []
                return _num(arr[i], default) if i < len(arr) else default
            db.add(EventStat(room_id=em.room_id, team=team, idx=i, name=str(name),
                             skulls=pick("skulls", 50), energy=pick("energy", 0),
                             rating=pick("rating", 0)))
    delete_cells(db, "e", em.room_id)
    for i, c in enumerate((state.get("cells") or [])[:EVENT_CELLS]):
        db.add(Cell(scope="e", room_id=em.room_id, idx=i, type=str(c.get("type", "brown_useless")),
                    owner=_num(c.get("owner")), ability_spent=bool(c.get("ability_spent", False)),
                    cooldown_end=_num(c.get("cooldown_end")),
                    rev_a=bool(c.get("revealed_team1", False)),
                    rev_b=bool(c.get("revealed_team2", False)),
                    blocked=json.dumps(c.get("blocked_directions", []))))
    db.flush()


# ---- лобби ----

def new_room_code(db):
    alphabet = "".join(ch for ch in string.ascii_uppercase if ch not in "OI") + "23456789"
    for _ in range(50):
        code = "".join(random.choice(alphabet) for _ in range(6))
        if not db.query(EventRoom).filter(EventRoom.room_code == code).first():
            return code
    return "".join(random.choice(alphabet) for _ in range(8))


def ev_slots(db, room_id):
    return (db.query(EventSlot).filter(EventSlot.room_id == room_id)
            .order_by(EventSlot.slot).all())


def event_roster(db, room_id):
    """{1: [слоты команды 1], 2: [слоты команды 2]} — упорядочено по номеру места."""
    res = {1: [], 2: []}
    for s in ev_slots(db, room_id):
        res[SLOT_TEAM[s.slot]].append(s)
    return res


BOT_MODELS = ["SM-S918B", "SM-A546B", "SM-G991B", "SM-A137F", "Pixel_7", "Pixel_8_Pro",
              "Redmi_Note_12", "M2101K6G", "2201117TG", "CPH2451", "RMX3393", "V2250",
              "moto_g54_5G", "Nokia_G42", "iPhone15,2", "iPhone13,4", "ONEPLUS_A6013"]


def make_bot_name(taken):
    # формат как у настоящих ников: модель_5 символов из device_id
    for _ in range(30):
        name = f"{random.choice(BOT_MODELS)}_{''.join(random.choice('0123456789abcdef') for _ in range(5))}"
        if name not in taken:
            return name
    return f"Player_{random.randint(10000, 99999)}"


def make_bot_rating(humans):
    rated = [h.rating for h in humans if h.rating and h.rating > 0]
    if rated:  # рядом со средним рейтингом живых игроков
        avg = sum(rated) / len(rated)
        return max(0, int(round(random.gauss(avg, max(120.0, avg * 0.2)))))
    return int(random.triangular(100, 2000, 600))


def purge_silent_humans(db, room, t):
    """Только пока лобби открыто: люди, которые перестали опрашивать, считаются ушедшими."""
    if room.status != "WAITING":
        return
    for s in ev_slots(db, room.room_id):
        if not s.is_bot and t - (s.last_seen or 0) > EVENT_HUMAN_TTL:
            db.delete(s)
    db.flush()
    humans = [s for s in ev_slots(db, room.room_id) if not s.is_bot]
    if humans and room.host_name not in [h.name for h in humans]:
        room.host_name = min(humans, key=lambda h: h.join_at).name


def claim_slot(db, room, name, rating, t):
    """Посадить человека: свободное место, иначе вытеснить бота (сначала ещё «не появившегося»)."""
    slots = ev_slots(db, room.room_id)
    taken = {s.slot: s for s in slots}
    target = next((sl for sl in HUMAN_FILL_ORDER if sl not in taken), None)
    if target is None:
        bots = [s for s in slots if s.is_bot]
        if not bots:
            return None
        pending = [b for b in bots if b.join_at > t]
        victim = min(pending or bots, key=lambda b: HUMAN_FILL_ORDER.index(b.slot))
        target = victim.slot
        db.delete(victim)
        db.flush()
    s = EventSlot(room_id=room.room_id, slot=target, name=name, rating=int(rating or 0),
                  is_bot=False, join_at=t, last_seen=t)
    db.add(s)
    db.flush()
    humans = [x for x in ev_slots(db, room.room_id) if not x.is_bot]
    if len(humans) == 4:  # комната собрана людьми — не заставляем ждать весь таймер
        room.start_at = min(room.start_at, t + FULL_ROOM_START_DELAY)
    return s


def plan_bots(db, room, t):
    """
    Реалистичное появление ботов:
      * первые 25% окна ожидания ботов нет совсем — места остаются людям;
      * у каждого бота СВОЁ случайное время появления, между ботами пауза 1.5–4 c;
      * рейтинг бота — около среднего рейтинга живых игроков, ник — в формате настоящих;
      * человек, пришедший позже, вытесняет бота (бот «выходит из лобби»).
    """
    if room.status != "WAITING":
        return
    window = max(1.0, room.start_at - room.created_at)
    earliest = room.created_at + window * BOTS_NOT_BEFORE
    slots = ev_slots(db, room.room_id)
    occupied = {s.slot for s in slots}
    empty = [sl for sl in HUMAN_FILL_ORDER if sl not in occupied]
    if not empty or t < earliest:
        return
    humans = [s for s in slots if not s.is_bot]
    taken_names = {s.name for s in slots}
    deadline = max(t, room.start_at - 1.0)       # последний бот — не позже, чем за секунду до старта
    cursor = max(t, earliest)
    bot_times = [s.join_at for s in slots if s.is_bot]
    if bot_times:                                 # не сразу после уже запланированного бота
        cursor = max(cursor, max(bot_times) + random.uniform(*BOTS_GAP))
    cursor = min(cursor, deadline)
    times = sorted(random.uniform(cursor, deadline) for _ in empty)
    prev = cursor - random.uniform(*BOTS_GAP)
    for slot, when in zip(empty, times):
        when = min(max(when, prev + random.uniform(*BOTS_GAP)), deadline)
        prev = when
        name = make_bot_name(taken_names)
        taken_names.add(name)
        db.add(EventSlot(room_id=room.room_id, slot=slot, name=name,
                         rating=make_bot_rating(humans), is_bot=True, join_at=when, last_seen=when))
    db.flush()


def start_event_room(db, room, t):
    """Закрыть лобби: добить пустые места ботами, все «появившиеся»; комната → RUNNING."""
    if room.status != "WAITING":
        return
    slots = ev_slots(db, room.room_id)
    occupied = {s.slot for s in slots}
    humans = [s for s in slots if not s.is_bot]
    taken_names = {s.name for s in slots}
    for s in slots:
        if s.is_bot and s.join_at > t:
            s.join_at = t
    for sl in HUMAN_FILL_ORDER:
        if sl not in occupied:
            name = make_bot_name(taken_names)
            taken_names.add(name)
            db.add(EventSlot(room_id=room.room_id, slot=sl, name=name,
                             rating=make_bot_rating(humans), is_bot=True, join_at=t, last_seen=t))
    room.status = "RUNNING"
    room.started_at = t
    room.last_seen = t
    db.flush()


def visible_players(db, room, t):
    out = []
    for s in ev_slots(db, room.room_id):
        if s.join_at <= t:
            out.append({"name": s.name, "rating": s.rating, "team": SLOT_TEAM[s.slot],
                        "slot": s.slot, "is_bot": bool(s.is_bot), "is_host": s.name == room.host_name})
    out.sort(key=lambda p: (p["team"], p["slot"]))
    return out


def lobby_dict(db, room, t, me=None):
    players = visible_players(db, room, t)
    started = room.status != "WAITING"
    d = {
        "status": "ok", "room_id": room.room_id, "room_code": room.room_code,
        "lobby_state": room.status, "server_time": t, "start_at": room.start_at,
        "seconds_left": 0 if started else max(0, int(math.ceil(room.start_at - t))),
        "host": room.host_name, "is_host": bool(me and me == room.host_name),
        "players": players, "game_started": started,
    }
    if started:
        roster = event_roster(db, room.room_id)
        d["team1_players"] = [s.name for s in roster[1]]
        d["team2_players"] = [s.name for s in roster[2]]
    return d


def remove_human(db, room, name, t):
    """Игрок вышел из лобби/комнаты."""
    for s in ev_slots(db, room.room_id):
        if s.name == name and not s.is_bot:
            db.delete(s)
    db.flush()
    humans = [s for s in ev_slots(db, room.room_id) if not s.is_bot]
    if room.status == "WAITING":
        if not humans:
            delete_event_room(db, room)
            return "deleted"
        if room.host_name == name:
            room.host_name = min(humans, key=lambda h: h.join_at).name
    return "left"


def delete_event_room(db, room):
    rid = room.room_id
    db.query(EventSlot).filter(EventSlot.room_id == rid).delete()
    db.query(EventStat).filter(EventStat.room_id == rid).delete()
    db.query(EventMatch).filter(EventMatch.room_id == rid).delete()
    delete_cells(db, "e", rid)
    db.delete(room)
    drop_lock(rid)


def leave_other_event_rooms(db, name, keep_room_id=None, t=None):
    for s in db.query(EventSlot).filter(EventSlot.name == name, EventSlot.is_bot == False).all():  # noqa: E712
        if s.room_id == keep_room_id:
            continue
        r = ev_room(db, s.room_id)
        if r and r.status == "WAITING":
            remove_human(db, r, name, t or now())
        elif not r:
            db.delete(s)


# ───────────────────────────── УБОРЩИК ─────────────────────────────

def cleanup(db, t=None):
    """Единственное место, где чистятся «висячие» комнаты. Вызывается фоново и по /cleanup_old_rooms."""
    t = t or now()
    removed = {"rooms": 0, "event_rooms": 0}

    for r in db.query(Room).all():
        dead = False
        if r.status == "WAITING" and t - (r.last_seen or 0) > WAITING_ROOM_TTL:
            dead = True
        elif r.status == "READY" and t - (r.last_seen or 0) > READY_ROOM_IDLE:
            dead = True
        elif r.status == "FINISHED" and t - (r.last_seen or 0) > FINISHED_KEEP:
            dead = True
        if dead:
            delete_room(db, r)
            removed["rooms"] += 1
    live = {r.room_id for r in db.query(Room).all()}
    for m in db.query(Match).all():  # матчи-сироты
        if m.room_id not in live:
            delete_cells(db, "m", m.room_id)
            db.delete(m)

    for r in db.query(EventRoom).all():
        dead = False
        if r.status == "WAITING":
            purge_silent_humans(db, r, t)
            humans = [s for s in ev_slots(db, r.room_id) if not s.is_bot]
            dead = not humans
        elif r.status == "RUNNING":
            dead = t - (r.last_seen or 0) > EVENT_RUNNING_IDLE
        elif r.status == "FINISHED":
            dead = t - (r.finished_at or r.last_seen or 0) > FINISHED_KEEP
        if dead:
            delete_event_room(db, r)
            removed["event_rooms"] += 1
    live_ev = {r.room_id for r in db.query(EventRoom).all()}
    for m in db.query(EventMatch).all():  # матч создаётся раньше/без комнаты — даём час
        if m.room_id not in live_ev and t - (m.last_seen or 0) > 3600:
            db.query(EventStat).filter(EventStat.room_id == m.room_id).delete()
            delete_cells(db, "e", m.room_id)
            db.delete(m)

    with _relay_lock:
        for k in [k for k, (ts, _s) in _relay.items() if t - ts > 600]:
            _relay.pop(k, None)
    return removed


def run_cleanup():
    with MM_LOCK, session() as db:
        res = cleanup(db)
    if res["rooms"] or res["event_rooms"]:
        log.info("cleanup: %s", res)
    return res


@asynccontextmanager
async def lifespan(_app):
    try:
        await asyncio.to_thread(run_cleanup)
    except Exception:
        log.exception("startup cleanup failed")

    async def janitor():
        while True:
            await asyncio.sleep(JANITOR_INTERVAL)
            try:
                await asyncio.to_thread(run_cleanup)
            except Exception:
                log.exception("janitor failed")

    task = asyncio.create_task(janitor())
    yield
    task.cancel()


app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
api = APIRouter()

_relay = {}  # (room_id, player_id) -> (время, состояние): мгновенный обмен состоянием, не хранится в БД
_relay_lock = threading.Lock()

# ───────────────────────────── ЭНДПОИНТЫ: ИГРОКИ ─────────────────────────────


@app.get("/")
def root():
    return {"status": "server_online"}


@api.get("/health")
def health():
    return {"status": "ok", "time": now()}


# ───────────── НАСТРОЙКИ ОФОРМЛЕНИЯ (совместимость со старым API) ─────────────
# Значения сохраняются в существующей таблице kv_store, поэтому не теряются
# при перезапуске Render. Установите ADMIN_CONTENT_TOKEN на Render, чтобы
# ограничить изменение настроек: ?admin_token=... (старые клиенты без токена
# продолжают читать /get_* как раньше).
from fastapi import HTTPException

CONTENT_DEFAULTS = {
    "ui_background_url": "",
    "ui_event_text": "",
    "ui_event_info": "Удвоенные очки за победу в событии!",
    "ui_event_image_url": "https://your-server.com/images/event_banner.png",
}


def _content_read(tag: str) -> str:
    with session() as db:
        row = db.query(Kv).filter(Kv.tag == tag).first()
        return row.value if row else CONTENT_DEFAULTS[tag]


def _content_write(tag: str, value: str) -> None:
    with session() as db:
        row = db.query(Kv).filter(Kv.tag == tag).first()
        if row:
            row.value, row.updated_at = value, now()
        else:
            db.add(Kv(tag=tag, value=value, updated_at=now()))


def _content_admin_guard(admin_token: str = "") -> None:
    expected = os.getenv("ADMIN_CONTENT_TOKEN", "") or os.getenv("ADMIN_PANEL_TOKEN", "")
    if expected:
        import hmac
        if not hmac.compare_digest(admin_token, expected):
            raise HTTPException(status_code=403, detail="admin_token_required")


def _valid_url(url: str) -> bool:
    from urllib.parse import urlsplit
    p = urlsplit(url)
    return p.scheme in ("https", "http") and bool(p.netloc)


@api.get("/get_background")
def get_background():
    return {"background_url": _content_read("ui_background_url")}


@api.get("/set_background")
def set_background(url: str, admin_token: str = ""):
    _content_admin_guard(admin_token)
    if url and not _valid_url(url):
        raise HTTPException(status_code=400, detail="invalid_url")
    _content_write("ui_background_url", url)
    return {"status": "ok", "background_url": url}


@api.get("/get_event_info")
def get_event_info():
    return {"event_text": _content_read("ui_event_text")}


@api.get("/set_event_info")
def set_event_info(text: str, admin_token: str = ""):
    _content_admin_guard(admin_token)
    if len(text) > 4000:
        raise HTTPException(status_code=400, detail="text_too_long")
    _content_write("ui_event_text", text)
    return {"status": "ok", "event_text": text}


@api.get("/get_event")
def get_event():
    return {
        "info": _content_read("ui_event_info"),
        "image_url": _content_read("ui_event_image_url"),
    }


@api.get("/set_event")
def set_event(info: str = "", image_url: str = "", admin_token: str = ""):
    _content_admin_guard(admin_token)
    if len(info) > 4000:
        raise HTTPException(status_code=400, detail="text_too_long")
    if image_url and not _valid_url(image_url):
        raise HTTPException(status_code=400, detail="invalid_url")
    # Точно как старый сервер: пустые параметры не перезаписывают значения.
    if info:
        _content_write("ui_event_info", info)
    if image_url:
        _content_write("ui_event_image_url", image_url)
    return {"status": "ok", "event_data": get_event()}


# ─── Панель администратора: баны и удаление комнат ───
class AdminBan(Base):
    __tablename__ = "admin_bans"
    player_id = Column(String(255), primary_key=True)
    created_at = Column(Float, default=0)


Base.metadata.create_all(bind=engine, tables=[AdminBan.__table__])

def _admin_guard(token: str):
    import hmac
    secret = os.getenv("ADMIN_PANEL_TOKEN", "")
    if not secret or not hmac.compare_digest(token, secret):
        raise HTTPException(status_code=403, detail="admin_auth_required")


def _banned(db, player_id: str) -> bool:
    return bool(player_id and db.query(AdminBan).filter(AdminBan.player_id == player_id).first())


@api.get("/admin_bans")
def admin_bans(admin_token: str = ""):
    _admin_guard(admin_token)
    with session() as db:
        return {"players": [r.player_id for r in db.query(AdminBan).order_by(AdminBan.created_at.desc()).all()]}


@api.get("/admin_set_ban")
def admin_set_ban(player_id: str, banned: bool = True, admin_token: str = ""):
    _admin_guard(admin_token)
    player_id = player_id.strip()
    if not player_id or len(player_id) > 255:
        raise HTTPException(status_code=400, detail="invalid_player_id")
    with MM_LOCK, session() as db:
        row = db.query(AdminBan).filter(AdminBan.player_id == player_id).first()
        if banned and not row:
            db.add(AdminBan(player_id=player_id, created_at=now()))
        if not banned and row:
            db.delete(row)
        if banned:
            for room in db.query(Room).filter((Room.host_id == player_id) | (Room.guest_id == player_id)).all():
                delete_room(db, room)
            # Удаляем из сезонных лобби; игровые комнаты других участников остаются.
            for slot in db.query(EventSlot).filter(EventSlot.name == player_id, EventSlot.is_bot == False).all():
                room = db.query(EventRoom).filter(EventRoom.room_id == slot.room_id).first()
                if room:
                    remove_human(db, room, player_id, now())
    return {"status": "ok", "player_id": player_id, "banned": banned}


@api.get("/admin_delete_room")
def admin_delete_room(room_id: str, admin_token: str = ""):
    _admin_guard(admin_token)
    with MM_LOCK, session() as db:
        r = db.query(Room).filter(Room.room_id == room_id).first()
        if r:
            delete_room(db, r)
            return {"status": "deleted", "mode": "1x1"}
        er = db.query(EventRoom).filter(EventRoom.room_id == room_id).first()
        if er:
            delete_event_room(db, er)
            return {"status": "deleted", "mode": "2x2"}
    return {"status": "not_found"}


@api.get("/get_player")
def get_player(player_id: str):
    with session() as db:
        p = ensure_player(db, player_id)
        return {"status": "success", "player_id": p.player_id, "total_ex": p.total_ex}


@api.get("/update_score")
def update_score(player_id: str, add_score: int):
    with session() as db:
        ensure_player(db, player_id)
        new_total = Player.total_ex + add_score
        db.execute(update(Player).where(Player.player_id == player_id)
                   .values(total_ex=case((new_total < 0, 0), else_=new_total)))
        total = db.query(Player.total_ex).filter(Player.player_id == player_id).scalar()
    log.info("[UPDATE SCORE] %s %+d = %s", player_id, add_score, total)
    return {"status": "updated", "total_ex": total}


@api.get("/store_data")
def store_data(tag: str, value: str):
    """Магазин присылает tag = "ex_<имя>-<device_id>" и новое значение рейтинга."""
    if tag.startswith("ex_"):
        rest = tag[3:]
        cuts = [i for i, ch in enumerate(rest) if ch == "-"]
        candidates = [rest[:i] for i in cuts] + [rest]  # имя игрока само может содержать "-"
        try:
            new_val = max(0, int(value))
        except ValueError:
            return {"status": "bad_value"}
        with session() as db:
            found = (db.query(Player).filter(Player.player_id.in_(candidates)).all())
            if found:
                p = max(found, key=lambda x: len(x.player_id))
            else:
                p = ensure_player(db, candidates[0] if candidates else rest)
            p.total_ex = new_val
        return {"status": "updated", "total_ex": new_val}
    with session() as db:
        row = db.query(Kv).filter(Kv.tag == tag).first()
        if row:
            row.value, row.updated_at = value, now()
        else:
            db.add(Kv(tag=tag, value=value, updated_at=now()))
    return {"status": "updated"}


def _top(n):
    with session() as db:
        rows = db.query(Player).order_by(Player.total_ex.desc()).limit(n).all()
        return [{"player_id": p.player_id, "total_ex": p.total_ex} for p in rows]


@api.get("/get_rating")
def get_rating():
    return _top(20)


@api.get("/get_rating_100")
def get_rating_100():
    return _top(100)


# ───────────────────────────── ЭНДПОИНТЫ: КОМНАТЫ 1 НА 1 ─────────────────────────────

def _abandon_old_rooms(db, player_id, t):
    """Игрок ищет новую игру — его старые комнаты больше никому не нужны."""
    for r in db.query(Room).filter((Room.host_id == player_id) | (Room.guest_id == player_id)).all():
        if r.status == "WAITING" or r.status == "READY":
            delete_room(db, r)


@api.get("/find_or_create_room")
def find_or_create_room(player_id: str):
    with session() as ban_db:
        if _banned(ban_db, player_id):
            # Совместимо с Lobby.gd: ожидает 15 секунд и открывает Bot.tscn.
            return {"status": "created", "role": "host", "room_id": "bot_wait_" + player_id[:40]}
    with MM_LOCK, session() as db:
        t = now()
        cleanup_waiting = db.query(Room).filter(
            Room.status == "WAITING", Room.last_seen < t - WAITING_ROOM_TTL).all()
        for r in cleanup_waiting:
            delete_room(db, r)

        own = db.query(Room).filter(Room.host_id == player_id, Room.status == "WAITING").first()
        if own:  # уже ждёт соперника — не плодим вторую комнату
            own.last_seen = t
            return {"status": "created", "role": "host", "room_id": own.room_id}

        for r in db.query(Room).filter((Room.host_id == player_id) | (Room.guest_id == player_id)).all():
            if r.status == "READY":
                delete_room(db, r)  # прошлая незавершённая партия брошена
        db.flush()

        waiting = (db.query(Room)
                   .filter(Room.status == "WAITING", Room.host_id != player_id,
                           Room.last_seen >= t - WAITING_HOST_FRESH)
                   .order_by(Room.created_at).first())
        if waiting:
            with room_lock(waiting.room_id):
                waiting.guest_id = player_id
                waiting.status = "READY"
                waiting.last_seen = t
                if not get_match_row(db, waiting.room_id):
                    new_match(db, waiting)
                return {"status": "joined", "role": "guest", "room_id": waiting.room_id}

        room_id = f"room_{int(t)}_{random.randint(1000, 9999)}"
        db.add(Room(room_id=room_id, host_id=player_id, guest_id="", status="WAITING",
                    created_at=t, last_seen=t))
        return {"status": "created", "role": "host", "room_id": room_id}


@api.get("/cancel_room")
def cancel_room(room_id: str):
    with session() as db:
        room = db.query(Room).filter(Room.room_id == room_id).first()
        if room:
            delete_room(db, room)
    return {"status": "cancelled"}


@api.get("/leave_room")
def leave_room(room_id: str, player_id: str = ""):
    """Игрок покинул экран матча. Без player_id — просто убираем ждущую комнату."""
    with room_lock(room_id), session() as db:
        room = db.query(Room).filter(Room.room_id == room_id).first()
        if not room:
            return {"status": "left"}
        m = get_match_row(db, room_id)
        if room.status == "WAITING":
            delete_room(db, room)
        elif m and m.winner:
            room.status = "FINISHED"  # соперник ещё может дочитать итог; уберёт уборщик
            room.last_seen = now()
        elif player_id and m and player_id in (m.host_id, m.guest_id):
            # ушёл посреди партии — техническое поражение
            m.winner = m.guest_id if player_id == m.host_id else m.host_id
            room.status = "FINISHED"
            room.last_seen = now()
        return {"status": "left"}


@api.get("/check_room_status")
def check_room_status(room_id: str):
    if room_id.startswith("bot_wait_"):
        return {"status": "success", "room_status": "WAITING"}
    with session() as db:
        room = db.query(Room).filter(Room.room_id == room_id).first()
        if not room:
            return {"status": "not_found"}
        touch(room)
        return {"status": "success", "room_status": room.status}


@api.get("/create_match")
def create_match(room_id: str):
    with room_lock(room_id), session() as db:
        room = db.query(Room).filter(Room.room_id == room_id).first()
        if not room:
            return {"status": "room_not_found"}
        if get_match_row(db, room_id):
            return {"status": "already_exists"}
        new_match(db, room)
        return {"status": "created"}


@api.get("/get_match")
def get_match(room_id: str):
    with session() as db:
        m = get_match_row(db, room_id)
        if not m:
            return {"status": "not_found"}
        room = db.query(Room).filter(Room.room_id == room_id).first()
        if room:
            touch(room)
        return match_dict(m, load_cells(db, "m", room_id))


@api.get("/pick_start")
def pick_start(room_id: str, player_id: str, cell_index: int):
    if cell_index < 0 or cell_index >= 25:
        return {"status": "bad_index"}
    with room_lock(room_id), session() as db:
        m = get_match_row(db, room_id)
        if not m:
            return {"status": "not_found"}
        cells = load_cells(db, "m", room_id)

        if m.status == "HOST_PICK":
            if player_id != m.host_id:
                return {"status": "wait_host"}
            m.host_start = cell_index
            m.status = "GUEST_PICK"
            return {"status": "ok"}

        if m.status == "GUEST_PICK":
            if player_id != m.guest_id:
                return {"status": "wait_guest"}
            if cell_index == m.host_start:
                m.host_start = m.guest_start = -1
                m.status = "HOST_PICK"
                return {"status": "same_cell"}
            m.guest_start = cell_index
            hs, gs = m.host_start, m.guest_start
            cells[hs].owner, cells[hs].type = 1, "yellow_two_line"
            cells[gs].owner, cells[gs].type = 2, "yellow_two_line"
            host_nb = [n for n in get_neighbors(hs) if n != gs and cells[n].owner == 0]
            guest_nb = [n for n in get_neighbors(gs) if n != hs and cells[n].owner == 0]
            for n in host_nb:
                cells[n].rev_a = True
            for n in guest_nb:
                cells[n].rev_b = True
            host_storage = random.choice(host_nb) if host_nb else -1
            if host_storage >= 0:
                cells[host_storage].type = "brown_storage"
            avail = [n for n in guest_nb if n != host_storage]
            if avail:
                cells[random.choice(avail)].type = "brown_storage"
            m.status = "RUNNING"
            return {"status": "started"}
        return {"status": "already_running"}


@api.get("/capture_cell")
def capture_cell(room_id: str, player_id: str, cell_index: int):
    with room_lock(room_id), session() as db:
        m = get_match_row(db, room_id)
        if not m:
            return {"status": "no_match"}
        if m.winner != "":
            return {"status": "finished"}
        if cell_index < 0 or cell_index >= 25:
            return {"status": "bad_index"}
        if player_id not in (m.host_id, m.guest_id):
            return {"status": "not_in_match"}
        cells = load_cells(db, "m", room_id)
        is_host = player_id == m.host_id
        my, enemy = (1, 2) if is_host else (2, 1)
        cell = cells[cell_index]
        enemy_start = m.guest_start if is_host else m.host_start

        if cell.owner == my:
            return {"status": "already_owned"}
        # защита (red_flamew) принадлежит владельцу клетки: враг её защитил — нельзя
        enemy_protection = m.guest_protection_until if is_host else m.host_protection_until
        if cell.owner == enemy and enemy_protection > int(now()):
            return {"status": "protected"}
        if not any(cells[n].owner == my for n in get_neighbors(cell_index)):
            return {"status": "not_connected"}

        # стоимость (клиент списывает сам, сервер только сообщает)
        cost = 50 if cell.owner == 0 else 70
        mine_spent = lambda typ: any(c.owner == my and c.type == typ and c.ability_spent for c in cells)  # noqa: E731
        enemy_spent = lambda typ: any(c.owner == enemy and c.type == typ and c.ability_spent for c in cells)  # noqa: E731
        if cell.owner == 0 and mine_spent("red_occupied"):
            cost = 40
        if cell.owner == enemy:
            if mine_spent("brown_passcode"):
                cost = 50
            if enemy_spent("blue_light"):
                cost = 100  # у владельца включён blue_light — отбить клетку дороже

        gain = 1 if cell.owner == 0 else 2
        if is_host:
            m.host_rating += gain
        else:
            m.guest_rating += gain
        cell.owner = my
        cell.ability_spent = False
        if is_host:
            cell.rev_a = True
        else:
            cell.rev_b = True
        if cell_index == enemy_start:
            m.winner = player_id
        room = db.query(Room).filter(Room.room_id == room_id).first()
        if room:
            room.last_seen = now()
            if m.winner:
                room.status = "FINISHED"
        return {"status": "ok", "cost": cost, "winner": m.winner,
                "rating": m.host_rating if is_host else m.guest_rating}


def _enemy_cells(cells, enemy, m):
    return [c.idx for c in cells if c.owner == enemy and c.idx not in (m.host_start, m.guest_start)]


@api.get("/use_cell")
def use_cell(room_id: str, player_id: str, cell_index: int):
    with room_lock(room_id), session() as db:
        m = get_match_row(db, room_id)
        if not m:
            return {"status": "no_match"}
        if m.winner != "":
            return {"status": "finished"}
        if cell_index < 0 or cell_index >= 25:
            return {"status": "bad_index"}
        cells = load_cells(db, "m", room_id)
        is_host = player_id == m.host_id
        my, enemy = (1, 2) if is_host else (2, 1)
        cell = cells[cell_index]
        if cell.owner != my:
            return {"status": "not_owner"}
        typ, t = cell.type, int(now())

        if typ == "yellow_atomic":
            targets = _enemy_cells(cells, enemy, m)
            if targets:
                cells[random.choice(targets)].owner = 0
        elif typ == "green_vacant":
            hidden = [c for c in cells if c.owner == 0]
            random.shuffle(hidden)
            for c in hidden[:2]:
                if is_host:
                    c.rev_a = True
                else:
                    c.rev_b = True
        elif typ == "red_exit":
            if is_host:
                m.guest_hidden_until = t + 10
            else:
                m.host_hidden_until = t + 10
        elif typ == "yellow_thri_line":
            if is_host:
                m.guest_glitch_until = t + 10
            else:
                m.host_glitch_until = t + 10
        elif typ == "red_flamew":
            if is_host:
                m.host_protection_until = t + 10
            else:
                m.guest_protection_until = t + 10
        elif typ == "blue_hydralic":
            for n in get_neighbors(cell_index):
                if is_host:
                    cells[n].rev_a = True
                else:
                    cells[n].rev_b = True
        return {"status": "ok", "cell_type": typ}


@api.get("/atomic_strike")
def atomic_strike(room_id: str, player_id: str):
    """Сделать случайную вражескую клетку (кроме стартовых) нейтральной."""
    with room_lock(room_id), session() as db:
        m = get_match_row(db, room_id)
        if not m:
            return {"status": "no_match"}
        if m.winner != "":
            return {"status": "finished"}
        cells = load_cells(db, "m", room_id)
        enemy = 2 if player_id == m.host_id else 1
        targets = _enemy_cells(cells, enemy, m)
        if not targets:
            return {"status": "no_targets"}
        idx = random.choice(targets)
        cells[idx].owner = 0
        return {"status": "ok", "cell": idx}


@api.get("/apply_debuff")
def apply_debuff(room_id: str, player_id: str, debuff_type: str, duration: int = 10):
    """hide — скрыть клетки врага, glitch — глитч врагу, protect — защита своих клеток."""
    duration = max(1, min(int(duration), 60))
    with room_lock(room_id), session() as db:
        m = get_match_row(db, room_id)
        if not m:
            return {"status": "no_match"}
        is_host = player_id == m.host_id
        until = int(now()) + duration
        if debuff_type in ("hide", "hidden"):
            attr = "guest_hidden_until" if is_host else "host_hidden_until"
        elif debuff_type == "glitch":
            attr = "guest_glitch_until" if is_host else "host_glitch_until"
        elif debuff_type in ("protect", "protection", "shield"):
            attr = "host_protection_until" if is_host else "guest_protection_until"
        else:
            return {"status": "unknown_debuff"}
        setattr(m, attr, max(getattr(m, attr) or 0, until))
        return {"status": "ok", "until": until}


@api.get("/steal_skulls")
def steal_skulls(room_id: str, player_id: str, amount: int):
    with room_lock(room_id), session() as db:
        m = get_match_row(db, room_id)
        if not m:
            return {"status": "no_match"}
        if player_id == m.host_id:
            stolen = max(0, min(amount, m.guest_skulls))
            m.host_skulls += stolen
            m.guest_skulls -= stolen
        else:
            stolen = max(0, min(amount, m.host_skulls))
            m.guest_skulls += stolen
            m.host_skulls -= stolen
        return {"status": "ok", "stolen": stolen}


@api.get("/finish_match")
def finish_match(room_id: str):
    """Начислить рейтинг победителю (один раз!). Матч остаётся на пару минут — второй игрок увидит итог."""
    with room_lock(room_id), session() as db:
        m = get_match_row(db, room_id)
        if not m:
            return {"status": "no_match"}
        if m.winner == "":
            return {"status": "no_winner"}
        if not m.rewarded:
            rating = m.host_rating if m.winner == m.host_id else m.guest_rating
            p = ensure_player(db, m.winner)
            p.total_ex = (p.total_ex or 0) + rating
            m.rewarded = True
        room = db.query(Room).filter(Room.room_id == room_id).first()
        if room:
            room.status = "FINISHED"
            room.last_seen = now()
        return {"status": "finished", "winner": m.winner}


@api.get("/get_rooms")
def get_rooms():
    """Список комнат для админки (все значения — строки, как ждёт клиент)."""
    with session() as db:
        res = [{"room_id": r.room_id, "host_id": r.host_id or "", "guest_id": r.guest_id or "",
                "status": r.status or ""} for r in db.query(Room).order_by(Room.created_at.desc()).limit(100)]
        for r in db.query(EventRoom).order_by(EventRoom.created_at.desc()).limit(50):
            humans = sum(1 for s in ev_slots(db, r.room_id) if not s.is_bot)
            res.append({"room_id": r.room_id, "host_id": r.host_name or "",
                        "guest_id": f"event 2v2: {humans} чел.", "status": r.status or ""})
        return res


@api.get("/cleanup_old_rooms")
def cleanup_old_rooms():
    res = run_cleanup()
    return {"status": "cleaned", "deleted": res["rooms"] + res["event_rooms"], **res}


@api.post("/send_state")
def send_state(room_id: str, player_id: str, state: dict = Body(default={})):
    with _relay_lock:
        _relay[(room_id, player_id)] = (now(), state)
    return {"status": "ok"}


@api.get("/get_state")
def get_state(room_id: str, player_id: str):
    with session() as db:
        room = db.query(Room).filter(Room.room_id == room_id).first()
        if not room:
            return {"status": "no_room", "enemy_state": {}}
        enemy = room.guest_id if player_id == room.host_id else room.host_id
    with _relay_lock:
        ts_state = _relay.get((room_id, enemy))
    return {"status": "ok", "enemy_state": ts_state[1] if ts_state else {}}


# ───────────────────────────── ЭНДПОИНТЫ: СЕЗОН 2 НА 2 ─────────────────────────────

@api.get("/find_active_room")
def find_active_room(player_id: str = ""):
    """Есть ли открытое лобби, куда можно войти?"""
    with MM_LOCK, session() as db:
        t = now()
        for r in db.query(EventRoom).filter(EventRoom.status == "WAITING").order_by(EventRoom.created_at):
            purge_silent_humans(db, r, t)
            slots = ev_slots(db, r.room_id)
            humans = [s for s in slots if not s.is_bot]
            if not humans:
                continue
            mine = any(s.name == player_id for s in humans)
            if mine or (len(humans) < 4 and r.start_at - t > FULL_ROOM_START_DELAY):
                return {"room_found": True, "room_code": r.room_code, "room_id": r.room_id}
        return {"room_found": False}


@api.get("/create_event_room")
def create_event_room(player_id: str, room_id: str = "", rating: int = 0):
    with session() as check_db:
        if _banned(check_db, player_id):
            return {"status": "banned"}
    with MM_LOCK, session() as db:
        t = now()
        leave_other_event_rooms(db, player_id, None, t)
        db.flush()
        rid = room_id or f"ev_{int(t)}_{random.randint(1000, 9999)}"
        existing = ev_room(db, rid)
        if existing:
            return {"status": "already_exists", "room_id": rid, "room_code": existing.room_code,
                    "is_host": existing.host_name == player_id}
        room = EventRoom(room_id=rid, room_code=new_room_code(db), status="WAITING",
                         host_name=player_id, created_at=t, start_at=t + LOBBY_SECONDS, last_seen=t)
        db.add(room)
        db.flush()
        slot = claim_slot(db, room, player_id, rating, t)
        return {"status": "created", "room_id": rid, "room_code": room.room_code, "is_host": True,
                "slot": slot.slot, "team": SLOT_TEAM[slot.slot], "start_at": room.start_at,
                "server_time": t}


@api.get("/join_event_room")
def join_event_room(player_id: str, room_code: str = "", room_id: str = "", rating: int = 0):
    with session() as check_db:
        if _banned(check_db, player_id):
            return {"status": "banned"}
    with MM_LOCK, session() as db:
        t = now()
        q = db.query(EventRoom)
        room = (q.filter(EventRoom.room_code == room_code.upper()).first() if room_code
                else q.filter(EventRoom.room_id == room_id).first())
        if not room:
            return {"status": "not_found"}
        if room.status != "WAITING":
            return {"status": "already_started"}
        purge_silent_humans(db, room, t)
        mine = next((s for s in ev_slots(db, room.room_id) if s.name == player_id and not s.is_bot), None)
        if not mine:
            leave_other_event_rooms(db, player_id, room.room_id, t)
            mine = claim_slot(db, room, player_id, rating, t)
        if not mine:
            return {"status": "room_full"}
        mine.last_seen = t
        return {"status": "joined", "room_id": room.room_id, "room_code": room.room_code,
                "is_host": room.host_name == player_id, "slot": mine.slot,
                "team": SLOT_TEAM[mine.slot], "start_at": room.start_at, "server_time": t}


@api.get("/event_lobby")
def event_lobby(room_id: str, player_id: str = ""):
    """Опрос лобби раз в секунду: состав, таймер, старт. Сервер сам ведёт таймер и ботов."""
    with MM_LOCK, session() as db:
        room = ev_room(db, room_id)
        if not room:
            return {"status": "not_found"}
        t = now()
        if player_id:
            me = next((s for s in ev_slots(db, room_id) if s.name == player_id and not s.is_bot), None)
            if me:
                me.last_seen = t
            elif room.status == "WAITING":
                return {"status": "not_in_room"}
        if room.status == "WAITING":
            purge_silent_humans(db, room, t)
            if not [s for s in ev_slots(db, room_id) if not s.is_bot]:
                delete_event_room(db, room)
                return {"status": "not_found"}
            plan_bots(db, room, t)
            if t >= room.start_at:
                start_event_room(db, room, t)
        room.last_seen = t
        return lobby_dict(db, room, t, player_id)


@api.get("/leave_event_room")
def leave_event_room(room_id: str, player_id: str):
    with MM_LOCK, session() as db:
        room = ev_room(db, room_id)
        if not room:
            return {"status": "left"}
        return {"status": remove_human(db, room, player_id, now())}


@api.get("/cancel_event_room")
def cancel_event_room(room_id: str, player_id: str = ""):
    """С player_id — выход игрока. Без него (так зовёт игровой код) — закрыть, но не рубить чужую партию."""
    with MM_LOCK, session() as db:
        room = ev_room(db, room_id)
        if not room:
            return {"status": "cancelled"}
        if player_id:
            return {"status": remove_human(db, room, player_id, now())}
        if room.status in ("WAITING", "FINISHED"):
            delete_event_room(db, room)
            return {"status": "cancelled"}
        return {"status": "kept_running"}  # матч идёт — его уберёт уборщик, когда все уйдут


@api.get("/create_virtual_guest")
def create_virtual_guest(room_id: str = "", room_code: str = ""):
    """Тест/админка: сразу посадить в лобби видимого бота."""
    with MM_LOCK, session() as db:
        q = db.query(EventRoom)
        room = (q.filter(EventRoom.room_code == room_code.upper()).first() if room_code
                else q.filter(EventRoom.room_id == room_id).first())
        if not room:  # старый вызов передавал room_code в параметре room_id
            room = q.filter(EventRoom.room_code == room_id.upper()).first()
        if not room or room.status != "WAITING":
            return {"status": "not_found"}
        t = now()
        slots = ev_slots(db, room.room_id)
        free = next((sl for sl in HUMAN_FILL_ORDER if sl not in {s.slot for s in slots}), None)
        if free is None:
            return {"status": "room_full"}
        humans = [s for s in slots if not s.is_bot]
        name = make_bot_name({s.name for s in slots})
        db.add(EventSlot(room_id=room.room_id, slot=free, name=name,
                         rating=make_bot_rating(humans), is_bot=True, join_at=t, last_seen=t))
        return {"status": "ok", "name": name, "team": SLOT_TEAM[free]}


@api.post("/create_event_match")
def create_event_match(room_id: str):
    with room_lock(room_id), session() as db:
        if ev_match(db, room_id):
            return {"status": "already_exists"}
        db.add(EventMatch(room_id=room_id, status="NOT_READY", created_at=now(), last_seen=now()))
        return {"status": "created"}


@api.post("/update_event_match")
def update_event_match(room_id: str, state: dict = Body(...)):
    with room_lock(room_id), session() as db:
        em = ev_match(db, room_id)
        if not em:
            return {"status": "not_found"}
        set_event_state(db, em, state)
        em.last_seen = now()
        room = ev_room(db, room_id)
        if room:
            room.last_seen = now()
        return {"status": "updated"}


@api.get("/get_event_match")
def get_event_match(room_id: str, player_id: str = ""):
    with session() as db:
        em = ev_match(db, room_id)
        if not em:
            return {"status": "not_found"}
        t = now()
        touch(em, t, 5.0)
        room = ev_room(db, room_id)
        if room:
            touch(room, t, 5.0)
        if em.status == "NOT_READY":
            return {"status": "not_ready"}
        return event_state_dict(db, em)


def _ev_player(db, room_id, name):
    return db.query(EventStat).filter(EventStat.room_id == room_id, EventStat.name == name).first()


@api.get("/event_capture")
def event_capture(room_id: str, player_id: str, cell_index: int):
    with room_lock(room_id), session() as db:
        em = ev_match(db, room_id)
        if not em or em.status == "NOT_READY":
            return {"status": "no_match"}
        me = _ev_player(db, room_id, player_id)
        if not me:
            return {"status": "player_not_found"}
        if em.winner != "":
            return {"status": "finished"}
        if cell_index < 0 or cell_index >= EVENT_CELLS:
            return {"status": "bad_index"}
        team = me.team
        enemy = 2 if team == 1 else 1
        cells = load_cells(db, "e", room_id)
        cell = cells[cell_index]
        if cell.owner == team:
            return {"status": "already_owned"}
        if getattr(em, f"team{enemy}_protection_until") > int(now()) and cell.owner == enemy:
            return {"status": "protected"}
        if not any(cells[n].owner == team for n in event_neighbors(cell_index)):
            return {"status": "not_connected"}
        cost = 50 if cell.owner == 0 else 70
        if me.skulls < cost:
            return {"status": "no_skulls"}
        me.skulls -= cost
        me.rating += 1 if cell.owner == 0 else 2
        cell.owner = team
        cell.ability_spent = False
        if team == 1:
            cell.rev_a = True
        else:
            cell.rev_b = True
        starts = EVENT_STARTS[enemy]
        if cell_index in starts and all(cells[s].owner == team for s in starts):
            em.winner = f"team{team}"
            room = ev_room(db, room_id)
            if room:
                room.status, room.finished_at = "FINISHED", now()
            log.info("[WIN] %s захватила обе стартовые клетки соперника", em.winner)
        return {"status": "ok", "cost": cost}


def _apply_event_ability(db, em, cells, typ, team, enemy, t):
    if typ == "yellow_atomic":
        protected = EVENT_STARTS[enemy]
        targets = [c for c in cells if c.owner == enemy and c.idx not in protected]
        if targets:
            random.choice(targets).owner = 0
    elif typ == "red_exit":
        setattr(em, f"team{enemy}_hidden_until", t + 10)
    elif typ == "yellow_thri_line":
        setattr(em, f"team{enemy}_glitch_until", t + 10)
    elif typ == "red_flamew":
        setattr(em, f"team{team}_protection_until", t + 10)


@api.get("/event_use")
def event_use(room_id: str, player_id: str, cell_index: int):
    with room_lock(room_id), session() as db:
        em = ev_match(db, room_id)
        if not em or em.status == "NOT_READY":
            return {"status": "no_match"}
        me = _ev_player(db, room_id, player_id)
        if not me:
            return {"status": "player_not_found"}
        if em.winner != "":
            return {"status": "finished"}
        if cell_index < 0 or cell_index >= EVENT_CELLS:
            return {"status": "bad_index"}
        team = me.team
        enemy = 2 if team == 1 else 1
        cells = load_cells(db, "e", room_id)
        cell = cells[cell_index]
        if cell.owner != team:
            return {"status": "not_owner"}
        if cell.ability_spent:
            return {"status": "already_used"}
        t = int(now())
        if (cell.cooldown_end or 0) > t:
            return {"status": "on_cooldown"}
        energy_cost, cooldown = ABILITY_STATS_2V2.get(cell.type, [0, 0])
        if me.energy < energy_cost:
            return {"status": "no_energy"}
        me.energy -= energy_cost
        _apply_event_ability(db, em, cells, cell.type, team, enemy, t)
        if cooldown > 0:
            cell.cooldown_end = t + cooldown
        if cooldown == -1:
            cell.ability_spent = True
        return {"status": "ok"}


@api.get("/update_event_resources")
def update_event_resources(room_id: str, player_id: str, skulls: int, energy: int, rating: int):
    with room_lock(room_id), session() as db:
        if not ev_match(db, room_id):
            return {"status": "no_match"}
        me = _ev_player(db, room_id, player_id)
        if not me:
            return {"status": "player_not_found"}
        me.skulls, me.energy, me.rating = skulls, energy, rating
        return {"status": "ok"}


# старые адреса (без /api) и новые (с /api) — оба работают
app.include_router(api, prefix="/api")
app.include_router(api)

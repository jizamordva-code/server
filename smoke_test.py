import os, time, json
os.environ["DATABASE_URL"] = "sqlite:////tmp/test_game.db"
os.environ["EVENT_LOBBY_SECONDS"] = "12"
if os.path.exists("/tmp/test_game.db"): os.remove("/tmp/test_game.db")
from fastapi.testclient import TestClient
import main
c = TestClient(main.app)
g = lambda p, **kw: c.get("/api" + p, params=kw).json()

# --- игроки / магазин
assert g("/get_player", player_id="SM-G991B_ab12c")["total_ex"] == 0
assert g("/update_score", player_id="SM-G991B_ab12c", add_score=500)["status"] == "updated"
r = g("/store_data", tag="ex_SM-G991B_ab12c-1111-2222-ab12c", value="400"); assert r["total_ex"] == 400, r
assert g("/get_rating")[0]["total_ex"] == 400

# --- 1x1
a = g("/find_or_create_room", player_id="A"); assert a["role"] == "host"
assert g("/find_or_create_room", player_id="A")["room_id"] == a["room_id"]   # не плодит комнаты
b = g("/find_or_create_room", player_id="B"); assert b["status"] == "joined"
rid = a["room_id"]
assert g("/check_room_status", room_id=rid)["room_status"] == "READY"
assert g("/pick_start", room_id=rid, player_id="A", cell_index=0)["status"] == "ok"
assert g("/pick_start", room_id=rid, player_id="B", cell_index=24)["status"] == "started"
m = g("/get_match", room_id=rid); assert m["status"] == "RUNNING" and len(m["cells"]) == 25
print("capture", g("/capture_cell", room_id=rid, player_id="A", cell_index=1))
for fn, kw in [("/apply_debuff", dict(debuff_type="hide", duration=10)), ("/atomic_strike", {})]:
    print(fn, g(fn, room_id=rid, player_id="A", **kw))
print(g("/get_state", room_id=rid, player_id="A"),
      c.post(f"/api/send_state?room_id={rid}&player_id=B", json={"x": 1}).json(), g("/get_state", room_id=rid, player_id="A"))
print("rooms", g("/get_rooms"))
print("leave", g("/leave_room", room_id=rid))

# --- сезон: 1 человек + боты
r = g("/find_active_room", player_id="H1"); assert r["room_found"] is False
cr = g("/create_event_room", player_id="H1", rating=800); assert cr["status"] == "created", cr
room = cr["room_id"]
assert g("/find_active_room", player_id="H2")["room_found"] is True
j = g("/join_event_room", player_id="H2", room_code=cr["room_code"], rating=900); assert j["status"] == "joined" and j["team"] == 2, j
timeline = []
for i in range(16):
    l1 = g("/event_lobby", room_id=room, player_id="H1"); g("/event_lobby", room_id=room, player_id="H2")
    timeline.append((i, len(l1["players"]), l1["seconds_left"]))
    if l1["game_started"]: break
    time.sleep(1)
print("таймлайн (сек, игроков видно, осталось):", timeline)
print(json.dumps(l1["players"], ensure_ascii=False))
assert l1["game_started"] and len(l1["players"]) == 4 and len(l1["team1_players"]) == 2 and len(l1["team2_players"]) == 2
bots = [p for p in l1["players"] if p["is_bot"]]; assert len(bots) == 2

# матч сезона
assert g("/get_event_match", room_id=room)["status"] == "not_found"
assert c.post(f"/api/create_event_match?room_id={room}").json()["status"] == "created"
assert g("/get_event_match", room_id=room)["status"] == "not_ready"
cells = [{"type": "yellow_two_line", "owner": 0, "ability_spent": False, "cooldown_end": 0,
          "revealed_team1": False, "revealed_team2": False, "blocked_directions": []} for _ in range(49)]
for p in (0, 6): cells[p]["owner"] = 1
for p in (42, 48): cells[p]["owner"] = 2
st = {"status": "RUNNING", "team1_players": l1["team1_players"], "team2_players": l1["team2_players"],
      "team1_skulls": [50, 50], "team1_energy": [0, 0], "team1_rating": [0, 0],
      "team2_skulls": [50, 50], "team2_energy": [0, 0], "team2_rating": [0, 0], "cells": cells, "winner": "", "turn": 0, "turn_end_time": 0}
assert c.post(f"/api/update_event_match?room_id={room}", json=st).json()["status"] == "updated"
ev = g("/get_event_match", room_id=room); assert len(ev["cells"]) == 49 and ev["team1_players"] == l1["team1_players"]
t1 = l1["team1_players"][0]
print("capture", g("/event_capture", room_id=room, player_id=t1, cell_index=1), g("/event_use", room_id=room, player_id=t1, cell_index=0))
print(g("/update_event_resources", room_id=room, player_id=t1, skulls=99, energy=5, rating=3))
print("cancel без player_id во время матча:", g("/cancel_event_room", room_id=room))

# --- уборщик
time.sleep(0.1)
print("cleanup", g("/cleanup_old_rooms"))
assert c.get("/get_player", params={"player_id": "A"}).status_code == 200   # путь без /api
print("ВСЁ ОК")

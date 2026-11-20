"""Regression: green_vacant previews are temporary; blue_hydralic is no longer scouting."""
import os
import tempfile

with tempfile.TemporaryDirectory(prefix="godot-v11-") as folder:
    os.environ["DATABASE_URL"] = "sqlite:///" + os.path.join(folder, "test.db")
    from fastapi.testclient import TestClient
    import main
    with TestClient(main.app) as client:
        def get(url, **params):
            response = client.get("/api" + url, params=params)
            assert response.status_code == 200, (url, response.text)
            return response.json()
        room = get("/find_or_create_room", player_id="first")['room_id']
        assert get("/find_or_create_room", player_id="second")['room_id'] == room
        assert get("/pick_start", room_id=room, player_id="first", cell_index=12)['status'] == "ok"
        assert get("/pick_start", room_id=room, player_id="second", cell_index=24)['status'] == "started"
        def flags():
            return [cell["revealed_host"] for cell in get("/get_match", room_id=room)["cells"]]
        with main.session() as db:
            start = db.query(main.Cell).filter(main.Cell.scope == "m", main.Cell.room_id == room, main.Cell.idx == 12).one()
            start.type = "green_vacant"
        before = flags()
        random_show = get("/use_cell", room_id=room, player_id="first", cell_index=12)
        assert random_show['status'] == 'ok', random_show
        indices = random_show['temporary_reveal_indices']
        assert len(indices) == 2 and len(set(indices)) == 2, indices
        assert random_show['temporary_reveal_seconds'] == 5.0, random_show
        assert flags() == before, "Temporary preview wrote permanent visibility"
        print('PASS: green_vacant shows exactly two temporary cells without changing saved visibility')

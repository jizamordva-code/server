"""Regression smoke test for online 1x1 skull theft and capture adjacency.

Run from server/ with: python test_v9_capture_steal.py
Requires packages from requirements.txt, does not touch production DB.
"""
import os
import tempfile

with tempfile.TemporaryDirectory(prefix='godot-v9-test-') as temporary:
    os.environ['DATABASE_URL'] = 'sqlite:///' + os.path.join(temporary, 'game.sqlite')
    from fastapi.testclient import TestClient
    import main

    with TestClient(main.app) as client:
        def request(route, **params):
            r = client.get('/api' + route, params=params)
            assert r.status_code == 200, (route, r.text)
            return r.json()

        room = request('/find_or_create_room', player_id='host')['room_id']
        assert request('/find_or_create_room', player_id='guest')['room_id'] == room
        assert request('/pick_start', room_id=room, player_id='host', cell_index=0)['status'] == 'ok'
        assert request('/pick_start', room_id=room, player_id='guest', cell_index=24)['status'] == 'started'

        def poll(who, skulls):
            return request('/get_match', room_id=room, player_id=who, skulls=skulls, energy=0)

        poll('host', 50)
        poll('guest', 50)
        assert request('/steal_skulls', room_id=room, player_id='host', amount=30)['stolen'] == 30
        victim = poll('guest', 50)  # old client balance must not undo theft
        assert victim['skulls_lost'] == 30
        assert victim['guest_data']['skulls'] == 20
        assert poll('guest', 20)['skulls_lost'] == 0  # no second deduction
        assert poll('guest', 20)['guest_data']['skulls'] == 20
        assert request('/steal_skulls', room_id=room, player_id='stranger', amount=30)['status'] == 'not_in_match'

        capture = request('/capture_cell', room_id=room, player_id='host', cell_index=1)
        assert capture['status'] == 'ok'
        assert capture['state']['cells'][1]['owner'] == 1
        next_capture = request('/capture_cell', room_id=room, player_id='host', cell_index=2)
        assert next_capture['status'] == 'ok'
        assert next_capture['state']['cells'][2]['owner'] == 1
        assert request('/capture_cell', room_id=room, player_id='guest', cell_index=12)['status'] == 'not_connected'

print('OK: theft synced once; capture ownership persists; adjacency valid')

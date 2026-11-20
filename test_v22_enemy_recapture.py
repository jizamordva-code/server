"""Regression: an enemy taking one of our cells must not change adjacent owners."""
import os
import tempfile
with tempfile.TemporaryDirectory(prefix='cvorcles-v22-') as temp:
    os.environ['DATABASE_URL'] = 'sqlite:///' + os.path.join(temp, 'test.sqlite')
    from fastapi.testclient import TestClient
    import main
    with TestClient(main.app) as client:
        def req(route, **params):
            r = client.get('/api' + route, params=params)
            assert r.status_code == 200, r.text
            return r.json()
        room = req('/find_or_create_room', player_id='host')['room_id']
        assert req('/find_or_create_room', player_id='guest')['room_id'] == room
        assert req('/pick_start', room_id=room, player_id='host', cell_index=0)['status'] == 'ok'
        assert req('/pick_start', room_id=room, player_id='guest', cell_index=24)['status'] == 'started'
        def capture(player, idx):
            result = req('/capture_cell',room_id=room,player_id=player,cell_index=idx)
            assert result['status'] == 'ok', result
            return [c['owner'] for c in result['state']['cells']]
        for idx in [1,2,7]: capture('host',idx)
        for idx in [19,14,9,8]: before = capture('guest',idx)
        after = capture('guest',7)
        assert before[7] == 1 and after[7] == 2
        assert [i for i in range(25) if before[i] != after[i]] == [7]
        assert after[6] == after[12] == 0 and after[2] == 1
        for player in ['host','guest']:
            snap = req('/get_match',room_id=room,player_id=player,skulls=50,energy=0)
            assert [c['owner'] for c in snap['cells']] == after
print('PASS: enemy recapture changes exactly one owner; both clients receive identical owners')

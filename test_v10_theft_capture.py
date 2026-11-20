"""Isolated 1x1 regression test; never uses production database.
Run: python test_v10_theft_capture.py  (fastapi, sqlalchemy, httpx required)
"""
import os
import tempfile

with tempfile.TemporaryDirectory(prefix='game-v10-') as path:
    os.environ['DATABASE_URL'] = 'sqlite:///' + os.path.join(path,'test.sqlite')
    from fastapi.testclient import TestClient
    import main
    with TestClient(main.app) as client:
        def fetch(path,**args):
            response=client.get('/api'+path,params=args)
            assert response.status_code==200,response.text
            return response.json()
        room=fetch('/find_or_create_room',player_id='alpha')['room_id']
        assert fetch('/find_or_create_room',player_id='beta')['room_id']==room
        assert fetch('/pick_start',room_id=room,player_id='alpha',cell_index=0)['status']=='ok'
        assert fetch('/pick_start',room_id=room,player_id='beta',cell_index=24)['status']=='started'
        def poll(player,balance,ack=0):
            return fetch('/get_match',room_id=room,player_id=player,skulls=balance,energy=0,theft_ack=ack)
        poll('alpha',50)
        poll('beta',50)
        assert fetch('/steal_skulls',room_id=room,player_id='alpha',amount=30)['stolen']==30
        initial=poll('beta',50,0)
        assert (initial['skulls_lost'],initial['theft_total'],initial['guest_data']['skulls'])==(30,30,20),initial
        # A response from a second request sent before the theft was seen
        # must NOT reset the loss to 0 or restore the victim's balance to 50.
        stale=poll('beta',50,0)
        assert (stale['skulls_lost'],stale['guest_data']['skulls'])==(30,20),stale
        acknowleged=poll('beta',20,30)
        assert (acknowleged['skulls_lost'],acknowleged['guest_data']['skulls'])==(0,20),acknowleged
        # A late stale poll after the acknowledgement also remains safe.
        stale_after_ack=poll('beta',50,0)
        assert (stale_after_ack['skulls_lost'],stale_after_ack['guest_data']['skulls'])==(30,20),stale_after_ack
        assert poll('beta',20,30)['guest_data']['skulls']==20
        # Second theft reports only the NEW incremental loss.
        assert fetch('/steal_skulls',room_id=room,player_id='alpha',amount=10)['stolen']==10
        next_loss=poll('beta',20,30)
        assert (next_loss['skulls_lost'],next_loss['theft_total'],next_loss['guest_data']['skulls'])==(10,40,10),next_loss
        assert poll('beta',10,40)['guest_data']['skulls']==10
        assert fetch('/steal_skulls',room_id=room,player_id='unknown',amount=5)['status']=='not_in_match'
        tile=fetch('/capture_cell',room_id=room,player_id='alpha',cell_index=1)
        assert tile['status']=='ok' and tile['state']['cells'][1]['owner']==1,tile
        tile2=fetch('/capture_cell',room_id=room,player_id='alpha',cell_index=2)
        assert tile2['status']=='ok' and tile2['state']['cells'][2]['owner']==1,tile2
        assert fetch('/capture_cell',room_id=room,player_id='beta',cell_index=12)['status']=='not_connected'
print('PASS: theft acknowledgment, delayed poll races, second theft, owner snapshot and adjacency')

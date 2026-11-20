"""blue_hydralic is a one-use passive: +1 energy per later capture until its owner loses it."""
import os, tempfile
with tempfile.TemporaryDirectory(prefix='blue-hydralic-') as folder:
    os.environ['DATABASE_URL'] = 'sqlite:///' + folder + '/test.db'
    from fastapi.testclient import TestClient
    import main
    with TestClient(main.app) as c:
        def get(path, **params):
            r=c.get('/api'+path,params=params)
            assert r.status_code==200,(path,r.status_code,r.text)
            return r.json()
        # 1x1: activation is one-time, grants bonus on later captures, then stops when the cell is recaptured.
        room=get('/find_or_create_room',player_id='p1')['room_id']
        assert get('/find_or_create_room',player_id='p2')['room_id']==room
        assert get('/pick_start',room_id=room,player_id='p1',cell_index=0)['status']=='ok'
        assert get('/pick_start',room_id=room,player_id='p2',cell_index=2)['status']=='started'
        get('/get_match',room_id=room,player_id='p1',skulls=50,energy=10)
        with main.session() as db:
            passive=db.query(main.Cell).filter_by(scope='m',room_id=room,idx=1).one()
            passive.type='blue_hydralic'
        assert get('/capture_cell',room_id=room,player_id='p1',cell_index=1)['energy_bonus']==0
        use=get('/use_cell',room_id=room,player_id='p1',cell_index=1)
        assert use['status']=='ok' and use['temporary_reveal_indices']==[],use
        assert get('/use_cell',room_id=room,player_id='p1',cell_index=1)['status']=='already_used'
        cap=get('/capture_cell',room_id=room,player_id='p1',cell_index=5)
        assert cap['status']=='ok' and cap['energy_bonus']==1,cap
        # Opponent can take the passive tile from their adjacent start; its marker resets.
        stolen=get('/capture_cell',room_id=room,player_id='p2',cell_index=1)
        assert stolen['status']=='ok' and stolen['energy_bonus']==0,stolen
        with main.session() as db:
            passive=db.query(main.Cell).filter_by(scope='m',room_id=room,idx=1).one()
            assert passive.owner==2 and passive.ability_spent is False
        cap_after=get('/capture_cell',room_id=room,player_id='p1',cell_index=10)
        assert cap_after['status']=='ok' and cap_after['energy_bonus']==0,cap_after

        # 2x2: same passive is team-owned but the energy reward is credited to the player making the capture.
        er='blue-hydralic-event'
        with main.session() as db:
            db.add(main.EventRoom(room_id=er,room_code='BH2X2',status='RUNNING',created_at=main.now(),start_at=main.now(),started_at=main.now(),last_seen=main.now()))
            db.add(main.EventMatch(room_id=er,status='RUNNING',created_at=main.now(),last_seen=main.now()))
            db.add(main.EventSlot(room_id=er,slot=0,name='e1',is_bot=False,join_at=main.now(),last_seen=main.now()))
            db.add(main.EventSlot(room_id=er,slot=2,name='e2',is_bot=False,join_at=main.now(),last_seen=main.now()))
            db.add(main.EventStat(room_id=er,team=1,idx=0,name='e1',skulls=150,energy=10,rating=0))
            db.add(main.EventStat(room_id=er,team=2,idx=0,name='e2',skulls=50,energy=0,rating=0))
            for i in range(49):
                typ='blue_hydralic' if i==0 else 'yellow_two_line'
                owner=1 if i==0 else 2 if i==48 else 0
                db.add(main.Cell(scope='e',room_id=er,idx=i,type=typ,owner=owner,rev_a=(i in [0,1,6,7]),rev_b=(i in [41,42,43,47,48])))
        act=get('/event_use',room_id=er,player_id='e1',cell_index=0)
        assert act['status']=='ok' and act['resources']['energy']==0,act
        again=get('/event_use',room_id=er,player_id='e1',cell_index=0)
        assert again['status']=='already_used',again
        cap=get('/event_capture',room_id=er,player_id='e1',cell_index=1)
        assert cap['status']=='ok' and cap['energy_bonus']==1 and cap['resources']['energy']==1,cap
        # Simulate the opponent recapturing the activated cell; the passive marker is reset.
        with main.session() as db:
            passive=db.query(main.Cell).filter_by(scope='e',room_id=er,idx=0).one()
            passive.owner=2
            passive.ability_spent=False
        cap_after=get('/event_capture',room_id=er,player_id='e1',cell_index=8)
        assert cap_after['status']=='ok' and cap_after['energy_bonus']==0,cap_after
        print('PASS: 1x1 and 2x2 blue_hydralic passive activation, +1 energy capture bonus, and loss on recapture')

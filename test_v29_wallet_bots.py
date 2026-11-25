"""Regression coverage for v1.0.29 wallet synchronization, exactly-once 1v1 reward and bot takeover."""
import os, tempfile, time
from fastapi.testclient import TestClient

with tempfile.TemporaryDirectory() as folder:
    os.environ['DATABASE_URL'] = 'sqlite:///' + folder + '/test.db'
    import main as m
    clock = [time.time()]
    m.now = lambda: clock[0]
    with TestClient(m.app) as c:
        def get(path, **args):
            r = c.get('/api/'+path, params=args)
            assert r.status_code == 200, r.text
            return r.json()
        def sync(player, rating, spend=0, earn=0, op=''):
            return get('sync_score',player_id=player,local_rating=rating,spend=spend,earn=earn,operation_id=op)['total_ex']
        def balance(player):
            return get('get_player',player_id=player)['total_ex']
        assert sync('wallet',100)==100
        assert sync('wallet',70,30,0,'purchase-1')==70
        assert sync('wallet',70,30,0,'purchase-1')==70  # lost response/restart
        assert sync('wallet',140,0,70,'event-win')==140
        assert sync('wallet',140,0,70,'event-win')==140
        assert sync('wallet',115,40,15,'offline-win-and-shop')==115
        assert sync('wallet',115,40,15,'offline-win-and-shop')==115
        get('update_score',player_id='wallet',add_score=10)
        assert sync('wallet',115)==125
        assert sync('wallet',130)==130
        assert sync('wallet',30,100,0,'loss')==30
        assert balance('wallet')==30
        print('PASS durable local rating merge, reward +70, purchases, retries, penalty')

        def new1(a,b):
            room=get('find_or_create_room',player_id=a)['room_id']
            assert get('find_or_create_room',player_id=b)['room_id']==room
            assert get('pick_start',room_id=room,player_id=a,cell_index=0)['status']=='ok'
            assert get('pick_start',room_id=room,player_id=b,cell_index=24)['status']=='started'
            return room
        room=new1('winner','loser')
        get('update_score',player_id='winner',add_score=100)
        with m.session() as db:
            match=m.get_match_row(db,room);match.host_rating=17
            db.query(m.Cell).filter_by(scope='m',room_id=room,idx=23).one().owner=1
        result=get('capture_cell',room_id=room,player_id='winner',cell_index=24)
        assert result['rating']==19 and result['winner']=='winner'
        assert balance('winner')==119
        assert get('finish_match',room_id=room)['status']=='finished'
        assert get('finish_match',room_id=room)['status']=='finished'
        assert sync('winner',100)==119 and balance('winner')==119
        print('PASS 1v1 winner paid 19 exactly once, before finish request')
        r=new1('online','departed')
        get('get_match',room_id=r,player_id='online',skulls=50,energy=0)
        get('get_match',room_id=r,player_id='departed',skulls=50,energy=0)
        assert get('leave_room',room_id=r,player_id='departed')['status']=='left'
        assert get('get_match',room_id=r,player_id='departed')['status']=='player_left'
        assert get('get_match',room_id=r,player_id='online')['winner']==''
        clock[0]+=8
        get('get_match',room_id=r,player_id='online',skulls=50,energy=0)
        with m.session() as db:
            mm=m.get_match_row(db,r)
            assert mm.guest_rating>0 or mm.guest_skulls!=50 or mm.guest_energy!=0
        assert get('find_or_create_room',player_id='departed')['room_id']!=r
        assert get('get_match',room_id=r,player_id='online')['status']=='RUNNING'
        print('PASS 1v1 bot takeover, old room preserved when player starts new match')

        er='ev29'
        with m.session() as db:
            db.add(m.EventRoom(room_id=er,room_code='EV29',status='RUNNING',host_name='e1',last_seen=clock[0],created_at=clock[0]))
            for i,n in enumerate(['e1','e2','e3','e4']):
                db.add(m.EventSlot(room_id=er,slot=i,name=n,is_bot=False,last_seen=clock[0],join_at=clock[0]))
        snap=get('get_event_match',room_id=er,player_id='e1')
        assert len(snap['cells'])==49
        get('leave_event_room',room_id=er,player_id='e2')
        assert get('get_event_match',room_id=er,player_id='e2')['status']=='player_left'
        assert get('event_capture',room_id=er,player_id='e2',cell_index=5)['status']=='player_left'
        assert get('get_event_match',room_id=er,player_id='e1')['team1_players']==['e1','e2']
        for _ in range(12):
            clock[0]+=1
            new=get('get_event_match',room_id=er,player_id='e1')
            get('get_event_match',room_id=er,player_id='e3')
            get('get_event_match',room_id=er,player_id='e4')
            if new['revision']>snap['revision']:break
        assert new['revision']>snap['revision']
        assert new['team1_skulls'][0]==50 and new['capture_counts'].get('e1',0)==0
        assert set(new['capture_counts']).issubset({'e2'})
        clock[0]+=m.MATCH_HUMAN_TTL+1
        get('get_event_match',room_id=er,player_id='e1')
        with m.session() as db:
            slots=m.ev_slots(db,er)
            assert not slots[0].is_bot and all(slot.is_bot for slot in slots[1:])
        assert get('get_event_match',room_id=er,player_id='e3')['status']=='player_left'
        print('PASS 2v2 explicit and inactivity takeover, original slots/resources retained')
        get('leave_event_room',room_id=er,player_id='e1')
        get('leave_room',room_id=r,player_id='online')
        with m.session() as db:
            assert m.ev_room(db,er) is not None and m.get_match_row(db,r) is not None
        clock[0]+=m.EVENT_RUNNING_IDLE+1
        get('cleanup_old_rooms')
        with m.session() as db:
            assert m.ev_room(db,er) is None and m.get_match_row(db,r) is None
        print('PASS empty running rooms retained until janitor')

"""New regression tests: atomic theft, resource writes during seasonal theft, abilities."""
import os,tempfile
from fastapi.testclient import TestClient
with tempfile.TemporaryDirectory() as folder:
    os.environ['DATABASE_URL']='sqlite:///'+folder+'/test.db'
    import main
    with TestClient(main.app) as c:
        get=lambda url,**args:c.get('/api'+url,params=args).json()
        room=get('/find_or_create_room',player_id='h')['room_id']
        assert get('/find_or_create_room',player_id='g')['room_id']==room
        assert get('/pick_start',room_id=room,player_id='h',cell_index=0)['status']=='ok'
        assert get('/pick_start',room_id=room,player_id='g',cell_index=24)['status']=='started'
        get('/get_match',room_id=room,player_id='h',skulls=50,energy=20,theft_ack=0)
        get('/get_match',room_id=room,player_id='g',skulls=50,energy=20,theft_ack=0)
        with main.session() as db:
            st=db.query(main.Cell).filter_by(scope='m',room_id=room,idx=0).one()
            st.type='blue_gravity'
        act=get('/use_cell',room_id=room,player_id='h',cell_index=0)
        assert (act['status'],act['stolen'])==('ok',30),act
        assert get('/use_cell',room_id=room,player_id='h',cell_index=0)['status']=='on_cooldown'
        victim=get('/get_match',room_id=room,player_id='g',skulls=50,energy=20,theft_ack=0)
        assert (victim['guest_data']['skulls'],victim['theft_total'])==(20,30),victim
        get('/get_match',room_id=room,player_id='g',skulls=20,energy=20,theft_ack=30)
        # Construct a deterministic seasonal match without slow lobby/timer.
        er='ev-p12'
        with main.session() as db:
            db.add(main.EventRoom(room_id=er,room_code='PAT12',status='RUNNING'))
            db.add(main.EventMatch(room_id=er,status='RUNNING',created_at=main.now(),last_seen=main.now()))
            db.add(main.EventStat(room_id=er,team=1,idx=0,name='h2',skulls=50,energy=20))
            db.add(main.EventStat(room_id=er,team=2,idx=0,name='g2',skulls=50,energy=20))
            for i in range(49):
                db.add(main.Cell(scope='e',room_id=er,idx=i,type='blue_gravity' if i==0 else 'yellow_two_line',owner=1 if i==0 else 2 if i==48 else 0))
        act=get('/event_use',room_id=er,player_id='h2',cell_index=0)
        assert act['status']=='ok' and act['stolen']==30,act
        assert (act['resources']['skulls'],act['resources']['energy'])==(80,0),act
        state=get('/get_event_match',room_id=er,player_id='g2')
        assert state['team2_skulls']==[20] and state['theft_total']==30,state
        # A client request sent before the theft must not overwrite it.
        stale=get('/update_event_resources',room_id=er,player_id='g2',skulls=50,energy=20,rating=0,theft_ack=0)
        assert stale['status']=='ok'
        assert get('/get_event_match',room_id=er,player_id='g2')['team2_skulls']==[20]
        get('/update_event_resources',room_id=er,player_id='g2',skulls=20,energy=20,rating=0,theft_ack=30)
        assert get('/get_event_match',room_id=er,player_id='g2')['team2_skulls']==[20]
        assert get('/event_use',room_id=er,player_id='h2',cell_index=0)['status']=='on_cooldown'
        # A host/client retry must never wipe acquired territory or stolen skulls.
        replay=c.post('/api/update_event_match',params={'room_id':er},json={
            'status':'RUNNING','cells':[{'type':'yellow_two_line','owner':0} for _ in range(49)]}).json()
        assert replay['status']=='already_initialized',replay
        assert get('/get_event_match',room_id=er,player_id='g2')['team2_skulls']==[20]
        # Fresh matches still allow the one-time initial map submission.
        initial_room='init-event-p12'
        with main.session() as db:
            db.add(main.EventRoom(room_id=initial_room,room_code='INIT12',status='RUNNING'))
        assert c.post('/api/create_event_match',params={'room_id':initial_room}).json()['status']=='created'
        initial_state={'status':'RUNNING','team1_players':['a'],'team2_players':['b'],
                       'cells':[{'type':'yellow_two_line','owner':1 if i == 0 else 2 if i == 48 else 0}
                                for i in range(49)]}
        assert c.post('/api/update_event_match',params={'room_id':initial_room},json=initial_state).json()['status']=='updated'
        assert c.post('/api/update_event_match',params={'room_id':initial_room},json=initial_state).json()['status']=='already_initialized'
        assert len(get('/get_event_match',room_id=initial_room)['cells']) == 49
print('PASS patch12: atomic 1x1 theft; seasonal ack; cooldown; initial-map replay guarded')

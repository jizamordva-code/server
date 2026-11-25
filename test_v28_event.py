"""Regression tests for the authoritative 2v2 game, run with python test_v28_event.py."""
import os, tempfile, time
from concurrent.futures import ThreadPoolExecutor
from fastapi.testclient import TestClient

with tempfile.TemporaryDirectory() as tmp:
    os.environ['DATABASE_URL'] = 'sqlite:///' + tmp + '/event.db'
    import main as m
    clock = [time.time()]
    m.now = lambda: clock[0]
    with TestClient(m.app) as client:
        def get(endpoint, **params):
            response = client.get('/api/' + endpoint, params=params)
            assert response.status_code == 200, response.text
            return response.json()
        def state(room, player):
            return get('get_event_match', room_id=room, player_id=player)
        def act(room, player, index, mode='use'):
            return get('event_' + mode, room_id=room, player_id=player, cell_index=index)
        def room(room_id, bots=()):
            with m.session() as db:
                db.add(m.EventRoom(room_id=room_id,room_code=room_id.upper(),status='RUNNING',host_name='a',created_at=clock[0],last_seen=clock[0]))
                for i,name in enumerate('abcd'):
                    db.add(m.EventSlot(room_id=room_id,slot=i,name=name,is_bot=name in bots,join_at=clock[0],last_seen=clock[0]))
            result=state(room_id,'a')
            assert result['status']=='RUNNING' and len(result['cells'])==49
            assert result['team1_players']==['a','b'] and result['team2_players']==['c','d']
            assert [result['cells'][i]['captured_by'] for i in (0,6,42,48)]==list('abcd')
            return result
        def edit(room_id, cell, **changes):
            with m.session() as db:
                c=db.query(m.Cell).filter_by(scope='e',room_id=room_id,idx=cell).one()
                for key,val in changes.items(): setattr(c,key,val)
        def resources(room_id, player, **changes):
            with m.session() as db:
                stat=db.query(m.EventStat).filter_by(room_id=room_id,name=player).one()
                for key,val in changes.items(): setattr(stat,key,val)
        r='event28'
        room(r)
        edit(r,0,type='green_vacant')
        resources(r,'a',energy=30,skulls=200)
        resources(r,'b',energy=20,skulls=200)
        result=act(r,'a',0)
        assert result['status']=='ok' and result['resources']['energy']==20
        indices=result['temporary_reveal_indices']; assert len(indices)==2 and len(set(indices))==2
        a,b,c=state(r,'a'),state(r,'b'),state(r,'c')
        for i in indices:
            assert a['cells'][i]['reveal_until_team1']==b['cells'][i]['reveal_until_team1']==clock[0]+5
            assert not a['cells'][i]['revealed_team1'] and c['cells'][i]['reveal_until_team2']==0
        assert act(r,'b',0)['status']=='on_cooldown'
        assert state(r,'b')['team1_energy'][1]==20
        clock[0]+=5.01
        after=state(r,'b')
        assert all(after['cells'][i]['reveal_until_team1']<after['server_time'] for i in indices)
        assert act(r,'b',0)['status']=='ok'
        print('PASS shared green_vacant, exact cooldown, no permanent reveal')
        # Concurrent teammates try the same tile; only one debit and one capture.
        with ThreadPoolExecutor(max_workers=2) as pool:
            both=list(pool.map(lambda name:act(r,name,1,'capture'), ['a','b']))
        assert sorted(x['status'] for x in both)==['already_owned','ok']
        snap=state(r,'a'); actor=snap['cells'][1]['captured_by']
        assert actor in ['a','b'] and snap['capture_counts']=={actor:1}
        assert sum(snap['team1_skulls'])==350
        # Enemy recapture counts once in the streak, twice only in rating.
        edit(r,2,owner=2)
        resources(r,'c',skulls=200)
        assert act(r,'c',1,'capture')['status']=='ok'
        snap=state(r,'a'); assert snap['cells'][1]['captured_by']=='c' and snap['cells'][1]['cooldown_end']==0
        assert snap['capture_counts']['c']==1 and snap['team2_rating'][0]==2
        assert snap['revision']>result['state']['revision']
        print('PASS concurrent capture, ownership, recapture counts, revision')
        # No accidental use of decoys or failed actions.
        edit(r,6,type='blue_block')
        assert act(r,'b',6)['status']=='no_ability'
        before=state(r,'a')['team1_skulls'][0]
        assert act(r,'a',40,'capture')['status']=='not_connected'
        assert state(r,'a')['team1_skulls'][0]==before
        edit(r,0,type='blue_light',ability_spent=True,cooldown_end=0)
        edit(r,7,owner=1)
        edit(r,8,owner=2)
        assert act(r,'c',7,'capture')['cost']==100
        print('PASS no-op decoys, invalid capture, blue_light pricing')
        # Permanent passive and resource authority.
        edit(r,6,type='yellow_energy',ability_spent=False,cooldown_end=0)
        resources(r,'b',energy=25)
        assert act(r,'b',6)['status']=='ok'
        bal=state(r,'a')['team1_skulls']
        # Advance time while all humans stay connected.
        for _ in range(4):
            clock[0]+=10.025
            for name in 'abcd': state(r,name)
        newer=state(r,'a')['team1_skulls'];assert newer==[bal[0],bal[1]+20]
        assert state(r,'b')['team1_skulls']==newer
        get('update_event_resources',room_id=r,player_id='a',skulls=999999,energy=999999,rating=999999)
        assert state(r,'a')['team1_skulls']==newer
        print('PASS personal passive income, old resource writes ignored')
        # Bot-only moves are batched once per interval; human stats remain unchanged.
        rb='bot28';initial=room(rb, bots=('b','c','d'))
        clock[0]+=1.51
        assert state(rb,'a')['revision']==initial['revision']
        for _ in range(12):
            clock[0]+=1
            tick=state(rb,'a')
            if tick['revision']>initial['revision']: break
        assert tick['revision']>initial['revision']
        assert tick['team1_skulls'][0]==50 and tick['capture_counts'].get('a',0)==0
        again=state(rb,'a');assert again['revision']==tick['revision']
        assert set(tick['capture_counts']).issubset({'b','c','d'})
        print('PASS server bot batch, no human moves, throttled polling')
        # Ending a match does not disappear from the other player's polling.
        edit(r,42,owner=1);edit(r,47,owner=1)
        resources(r,'a',skulls=200)
        end=act(r,'a',48,'capture');assert end['state']['winner']=='team1' and end['state']['status']=='FINISHED'
        assert state(r,'d')['winner']=='team1'
        assert act(r,'d',48)['status']=='finished'
        print('PASS finish delivered to winning and losing teams')
print('PASS ALL v28 event scenarios')

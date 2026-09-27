"""Run only against the isolated demo Compose project; creates labelled QA orders."""
import asyncio
import hashlib
import hmac
import json
import os
import time
import uuid
import httpx
import websockets

BASE=os.getenv('TEST_API_URL','http://localhost:8000')
SECRET='demo_webhook_secret'

async def run():
    async with httpx.AsyncClient(base_url=BASE,timeout=20) as c:
        assert (await c.get('/health')).json()['payment_mode']=='demo'
        token=(await c.post('/api/staff/session',json={'pin':'2468'})).json()['token']
        staff={'Authorization':'Bearer '+token}
        assert (await c.get('/api/business/dashboard')).status_code==401
        counter_body={'request_key':str(uuid.uuid4()),'customer_name':'QA walk-in','items':[{'menu_item_id':'espresso','quantity':1,'modifiers':[]}]}
        counter=await c.post('/api/business/orders',headers=staff,json=counter_body)
        assert counter.status_code==201,counter.text
        counter_order=counter.json()
        assert counter_order['status']=='paid' and counter_order['payment_status']=='captured'
        assert counter_order['payment_method']=='counter' and counter_order['total_paise']==16000
        counter_retry=await c.post('/api/business/orders',headers=staff,json=counter_body)
        assert counter_retry.status_code==201 and counter_retry.json()['id']==counter_order['id']
        dashboard=(await c.get('/api/business/dashboard',headers=staff)).json()
        assert dashboard['metrics']['orders_today']>=1 and dashboard['metrics']['active_orders']>=1
        assert any(o['id']==counter_order['id'] for o in dashboard['orders'])
        assert (await c.patch('/api/barista/orders/'+counter_order['id'],headers=staff,json={'status':'cancelled','reason':'QA cleanup'})).status_code==200
        async def make():
            body={'checkout_key':str(uuid.uuid4()),'customer_name':'QA test','items':[{'menu_item_id':'espresso','quantity':1,'modifiers':[]}]}
            r=await c.post('/api/orders',json=body)
            assert r.status_code==201,r.text
            d=r.json();return d,body,{'Authorization':'Bearer '+d['token']}
        async def capture(d,headers,event=None,payment=None,amount=16000):
            raw=json.dumps({'event':'payment.captured','payload':{'payment':{'entity':{'id':payment or 'pay_qa_'+uuid.uuid4().hex,'order_id':d['payment']['provider_order_id'],'amount':amount,'currency':'INR','status':'captured'}}}}).encode()
            sig=hmac.new(SECRET.encode(),raw,hashlib.sha256).hexdigest()
            return await c.post('/api/webhooks/razorpay',content=raw,headers={'x-razorpay-signature':sig,'x-razorpay-event-id':event or str(uuid.uuid4())})
        d,body,auth=await make();oid=d['order']['id']
        # Concurrent checkout delivery must return the same order.
        duplicates=await asyncio.gather(*[c.post('/api/orders',json=body) for _ in range(6)])
        assert all(r.status_code==201 and r.json()['order']['id']==oid for r in duplicates)
        assert (await c.get('/api/barista/orders')).status_code==401
        assert (await c.get('/api/orders/'+oid)).status_code==403
        assert oid not in [o['id'] for o in (await c.get('/api/barista/orders',headers=staff)).json()]
        failed=await c.post('/api/orders/'+oid+'/demo-payment',json={'outcome':'failed'},headers=auth)
        assert failed.status_code==200,failed.text
        assert (await c.get('/api/orders/'+oid,headers=auth)).json()['payment_status']=='failed'
        retry=(await c.post('/api/orders/'+oid+'/retry-payment',headers=auth)).json()
        assert retry['provider_order_id']==d['payment']['provider_order_id']
        bad=await capture(d,auth,amount=1)
        assert bad.status_code==400,bad.text
        raw=json.dumps({'event':'payment.captured','payload':{'payment':{'entity':{'id':'pay_qa_'+uuid.uuid4().hex,'order_id':d['payment']['provider_order_id'],'amount':16000,'currency':'INR','status':'captured'}}}}).encode()
        signature=hmac.new(SECRET.encode(),raw,hashlib.sha256).hexdigest()
        event=str(uuid.uuid4());headers={'x-razorpay-signature':signature,'x-razorpay-event-id':event}
        assert (await c.post('/api/webhooks/razorpay',content=raw,headers={'x-razorpay-signature':'wrong'})).status_code==400
        results=await asyncio.gather(*[c.post('/api/webhooks/razorpay',content=raw,headers=headers) for _ in range(8)])
        assert all(r.status_code==200 for r in results),[r.text for r in results]
        assert sum(not r.json().get('duplicate',False) for r in results)==1
        # A different event ID for the same payment cannot regress preparation.
        assert (await c.patch('/api/barista/orders/'+oid,headers=staff,json={'status':'preparing'})).status_code==200
        headers['x-razorpay-event-id']=str(uuid.uuid4())
        assert (await c.post('/api/webhooks/razorpay',content=raw,headers=headers)).status_code==200
        assert (await c.get('/api/orders/'+oid,headers=auth)).json()['status']=='preparing'
        assert (await c.post('/api/orders/'+oid+'/cancel',headers=auth)).status_code==409
        assert (await c.patch('/api/barista/orders/'+oid,headers=staff,json={'status':'cancelled','reason':' '})).status_code==400
        assert (await c.patch('/api/barista/orders/'+oid,headers=staff,json={'status':'ready'})).status_code==200
        serving=(await c.get('/api/serving')).json()
        assert all(set(o)=={'number','status'} for o in serving)
        assert (await c.patch('/api/barista/orders/'+oid,headers=staff,json={'status':'picked_up'})).status_code==200
        assert (await c.post('/api/orders/'+oid+'/cancel',headers=auth)).status_code==409
        # Late capture on cancelled unpaid order -> refund, never a ticket.
        late,_,la=await make();lid=late['order']['id']
        assert (await c.post('/api/orders/'+lid+'/cancel',headers=la)).status_code==200
        assert (await capture(late,la)).status_code==200
        for _ in range(30):
            state=(await c.get('/api/orders/'+lid,headers=la)).json()
            if state['refunds'] and state['refunds'][0]['status']=='processed':break
            await asyncio.sleep(.2)
        assert state['status']=='cancelled' and state['refunds'][0]['status']=='processed',state
        paid,_,pa=await make();pid=paid['order']['id'];assert (await capture(paid,pa)).status_code==200
        assert (await c.post('/api/orders/'+pid+'/cancel',headers=pa)).status_code==200
        # Connect multiple sockets and receive the same event from all workers.
        sockets=[await websockets.connect(BASE.replace('http','ws')+'/ws?audience=public') for _ in range(8)]
        for ws in sockets:await ws.recv()
        event_order,_,ea=await make();t=time.monotonic();assert (await capture(event_order,ea)).status_code==200
        events=await asyncio.gather(*[asyncio.wait_for(ws.recv(),2) for ws in sockets])
        assert all(json.loads(e)['type']=='refresh' for e in events)
        latency=time.monotonic()-t
        for ws in sockets:await ws.close()
        await c.post('/api/orders/'+event_order['order']['id']+'/cancel',headers=ea)
        # Malformed JSON signed correctly still returns a clear 400.
        malformed=b'not json';sig=hmac.new(SECRET.encode(),malformed,hashlib.sha256).hexdigest()
        assert (await c.post('/api/webhooks/razorpay',content=malformed,headers={'x-razorpay-signature':sig})).status_code==400
        print(json.dumps({'passed':True,'checks':'pricing/auth/business dashboard/idempotent counter order/idempotent checkout/failed retry/concurrent webhook duplicates/amount validation/states/customer cancellation/late capture/refunds/public privacy/Redis fanout/malformed input','sockets':8,'live_update_seconds':round(latency,3)}))

if __name__=='__main__':asyncio.run(run())

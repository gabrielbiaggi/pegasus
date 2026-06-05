import asyncio
import json
import websockets
import time

async def test_download():
    # New public WebSocket endpoint
    url = "wss://api.derivws.com/trading/v1/options/ws/public"
    print(f"Connecting to public endpoint: {url}")
    
    try:
        async with websockets.connect(url, ping_interval=30, open_timeout=15) as ws:
            payload = {
                "ticks_history": "1HZ100V",
                "start": int(time.time()) - 3600,
                "end": int(time.time()),
                "style": "ticks",
                "count": 100,
            }
            print(f"Sending request: {json.dumps(payload)}")
            await ws.send(json.dumps(payload))
            
            resp = json.loads(await ws.recv())
            if "error" in resp:
                print(f"❌ Error: {resp['error']}")
            else:
                hist = resp.get("history", {})
                prices = hist.get("prices", [])
                times = hist.get("times", [])
                print(f"✅ Success! Received {len(prices)} ticks.")
                if prices:
                    print(f"First price: {prices[0]} at epoch {times[0]}")
    except Exception as e:
        print(f"❌ Connection error: {e}")

asyncio.run(test_download())

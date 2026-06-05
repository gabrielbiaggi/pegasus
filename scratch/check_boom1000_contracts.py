import asyncio
import json
import os
import sys
from pathlib import Path
import websockets
from dotenv import load_dotenv

sys.path.append(str(Path(__file__).parent.parent))
import deriv_auth

load_dotenv(Path(__file__).parent.parent / ".env")

DERIV_APP_ID = os.getenv("DERIV_APP_ID", "1089")

async def main():
    print("Connecting using deriv_auth...")
    auth = deriv_auth.get_auth(DERIV_APP_ID, "demo")
    
    async with websockets.connect(auth.ws_url) as ws:
        if not auth.is_new_api:
            await ws.send(json.dumps({"authorize": auth.legacy_token}))
            await ws.recv()

        print("Querying contracts for 1HZ100V...")
        await ws.send(json.dumps({"contracts_for": "1HZ100V"}))
        resp = json.loads(await ws.recv())
        
        contracts = resp.get("contracts_for", {}).get("available", [])
        
        # Filter for CALL/PUT
        rf_contracts = [c for c in contracts if c.get("contract_type") in ["CALL", "PUT"]]
        
        print(f"\n--- Rise/Fall (CALL/PUT) for 1HZ100V ({len(rf_contracts)} found) ---")
        for c in rf_contracts:
            print(f"Type: {c.get('contract_type')} | Category: {c.get('contract_category')} | Name: {c.get('contract_display')}")
            print(f"  Min Duration: {c.get('min_contract_duration')} | Max Duration: {c.get('max_contract_duration')}")
            print(f"  Supported duration units: {c.get('duration_units')}")
            print("-" * 50)

if __name__ == "__main__":
    asyncio.run(main())

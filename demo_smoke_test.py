from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any

import websockets

from config import load_config


async def recv_until(
    ws: websockets.WebSocketClientProtocol,
    msg_type: str,
    timeout: float = 15,
) -> dict[str, Any]:
    end = asyncio.get_running_loop().time() + timeout
    while True:
        remaining = end - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise TimeoutError(f"Timeout aguardando {msg_type}.")

        data = json.loads(await asyncio.wait_for(ws.recv(), timeout=remaining))
        if "error" in data:
            error = data["error"]
            raise RuntimeError(f"Deriv {data.get('msg_type')} {error.get('code')}: {error.get('message')}")
        if data.get("msg_type") == msg_type:
            return data


async def run_smoke_test(stake: float, close_after_updates: int, max_updates: int) -> dict[str, Any]:
    config = load_config()
    async with websockets.connect(config.ws_url, ping_interval=None, open_timeout=10) as ws:
        await ws.send(json.dumps({"authorize": config.token}))
        auth_data = await recv_until(ws, "authorize")
        auth = auth_data["authorize"]
        loginid = str(auth.get("loginid", ""))
        if not loginid.upper().startswith("VRTC"):
            raise RuntimeError(f"Smoke test bloqueado: token autorizou conta nao-demo {loginid}.")

        await ws.send(
            json.dumps(
                {
                    "proposal": 1,
                    "amount": stake,
                    "basis": "stake",
                    "contract_type": "ACCU",
                    "currency": config.currency,
                    "symbol": config.symbol,
                    "growth_rate": config.accumulator_growth_rate,
                }
            )
        )
        proposal = (await recv_until(ws, "proposal"))["proposal"]
        ask_price = float(proposal.get("ask_price") or stake)

        await ws.send(json.dumps({"buy": proposal["id"], "price": ask_price}))
        buy = (await recv_until(ws, "buy"))["buy"]
        contract_id = int(buy["contract_id"])

        await ws.send(json.dumps({"proposal_open_contract": 1, "contract_id": contract_id, "subscribe": 1}))
        last_contract: dict[str, Any] = {}
        sell_result: dict[str, Any] | None = None

        for update_count in range(1, max_updates + 1):
            contract = (await recv_until(ws, "proposal_open_contract", timeout=20))["proposal_open_contract"]
            last_contract = contract
            if contract.get("is_sold") or contract.get("status") == "sold":
                break

            bid_price = float(contract.get("bid_price") or 0.0)
            if update_count < close_after_updates or bid_price <= 0:
                continue

            try:
                await ws.send(json.dumps({"sell": contract_id, "price": round(bid_price, 2)}))
                sell_result = (await recv_until(ws, "sell", timeout=10))["sell"]
                break
            except RuntimeError as exc:
                if "InvalidtoSell" not in str(exc):
                    raise
                await asyncio.sleep(1.2)

        await ws.send(json.dumps({"balance": 1}))
        balance = (await recv_until(ws, "balance"))["balance"]

        return {
            "loginid": loginid,
            "contract_id": contract_id,
            "buy_price": buy.get("buy_price"),
            "sold": bool(sell_result) or bool(last_contract.get("is_sold")) or last_contract.get("status") == "sold",
            "sold_for": sell_result.get("sold_for") if sell_result else last_contract.get("sell_price"),
            "last_profit": last_contract.get("profit"),
            "balance": balance.get("balance"),
            "currency": balance.get("currency"),
        }


def main() -> None:
    parser = argparse.ArgumentParser(description="Compra e fecha um ACCU minimo na conta demo para validar a API.")
    parser.add_argument("--stake", type=float, default=1.0)
    parser.add_argument("--close-after-updates", type=int, default=2)
    parser.add_argument("--max-updates", type=int, default=12)
    args = parser.parse_args()

    result = asyncio.run(
        run_smoke_test(
            stake=args.stake,
            close_after_updates=args.close_after_updates,
            max_updates=args.max_updates,
        )
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

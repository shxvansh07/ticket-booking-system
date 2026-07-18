"""
Concurrency stress test: fires N simulated users at the *same* seat at the
same instant and verifies that exactly one of them ends up with a confirmed
booking -- the empirical proof that the locking strategy in backend/app.py
actually holds under real concurrent load, not just in theory.

Usage:
    python concurrency_test.py --seat-id 594 --n 50 --base-url http://127.0.0.1:8100

Each simulated user: joins (if the event requires it), holds the seat, and
immediately tries to confirm -- all N users start their requests at the same
moment via asyncio.gather, so the race is real, not simulated in sequence.
"""
import argparse
import asyncio
import time
import uuid

import httpx


async def simulate_user(client: httpx.AsyncClient, base_url: str, seat_id: int, user_id: int, event_id: int, high_demand: bool):
    token = f"stress-user-{user_id}-{uuid.uuid4().hex[:6]}"
    result = {"user": token, "held": False, "confirmed": False, "hold_status": None, "confirm_status": None}

    if high_demand:
        await client.post(f"{base_url}/events/{event_id}/queue/join", json={"token": token})
        # poll until admitted (bounded, so a broken queue doesn't hang the test forever)
        for _ in range(50):
            r = await client.get(f"{base_url}/events/{event_id}/queue/status", params={"token": token})
            if r.json().get("admitted"):
                break
            await asyncio.sleep(0.1)

    hold_resp = await client.post(f"{base_url}/seats/{seat_id}/hold", json={"user_token": token})
    result["hold_status"] = hold_resp.status_code
    if hold_resp.status_code != 200:
        return result
    result["held"] = True

    confirm_resp = await client.post(
        f"{base_url}/bookings/confirm",
        json={"seat_id": seat_id, "user_token": token, "idempotency_key": f"{token}-confirm"},
    )
    result["confirm_status"] = confirm_resp.status_code
    result["confirmed"] = confirm_resp.status_code == 200
    return result


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seat-id", type=int, required=True)
    parser.add_argument("--event-id", type=int, required=True)
    parser.add_argument("--n", type=int, default=50)
    parser.add_argument("--base-url", default="http://127.0.0.1:8100")
    parser.add_argument("--high-demand", action="store_true")
    args = parser.parse_args()

    async with httpx.AsyncClient(timeout=30.0) as client:
        start = time.time()
        tasks = [
            simulate_user(client, args.base_url, args.seat_id, i, args.event_id, args.high_demand)
            for i in range(args.n)
        ]
        results = await asyncio.gather(*tasks)
        elapsed = time.time() - start

    held_count = sum(1 for r in results if r["held"])
    confirmed_count = sum(1 for r in results if r["confirmed"])

    print(f"\nFired {args.n} concurrent users at seat {args.seat_id} in {elapsed:.2f}s")
    print(f"  Seats successfully held:     {held_count}")
    print(f"  Bookings successfully confirmed: {confirmed_count}")

    if confirmed_count == 1:
        print("  PASS -- exactly one booking succeeded, as required.")
    else:
        print(f"  FAIL -- expected exactly 1 confirmed booking, got {confirmed_count}.")

    conflict_statuses = [r["hold_status"] for r in results if r["hold_status"] != 200]
    print(f"  Rejected hold attempts: {len(conflict_statuses)} (status codes: {sorted(set(conflict_statuses))})")


if __name__ == "__main__":
    asyncio.run(main())

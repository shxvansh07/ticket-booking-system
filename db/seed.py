"""
Seeds a couple of sample events with realistic seat maps.

Run: python seed.py
Requires: pip install psycopg[binary]  (or psycopg2-binary)
"""
import datetime
import psycopg

DSN = "dbname=ticketbooking"

EVENTS = [
    {
        "name": "Arijit Singh — Live in Bengaluru",
        "venue": "Sree Kanteerava Stadium",
        "days_from_now": 30,
        "high_demand": True,
        "sections": [("A", 5, 20, 250000), ("B", 8, 24, 150000), ("C", 10, 30, 80000)],
    },
    {
        "name": "Bengaluru Tech Meetup — AI & Systems",
        "venue": "MIT Bengaluru Auditorium",
        "days_from_now": 10,
        "high_demand": False,
        "sections": [("General", 6, 15, 0)],
    },
]


def main():
    with psycopg.connect(DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE bookings, seat_events_log, seats, events RESTART IDENTITY CASCADE;")

            for ev in EVENTS:
                event_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=ev["days_from_now"])
                cur.execute(
                    "INSERT INTO events (name, venue, event_time, high_demand) VALUES (%s, %s, %s, %s) RETURNING id",
                    (ev["name"], ev["venue"], event_time, ev["high_demand"]),
                )
                event_id = cur.fetchone()[0]

                seat_rows = []
                for section, n_rows, seats_per_row, price_cents in ev["sections"]:
                    for row_idx in range(1, n_rows + 1):
                        row_label = chr(ord("A") + (row_idx - 1) % 26)
                        for seat_num in range(1, seats_per_row + 1):
                            seat_rows.append((event_id, section, row_label, seat_num, price_cents))

                cur.executemany(
                    "INSERT INTO seats (event_id, section, row_label, seat_number, price_cents) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    seat_rows,
                )
                print(f"Seeded event {event_id!r} ({ev['name']}) with {len(seat_rows)} seats")


if __name__ == "__main__":
    main()

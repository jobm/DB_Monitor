"""Publish synthetic Debezium CDC events to the Kind cluster's Kafka topic.

Run through the Makefile target (it pipes this file into the consumer pod,
which already has aiokafka installed):

    make monitor-seed-kind                 # ids 1..N
    SEED_BASE=100 make monitor-seed-kind   # a second, collision-free batch
"""

from __future__ import annotations

import asyncio
import json
import os
import time

from aiokafka import AIOKafkaProducer

TOPIC = "orderdb.public.orders"
BOOTSTRAP = "kafka:9092"

TABLES = {
    "orders": [
        ("id", "int32", True),
        ("customer_id", "int32", False),
        ("order_date", "int64", False),
        ("status", "string", False),
        ("total_amount", "double", False),
    ],
    "customers": [
        ("id", "int32", True),
        ("name", "string", False),
        ("email", "string", False),
        ("loyalty_tier", "string", False),
        ("created_at", "int64", False),
    ],
    "order_items": [
        ("id", "int32", True),
        ("order_id", "int32", False),
        ("product_name", "string", False),
        ("quantity", "int32", False),
        ("unit_price", "double", False),
    ],
}


def row_field(name: str, type_: str, pk: bool) -> dict:
    return {
        "field": name,
        "type": type_,
        "optional": not pk,
        "primaryKey": pk,
    }


def envelope_schema(table: str) -> dict:
    fields = [
        row_field(name, type_, pk) for name, type_, pk in TABLES[table]
    ]
    return {
        "type": "struct",
        "optional": False,
        "name": f"orderdb.public.{table}.Envelope",
        "fields": [
            {
                "type": "struct",
                "fields": fields,
                "optional": True,
                "name": f"orderdb.public.{table}.Value",
                "field": "before",
            },
            {
                "type": "struct",
                "fields": fields,
                "optional": True,
                "name": f"orderdb.public.{table}.Value",
                "field": "after",
            },
            {
                "type": "struct",
                "optional": False,
                "name": "io.debezium.connector.postgresql.Source",
                "field": "source",
                "fields": [
                    {"field": "version", "type": "string"},
                    {"field": "connector", "type": "string"},
                    {"field": "name", "type": "string"},
                    {"field": "ts_ms", "type": "int64"},
                    {"field": "snapshot", "type": "string"},
                    {"field": "db", "type": "string"},
                    {"field": "schema", "type": "string"},
                    {"field": "table", "type": "string"},
                ],
            },
            {"field": "op", "type": "string"},
            {"field": "ts_ms", "type": "int64"},
        ],
    }


def message(table: str, op: str, before: dict | None, after: dict | None,
            ts_ms: int) -> dict:
    return {
        "schema": envelope_schema(table),
        "payload": {
            "before": before,
            "after": after,
            "source": {
                "version": "2.5.0.Final",
                "connector": "postgresql",
                "name": "orderdb",
                "ts_ms": ts_ms,
                "snapshot": "false",
                "db": "orderdb",
                "schema": "public",
                "table": table,
            },
            "op": op,
            "ts_ms": ts_ms,
            "transaction": None,
        },
    }


def build_events() -> list[tuple[str, dict | None, dict]]:
    """Return (key, msg, table) triples covering creates/updates/deletes."""
    base = int(time.time() * 1000) - 60_000
    id_base = int(os.environ.get("SEED_BASE", "0"))
    events: list[tuple[str, dict, str]] = []

    def emit(table: str, op: str, before, after, ts_ms: int) -> None:
        row = after or before or {}
        key = json.dumps({"id": row.get("id")})
        events.append((key, message(table, op, before, after, ts_ms), table))

    # orders: inserts, an update, a delete
    orders: dict[int, dict] = {}
    for i in range(1, 13):
        oid = id_base + i
        ts = base + i * 1000
        row = {
            "id": oid,
            "customer_id": (i % 5) + 1,
            "order_date": ts,
            "status": "pending",
            "total_amount": round(19.99 + i * 7.5, 2),
        }
        emit("orders", "c", None, row, ts)
        orders[oid] = row
    for i in (2, 5, 9):
        oid = id_base + i
        ts = base + 20_000 + i * 100
        old = dict(orders[oid])
        new = {**old, "status": "shipped",
               "total_amount": round(old["total_amount"] + 12.0, 2)}
        emit("orders", "u", old, new, ts)
        orders[oid] = new
    for i in (3, 11):
        oid = id_base + i
        ts = base + 30_000 + i * 100
        emit("orders", "d", orders[oid], None, ts)

    # customers
    for i in range(1, 6):
        cid = id_base + i
        ts = base + 40_000 + i * 500
        emit("customers", "c", None, {
            "id": cid,
            "name": f"Customer {cid}",
            "email": f"customer{cid}@example.com",
            "loyalty_tier": ["bronze", "silver", "gold"][i % 3],
            "created_at": ts,
        }, ts)
    ts = base + 45_000
    cid = id_base + 2
    emit("customers", "u",
         {"id": cid, "name": f"Customer {cid}",
          "email": f"customer{cid}@example.com",
          "loyalty_tier": "silver", "created_at": base + 41_000},
         {"id": cid, "name": f"Customer {cid}",
          "email": f"customer{cid}@example.com",
          "loyalty_tier": "gold", "created_at": base + 41_000}, ts)

    # order_items
    item_id = id_base + 1
    for order_index in range(1, 7):
        for _n in range(2):
            ts = base + 50_000 + (item_id % 1000) * 300
            emit("order_items", "c", None, {
                "id": item_id,
                "order_id": id_base + order_index,
                "product_name": f"Product-{item_id:03d}",
                "quantity": (item_id % 3) + 1,
                "unit_price": round(4.5 + (item_id % 50) * 1.25, 2),
            }, ts)
            item_id += 1

    return events


async def main() -> None:
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode(),
        key_serializer=lambda v: v.encode() if v else None,
        compression_type=None,
        linger_ms=5,
    )
    await producer.start()
    try:
        events = build_events()
        for key, msg, table in events:
            await producer.send(TOPIC, msg, key=key)
        await producer.flush()
        counts: dict[str, int] = {}
        for _, _, table in events:
            counts[table] = counts.get(table, 0) + 1
        print(f"published {len(events)} events to {TOPIC}: {counts}")
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())

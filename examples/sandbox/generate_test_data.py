from __future__ import annotations

import argparse
import random
import string
import time

import psycopg2


DB_CONFIGS = {
    "order": "postgresql://postgres:postgres@localhost:5434/postgres",
    "catalog": "postgresql://postgres:postgres@localhost:5435/postgres",
    "shipping": "postgresql://postgres:postgres@localhost:5436/postgres",
}


def random_string(length: int = 10) -> str:
    """Return a random ASCII string for sandbox record generation."""
    return "".join(random.choices(string.ascii_letters, k=length))


def run_order_generator(conn, cursor, count: int) -> None:
    """Generate sandbox order-domain writes."""
    print(f"Generating {count} order events...")
    for _ in range(count):
        name = random_string(8)
        email = f"{name.lower()}@example.com"
        cursor.execute(
            (
                "INSERT INTO customers (name, email) VALUES (%s, %s) "
                "RETURNING id;"
            ),
            (name, email),
        )
        customer_id = cursor.fetchone()[0]

        amount = round(random.uniform(10.0, 500.0), 2)
        cursor.execute(
            (
                "INSERT INTO orders (customer_id, order_date, total_amount) "
                "VALUES (%s, NOW(), %s) RETURNING id;"
            ),
            (customer_id, amount),
        )
        order_id = cursor.fetchone()[0]

        if random.random() > 0.5:
            new_amount = amount + 20.0
            cursor.execute(
                "UPDATE orders SET total_amount = %s WHERE id = %s;",
                (new_amount, order_id),
            )

        if random.random() > 0.8:
            cursor.execute(
                "DELETE FROM orders WHERE id = %s;",
                (order_id,),
            )

        conn.commit()


def run_catalog_generator(conn, cursor, count: int) -> None:
    """Generate sandbox catalog-domain writes."""
    print(f"Generating {count} catalog events...")
    for _ in range(count):
        cat_name = random_string(6)
        cursor.execute(
            "INSERT INTO categories (name) VALUES (%s) RETURNING id;",
            (cat_name,),
        )
        cursor.fetchone()[0]

        prod_name = random_string(12)
        price = round(random.uniform(5.0, 100.0), 2)
        cursor.execute(
            "INSERT INTO products (name, price) VALUES (%s, %s) RETURNING id;",
            (prod_name, price),
        )
        prod_id = cursor.fetchone()[0]

        if random.random() > 0.5:
            new_price = price * 0.9
            cursor.execute(
                "UPDATE products SET price = %s WHERE id = %s;",
                (new_price, prod_id),
            )

        conn.commit()


def run_shipping_generator(conn, cursor, count: int) -> None:
    """Generate sandbox shipping-domain writes."""
    print(f"Generating {count} shipping events...")
    for _ in range(count):
        driver_name = random_string(10)
        cursor.execute(
            "INSERT INTO drivers (name) VALUES (%s) RETURNING id;",
            (driver_name,),
        )
        cursor.fetchone()[0]

        order_id = random.randint(1, 1000)
        status = random.choice(
            ["pending", "shipped", "delivered", "returned"]
        )
        cursor.execute(
            (
                "INSERT INTO shipments (order_id, shipped_at, status) "
                "VALUES (%s, NOW(), %s) RETURNING id;"
            ),
            (order_id, status),
        )
        shipment_id = cursor.fetchone()[0]

        if status != "delivered" and random.random() > 0.5:
            cursor.execute(
                "UPDATE shipments SET status = 'delivered' WHERE id = %s;",
                (shipment_id,),
            )

        conn.commit()


def generate_all(count: int, delay: float) -> None:
    """Generate writes across all bundled sandbox databases."""
    connections = {}
    cursors = {}
    for db_name, url in DB_CONFIGS.items():
        conn = psycopg2.connect(url)
        connections[db_name] = conn
        cursors[db_name] = conn.cursor()

    try:
        run_order_generator(connections["order"], cursors["order"], count)
        time.sleep(delay)
        run_catalog_generator(
            connections["catalog"],
            cursors["catalog"],
            count,
        )
        time.sleep(delay)
        run_shipping_generator(
            connections["shipping"],
            cursors["shipping"],
            count,
        )
        print("Finished generating test data.")
    finally:
        for cursor in cursors.values():
            cursor.close()
        for conn in connections.values():
            conn.close()


def main() -> int:
    """Run the bundled sandbox data generator via the example entrypoint."""
    parser = argparse.ArgumentParser(
        description="Generate random CDC data in the bundled sandbox DBs.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of entity groups to insert per sandbox database.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Delay between sandbox database operations.",
    )
    args = parser.parse_args()

    generate_all(args.count, args.delay)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

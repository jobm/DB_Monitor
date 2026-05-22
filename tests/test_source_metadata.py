from __future__ import annotations

from source_metadata import extract_source_coordinates


def test_extract_source_coordinates_supports_schema_qualified_tables() -> None:
    coordinates = extract_source_coordinates(
        {
            "source": {
                "name": "sqlserver1",
                "db": "inventory",
                "schema": "sales",
                "table": "orders",
            }
        }
    )

    assert coordinates is not None
    assert coordinates.service_name == "sqlserver1"
    assert coordinates.database_name == "inventory"
    assert coordinates.namespace_name == "sales"
    assert coordinates.raw_table_name == "orders"
    assert coordinates.table_name == "sales.orders"
    assert coordinates.table_filter_names() == [
        "orders",
        "sales.orders",
        "inventory.orders",
        "inventory.sales.orders",
    ]


def test_extract_source_coordinates_supports_collection_sources() -> None:
    coordinates = extract_source_coordinates(
        {
            "payload": {
                "source": {
                    "name": "mongo1",
                    "db": "inventory",
                    "collection": "orders",
                }
            }
        }
    )

    assert coordinates is not None
    assert coordinates.service_name == "mongo1"
    assert coordinates.database_name == "inventory"
    assert coordinates.namespace_name is None
    assert coordinates.raw_table_name == "orders"
    assert coordinates.table_name == "orders"

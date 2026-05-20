from __future__ import annotations

from schema_discovery import SchemaDiscovery


def test_extract_column_definitions_falls_back_to_payload_rows():
    discovery = SchemaDiscovery(lambda: None)

    columns = discovery._extract_column_definitions(
        {
            "before": {"id": 1, "status": "pending"},
            "after": {
                "id": 1,
                "status": "paid",
                "active": True,
                "metadata": {"source": "checkout"},
            },
        }
    )

    by_name = {column["column_name"]: column for column in columns}

    assert set(by_name) == {"id", "status", "active", "metadata"}
    assert by_name["id"]["data_type"] == "integer"
    assert by_name["status"]["data_type"] == "string"
    assert by_name["active"]["data_type"] == "boolean"
    assert by_name["metadata"]["data_type"] == "object"


def test_extract_column_definitions_reads_debezium_schema_envelope():
    discovery = SchemaDiscovery(lambda: None)

    columns = discovery._extract_column_definitions(
        {
            "schema": {
                "fields": [
                    {
                        "field": "before",
                        "fields": [
                            {"field": "id", "type": "int32", "optional": False},
                            {"field": "status", "type": "string", "optional": True},
                        ],
                    },
                    {
                        "field": "after",
                        "fields": [
                            {"field": "id", "type": "int32", "optional": False},
                            {"field": "status", "type": "string", "optional": True},
                            {"field": "__deleted", "type": "string", "optional": True},
                        ],
                    },
                ]
            }
        }
    )

    assert columns == [
        {
            "column_name": "id",
            "data_type": "int32",
            "is_primary_key": False,
            "is_nullable": False,
            "audit_enabled": True,
        },
        {
            "column_name": "status",
            "data_type": "string",
            "is_primary_key": False,
            "is_nullable": True,
            "audit_enabled": True,
        },
    ]

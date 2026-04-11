# DB Monitor API Documentation

This document describes the RESTful API endpoints exposed by the DB Monitor application.

## Authentication

Most endpoints require authentication using an API Key. Depending on your configuration, API keys might be passed via a header (e.g., `X-API-Key` or `Authorization: Bearer <key>`). 

The system uses Role-Based Access Control (RBAC) with `viewer` and `admin` roles.

### Bootstrap an Admin Key

The very first API key must be bootstrapped. This endpoint will **fail** if any active keys already exist.

**Endpoint:** `POST /auth/bootstrap`

**Query Parameters:**
- `owner_name` (string, required): A descriptor for who owns this key.

**Response:**
```json
{
  "api_key": "1.aBcDeFgHiJkLmNoPqRsTuVwXyZ",
  "owner_name": "admin_user",
  "role": "admin",
  "message": "Store this ADMIN key securely. It cannot be retrieved again."
}
```

### Create New Keys (Admin Only)

**Endpoint:** `POST /auth/keys`

**Query Parameters:**
- `owner_name` (string, required): A descriptor for the key owner.
- `role` (string, optional): The role to assign (`admin` or `viewer`). Defaults to `viewer`.

**Response:**
```json
{
  "api_key": "2.xYz...",
  "owner_name": "dev_user",
  "role": "viewer",
  "message": "Store this key securely. It cannot be retrieved again."
}
```

---

## Schema Discovery

### 1. List Monitored Tables
**Endpoint:** `GET /tables`
**Role:** Viewer

**Response:**
```json
{
  "tables": [...],
  "count": 10
}
```

### 2. Get Table Details
**Endpoint:** `GET /tables/{service_name}/{table_name}`
**Role:** Viewer

### 3. Get Table Columns
**Endpoint:** `GET /tables/{service_name}/{table_name}/columns`
**Role:** Viewer

---

## Event Tracking

### 1. Search Captured Events
**Endpoint:** `GET /events`
**Role:** Viewer

**Query Parameters:**
- `limit` (integer, default: 100, max: 1000): Number of records to return.
- `offset` (integer, default: 0): Pagination offset.
- `service_name` (string, optional): Filter by source service.
- `event_type` (string, optional): Filter by event type/operation (e.g., INSERT, UPDATE).
- `start_time` (string, optional): ISO8601 timestamp.
- `end_time` (string, optional): ISO8601 timestamp.
- `search_term` (string, optional): Search within the raw JSON payload.

### 2. Event Statistics
**Endpoint:** `GET /events/stats`
**Role:** Viewer

Returns aggregate statistics total events grouped by service, type, and database operation.

---

## Change History & Temporal Queries

### 1. Query Column Changes
**Endpoint:** `GET /changes`
**Role:** Viewer

**Query Parameters:**
- `table_name` (string, required): Full table name identifier (e.g., `orderdb.orders`).
- `column_name` (string, optional): Filter history to a specific column.
- `from_time` (string, optional): ISO8601 timestamp.
- `to_time` (string, optional): ISO8601 timestamp.
- `limit` (integer, default: 100): Result limit.

### 2. Point-in-Time Lookup
**Endpoint:** `GET /changes/{table_name}/{column_name}/at`
**Role:** Viewer
**Description:** Get the exact value of a column at a specific historical point in time.

**Query Parameters:**
- `timestamp` (string, required): ISO8601 timestamp.

---

## System Information

### 1. Health Check
**Endpoint:** `GET /health`
**Role:** Public (No authentication required)

Returns the health status of the application and whether it is currently shutting down.

### 2. App Info
**Endpoint:** `GET /info`
**Role:** Viewer

Returns basic metadata about the running FastAPI application instance.

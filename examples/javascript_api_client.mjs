import process from "node:process";

import { DBMonitorClient } from "../sdk/javascript/db_monitor_client.mjs";

const baseUrl = process.env.DB_MONITOR_BASE_URL || "http://localhost:8000";
const apiKey = process.env.DB_MONITOR_API_KEY;

// Default target table. Adjust to your custom database table when using in production.
// In the bundled evaluation sandbox environment, this defaults to 'orderdb.public.orders'.
const tableName = process.env.DB_MONITOR_TABLE || "orderdb.public.orders";
const rowIdentityRaw = process.env.DB_MONITOR_ROW_IDENTITY;

if (!apiKey) {
  throw new Error("Set DB_MONITOR_API_KEY before running this example.");
}

const client = new DBMonitorClient({ baseUrl, apiKey });
await client.exchangeAccessToken();

const tables = await client.getTables();
const events = await client.getEvents({ limit: 5 });
const changes = await client.getChanges({
  tableName,
  rowIdentity: rowIdentityRaw ? JSON.parse(rowIdentityRaw) : null,
  limit: 5,
});

console.log("Tables:");
console.log(JSON.stringify(tables, null, 2));
console.log("\nRecent events:");
console.log(JSON.stringify(events, null, 2));
console.log("\nRecent changes:");
console.log(JSON.stringify(changes, null, 2));
import urllib.request
import urllib.parse
import json
import os

BASE_URL = "http://localhost:8000"
api_key = os.getenv("DB_MONITOR_ADMIN_API_KEY")


def exchange_access_token(admin_api_key: str) -> str:
    req = urllib.request.Request(f"{BASE_URL}/auth/token", method="POST")
    req.add_header("x-api-key", admin_api_key)
    with urllib.request.urlopen(req) as response:
        return json.loads(response.read().decode())["access_token"]

# 1. Bootstrap the first admin key when bootstrap is enabled, otherwise rely on
# DB_MONITOR_ADMIN_API_KEY.
if not api_key:
    req = urllib.request.Request(
        f"{BASE_URL}/auth/bootstrap?owner_name=system_admin",
        method="POST",
    )
    with urllib.request.urlopen(req) as response:
        data = json.loads(response.read().decode())
        api_key = data["api_key"]
        print(f"Got Admin Key: {api_key}")

# 2. Exchange the admin key for a short-lived bearer token.
access_token = exchange_access_token(api_key)

# 3. Use the bearer token to hit /info (Requires Viewer Role Minimum)
req2 = urllib.request.Request(f"{BASE_URL}/info")
req2.add_header("authorization", f"Bearer {access_token}")
with urllib.request.urlopen(req2) as response:
    print(f"Info Status: {response.getcode()}")

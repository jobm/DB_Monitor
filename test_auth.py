import urllib.request
import urllib.parse
import json

# 1. Bootstrap the first admin key
req = urllib.request.Request("http://localhost:8000/auth/bootstrap?owner_name=system_admin", method="POST")
with urllib.request.urlopen(req) as response:
    data = json.loads(response.read().decode())
    api_key = data["api_key"]
    print(f"Got Admin Key: {api_key}")

# 2. Use the admin key to hit /info (Requires Viewer Role Minimum)
req2 = urllib.request.Request("http://localhost:8000/info")
req2.add_header("x-api-key", api_key)
with urllib.request.urlopen(req2) as response:
    print(f"Info Status: {response.getcode()}")

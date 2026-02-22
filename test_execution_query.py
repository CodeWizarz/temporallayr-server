import urllib.request
import urllib.error
import json
import time

URL = "http://localhost:8000/v1/query"
HEADERS = {"Content-Type": "application/json", "Authorization": "Bearer dev-test-key"}

# 1. Test Valid Query
valid_payload = {"query": "crash", "limit": 50}

req_valid = urllib.request.Request(
    URL, data=json.dumps(valid_payload).encode(), headers=HEADERS
)
try:
    response = urllib.request.urlopen(req_valid)
    print("Valid Payload Response:", response.read().decode())
    print("  -> Passed Valid Query Check\n")
except urllib.error.HTTPError as e:
    print("Valid Payload Failed with HTTP", e.code)
    print("Response Body:", e.read().decode())

# 2. Test Invalid Auth
req_invalid = urllib.request.Request(
    URL,
    data=json.dumps(valid_payload).encode(),
    headers={"Content-Type": "application/json"},
)
try:
    urllib.request.urlopen(req_invalid)
    print("Invalid Auth unexpectedly succeeded.")
except urllib.error.HTTPError as e:
    print(f"Invalid Auth correctly rejected with status: {e.code}")
    print("  -> Passed Auth Rejection\n")

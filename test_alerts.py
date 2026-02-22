import urllib.request
import urllib.error
import json

URL = "http://localhost:8000/v1/alerts"
HEADERS = {"Content-Type": "application/json", "Authorization": "Bearer dev-test-key"}

# 1. Test Valid URL
valid_payload = {
    "name": "runtime failures",
    "failure_type": "runtime_error",
    "node_name": None,
    "webhook_url": "https://example.com/webhook",
}

req_valid = urllib.request.Request(
    URL, data=json.dumps(valid_payload).encode(), headers=HEADERS
)
try:
    response = urllib.request.urlopen(req_valid)
    print("Valid Payload Response:", response.read().decode())
    print("  -> Passed Valid URL Check\n")
except Exception as e:
    print("Valid Payload Failed:", e)

# 2. Test Invalid URL
invalid_payload = {
    "name": "runtime failures",
    "failure_type": "runtime_error",
    "node_name": None,
    "webhook_url": "bad-url-string",
}

req_invalid = urllib.request.Request(
    URL, data=json.dumps(invalid_payload).encode(), headers=HEADERS
)
try:
    urllib.request.urlopen(req_invalid)
    print("Invalid Payload unexpectedly succeeded.")
except urllib.error.HTTPError as e:
    print(f"Invalid Payload correctly rejected with status: {e.code}")
    print(f"  -> {e.read().decode()}")
    print("  -> Passed Invalid URL Rejection\n")

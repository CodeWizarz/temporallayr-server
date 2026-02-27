import os
import sys

print("--- RAILWAY ENV DUMP ---")
for k, v in os.environ.items():
    if "KEY" in k or "SECRET" in k or "PASSWORD" in k or "URL" in k:
        print(f"{k}: ***HIDDEN***")
    else:
        print(f"{k}: {v}")
print("------------------------")

#!/usr/bin/env python3

import requests

# Test the various endpoints
base_url = "http://localhost:8081"

print("Testing S1 API endpoints...")

try:
    # Test GetBucketLocation
    print("\n1. Testing GetBucketLocation (?location=)")
    response = requests.get(f"{base_url}/astronauts?location=")
    print(f"Status: {response.status_code}")
    print(f"Content-Type: {response.headers.get('content-type')}")
    print(f"Response: {response.text[:200]}...")

    # Test ListObjects (default)
    print("\n2. Testing ListObjects (no params)")
    response = requests.get(f"{base_url}/astronauts")
    print(f"Status: {response.status_code}")
    print(f"Content-Type: {response.headers.get('content-type')}")
    print(f"Response: {response.text[:200]}...")

    # Test ListObjects with delimiter
    print("\n3. Testing ListObjects (?delimiter=/)")
    response = requests.get(f"{base_url}/astronauts?delimiter=/")
    print(f"Status: {response.status_code}")
    print(f"Content-Type: {response.headers.get('content-type')}")
    print(f"Response: {response.text[:200]}...")

except requests.exceptions.ConnectionError:
    print("Error: Could not connect to server. Make sure it's running on port 8081")
except Exception as e:
    print(f"Error: {e}")
#!/usr/bin/env python3
"""
Example: Running S1 with different storage backends

This script demonstrates how to configure S1 to use either GCS or local filesystem
as the storage backend with LRU caching enabled.
"""

# Example 1: Using Local Filesystem Backend
# ------------------------------------------
# Set these environment variables before running the server:
#
# export STORAGE_BACKEND=local
# export LOCAL_STORAGE_PATH=/data
# export STORAGE_CACHE_SIZE=256
# python src/main.py
#
# This will:
# - Use local filesystem at /data as storage
# - Enable LRU cache with size 256
# - Files organized as: /data/{bucket}/{object}

# Example 2: Using Google Cloud Storage Backend
# ----------------------------------------------
# Set these environment variables before running the server:
#
# export STORAGE_BACKEND=gcs
# export GCS_PROJECT=my-project-id
# export STORAGE_CACHE_SIZE=512
# python src/main.py
#
# This will:
# - Use GCS as storage backend
# - Enable LRU cache with size 512
# - Connect to specified GCS project

# Example 3: Using GCS Emulator for Testing
# -----------------------------------------
# Set these environment variables before running the server:
#
# export STORAGE_BACKEND=gcs
# export STORAGE_EMULATOR_HOST=http://localhost:9023
# export GCS_PROJECT=test-project
# python src/main.py
#
# This will:
# - Use GCS emulator for testing
# - Connect to local emulator at port 9023

# Example 4: Default Configuration
# --------------------------------
# No environment variables set (uses defaults):
#
# python src/main.py
#
# This will:
# - Use GCS backend (default)
# - Project: PROJECT
# - Cache size: 128 (default)

# Cache Benefits
# -------------
# The LRU cache significantly improves performance when:
# - Same objects are accessed multiple times
# - Serving as a caching layer for query engines like Opteryx
# - Multiple S3 Select queries on the same data
#
# Cache statistics are maintained internally and can be accessed
# programmatically via the storage backend's cache_info() method.

print("S1 Storage Backend Configuration Examples")
print("==========================================")
print()
print("See this file for configuration examples.")
print()
print("To run S1 with local filesystem:")
print("  STORAGE_BACKEND=local LOCAL_STORAGE_PATH=/data python src/main.py")
print()
print("To run S1 with GCS:")
print("  STORAGE_BACKEND=gcs GCS_PROJECT=my-project python src/main.py")
print()
print("To run S1 with custom cache size:")
print("  STORAGE_CACHE_SIZE=512 python src/main.py")

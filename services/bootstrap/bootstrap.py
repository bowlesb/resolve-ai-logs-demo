"""
Graylog Bootstrap Script

This script automates the initial setup of a Graylog instance by ensuring:
  1. The Graylog API is reachable before continuing.
  2. A default index set exists (created if missing) with a size-based rotation
     strategy and retention policy.
  3. A global GELF UDP input is configured for receiving log messages.

Environment Variables:
  GRAYLOG_API       - Base URL for the Graylog API (default: http://graylog:9000/api)
  GRAYLOG_USER      - Graylog admin username (default: admin)
  GRAYLOG_PASS      - Graylog admin password (default: admin)
  INDEX_NAME        - Name of the index set to create (default: demo-5gb)
  INDEX_MAX_MB      - Max size per index in MB before rotation (default: 250)
  INDEX_MAX_COUNT   - Max number of indices to retain (default: 20)
  INPUT_TITLE       - Title of the GELF UDP input (default: gelf-udp-12201)

Usage:
  Run as a standalone script after Graylog is up, typically from a container's
  entrypoint or an orchestration bootstrap step. It will exit with a non-zero
  status if the Graylog API cannot be reached in time.
"""

import os
import time
import requests
import json
import sys

API = os.environ.get("GRAYLOG_API", "http://graylog:9000/api").rstrip("/")
USER = os.environ.get("GRAYLOG_USER", "admin")
PASS = os.environ.get("GRAYLOG_PASS", "admin")
INDEX_NAME = os.environ.get("INDEX_NAME", "demo-5gb")
INDEX_MAX_MB = int(os.environ.get("INDEX_MAX_MB", "250"))
INDEX_MAX_COUNT = int(os.environ.get("INDEX_MAX_COUNT", "20"))
INPUT_TITLE = os.environ.get("INPUT_TITLE", "gelf-udp-12201")

session = requests.Session()
session.auth = (USER, PASS)
session.headers.update(
    {
        "X-Requested-By": "bootstrap",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
)


def wait_healthy():
    for attempt in range(120):
        try:
            response = session.get(f"{API}/system")
            if response.ok:
                print("Graylog API up")
                return
        except Exception:
            pass
        time.sleep(2)
    print("Graylog API not reachable in time", file=sys.stderr)
    sys.exit(1)


def ensure_gelf_udp():
    response = session.get(f"{API}/system/inputs")
    if not response.ok:
        print("Failed to list inputs:", response.text, file=sys.stderr)
        return
    for input_item in response.json().get("inputs", []):
        if input_item.get("title") == INPUT_TITLE:
            print("GELF UDP input already exists")
            return
    payload = {
        "title": INPUT_TITLE,
        "global": True,
        "type": "org.graylog2.inputs.gelf.udp.GELFUDPInput",
        "configuration": {
            "decompress_size_limit": 8388608,
            "bind_address": "0.0.0.0",
            "port": 12201,
            "recv_buffer_size": 1048576,
        },
    }
    response = session.post(f"{API}/system/inputs", data=json.dumps(payload))
    if response.ok:
        print("Created GELF UDP input")
    else:
        print("Failed to create GELF UDP input:", response.text, file=sys.stderr)


def ensure_index_set():
    response = session.get(f"{API}/system/indices/index_sets")
    if not response.ok:
        print("Failed to list index sets:", response.text, file=sys.stderr)
        return
    exists = any(
        index_set.get("title") == INDEX_NAME
        for index_set in response.json().get("index_sets", [])
    )
    if exists:
        print("Index set already exists")
        return
    payload = {
        "title": INDEX_NAME,
        "description": "Demo ~5GB index set (size-based rotation)",
        "index_prefix": "demo",
        "rotation_strategy_class": "org.graylog2.indexer.rotation.strategies.SizeBasedRotationStrategy",
        "rotation_strategy": {"max_size": INDEX_MAX_MB},
        "retention_strategy_class": "org.graylog2.indexer.retention.strategies.DeletionRetentionStrategy",
        "retention_strategy": {"max_number_of_indices": INDEX_MAX_COUNT},
        "shards": 1,
        "replicas": 0,
        "index_optimization_disabled": False,
        "writable": True,
        "is_default": True,
    }
    response = session.post(
        f"{API}/system/indices/index_sets", data=json.dumps(payload)
    )
    if response.ok:
        print("Created index set and set as default")
    else:
        print("Failed to create index set:", response.text, file=sys.stderr)


if __name__ == "__main__":
    wait_healthy()
    ensure_index_set()
    ensure_gelf_udp()
    print("Bootstrap done")

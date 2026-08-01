#!/usr/bin/env python3
"""Deterministic HTTP parity check for the Elixir and Rust logs APIs."""

import argparse
import datetime
import json
import time
import urllib.parse
import urllib.request


def request(url, method="GET", body=None, content_type=None):
    headers = {}
    data = None if body is None else body.encode()
    if content_type:
        headers["content-type"] = content_type
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=30) as response:
        return response.status, response.read().decode()


def fixture(base_seconds, token):
    rows = []
    for i in range(100):
        rows.append(
            {
                "_time": base_seconds + i,
                "_msg": f"{token} request {i}",
                "level": "error" if i % 10 == 0 else "info",
                "service": "parity",
                "status": "500" if i % 10 == 0 else "200",
            }
        )
    return "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows)


def normalize_time(value):
    parsed = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    return int(parsed.timestamp() * 1000)


def exercise(base, body_fixture, token):
    status, body = request(
        base + "/insert/jsonline",
        method="POST",
        body=body_fixture,
        content_type="application/x-ndjson",
    )
    assert status == 204, (base, status, body)
    status, body = request(base + "/api/v1/flush")
    assert status == 200, (base, status, body)
    params = urllib.parse.urlencode({"message": token, "limit": 200, "order": "asc"})
    deadline = time.monotonic() + 10
    while True:
        status, body = request(base + "/select/logsql/query?" + params)
        assert status == 200, (base, status, body)
        rows = [json.loads(line) for line in body.splitlines() if line]
        if len(rows) == 100:
            break
        if len(rows) > 100 or time.monotonic() >= deadline:
            raise AssertionError((base, len(rows)))
        time.sleep(0.02)
    normalized = []
    for row in rows:
        normalized.append(
            {
                "time_ms": normalize_time(row.pop("_time")),
                "message": row.pop("_msg"),
                "level": row.pop("level"),
                "metadata": row,
            }
        )
    return normalized


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--elixir", default="http://127.0.0.1:19428")
    parser.add_argument("--rust", default="http://127.0.0.1:19429")
    args = parser.parse_args()

    base_seconds = int(datetime.datetime.now(datetime.timezone.utc).timestamp()) - 100
    token = f"api-parity-v3-{time.time_ns()}"
    body_fixture = fixture(base_seconds, token)
    elixir = exercise(args.elixir, body_fixture, token)
    rust = exercise(args.rust, body_fixture, token)
    assert len(elixir) == 100, len(elixir)
    assert rust == elixir, {"elixir": elixir[:3], "rust": rust[:3]}
    print("api_parity|100|exact")


if __name__ == "__main__":
    main()

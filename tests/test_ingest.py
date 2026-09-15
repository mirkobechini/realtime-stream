"""Test per lo script di ingest idempotente."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest
import redis.asyncio as aioredis

from app.ingest import ensure_group, main, normalize, parse_args, process_batch, run


def test_parse_args_defaults():
    args = parse_args([])
    assert args.host == "localhost"
    assert args.port == 6379
    assert args.stream == "events"
    assert args.group == "ingest"
    assert args.consumer == "ingest-1"
    assert args.once is False
    assert args.count == 10


def test_parse_args_custom():
    args = parse_args(
        ["--host", "redis", "--port", "6380", "--stream", "s", "--once", "--count", "5"]
    )
    assert args.host == "redis"
    assert args.port == 6380
    assert args.stream == "s"
    assert args.once is True
    assert args.count == 5


def test_normalize():
    raw = {
        "id": "abc",
        "event_type": "metric",
        "payload": '{"cpu": 0.5}',
        "source": "redis",
        "timestamp": "1700000000.0",
        "sequence": "1",
    }
    ev = normalize(raw)
    assert ev.id == "abc"
    assert ev.event_type == "metric"
    assert ev.payload == {"cpu": 0.5}
    assert ev.source == "redis"
    assert ev.timestamp == 1700000000.0
    assert ev.sequence == 1


@pytest.mark.asyncio
async def test_ensure_group_creates():
    r = AsyncMock()
    await ensure_group(r, "events", "ingest")
    r.xgroup_create.assert_awaited_once_with(
        "events", "ingest", id="0", mkstream=True
    )


@pytest.mark.asyncio
async def test_ensure_group_busygroup_ignored():
    r = AsyncMock()
    r.xgroup_create.side_effect = aioredis_error("BUSYGROUP exists")
    await ensure_group(r, "events", "ingest")  # non deve sollevare


@pytest.mark.asyncio
async def test_ensure_group_other_error_raises():
    r = AsyncMock()
    r.xgroup_create.side_effect = aioredis_error("SOMETHING ELSE")
    with pytest.raises(Exception):
        await ensure_group(r, "events", "ingest")


@pytest.mark.asyncio
async def test_process_batch_empty():
    r = AsyncMock()
    r.xreadgroup.return_value = []
    n = await process_batch(r, "events", "ingest", "c1", 10)
    assert n == 0
    r.xack.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_batch_acks_events():
    r = AsyncMock()
    r.xreadgroup.return_value = [
        (
            "events",
            [
                (
                    "1-0",
                    {
                        "id": "a",
                        "event_type": "metric",
                        "payload": "{}",
                        "source": "redis",
                        "timestamp": "1.0",
                        "sequence": "1",
                    },
                ),
                (
                    "2-0",
                    {
                        "id": "b",
                        "event_type": "status",
                        "payload": "{}",
                        "source": "redis",
                        "timestamp": "2.0",
                        "sequence": "2",
                    },
                ),
            ],
        )
    ]
    n = await process_batch(r, "events", "ingest", "c1", 10)
    assert n == 2
    assert r.xack.await_count == 2


@pytest.mark.asyncio
async def test_process_batch_handles_bad_event():
    r = AsyncMock()
    r.xreadgroup.return_value = [
        (
            "events",
            [
                ("1-0", {"bad": "fields"}),
                (
                    "2-0",
                    {
                        "id": "b",
                        "event_type": "status",
                        "payload": "{}",
                        "source": "redis",
                        "timestamp": "2.0",
                        "sequence": "2",
                    },
                ),
            ],
        )
    ]
    n = await process_batch(r, "events", "ingest", "c1", 10)
    assert n == 1  # solo il buono viene ack
    assert r.xack.await_count == 1


@pytest.mark.asyncio
async def test_run_once_processes_and_closes():
    r = AsyncMock()
    r.xreadgroup.return_value = []
    with patch("app.ingest.aioredis.from_url", return_value=r) as m:
        args = parse_args(["--once"])
        code = await run(args)
    assert code == 0
    m.assert_called_once()
    r.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_retries_on_connection_error():
    r = AsyncMock()
    r.xreadgroup.side_effect = [aioredis.ConnectionError("down"), []]
    with patch("app.ingest.aioredis.from_url", return_value=r):
        with patch("app.ingest.asyncio.sleep", new=AsyncMock()) as sleep:
            args = parse_args(["--once"])
            code = await run(args)
    assert code == 0
    sleep.assert_awaited_once()


def test_main_runs():
    with patch("app.ingest.asyncio.run") as m:
        with patch("app.ingest.parse_args", return_value=parse_args([])):
            main()
    m.assert_called_once()


def aioredis_error(msg: str) -> Exception:
    import redis.exceptions as rex

    return rex.ResponseError(msg)
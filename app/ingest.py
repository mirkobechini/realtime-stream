"""Script CLI di ingest idempotente.

Legge eventi dal broker Redis (Streams) e li normalizza nel modello canonico
`Event`. Idempotente grazie al consumer group: gli eventi già acknowledge non
vengono riletti a riesecuzioni successive.

Uso:
    python -m app.ingest [--host HOST] [--port PORT] [--stream STREAM]
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os

import redis.asyncio as aioredis

from app.models.event import Event

log = logging.getLogger("ingest")

DEFAULT_HOST = os.getenv("REDIS_HOST", "localhost")
DEFAULT_PORT = int(os.getenv("REDIS_PORT", "6379"))
DEFAULT_STREAM = os.getenv("REDIS_STREAM", "events")
DEFAULT_GROUP = os.getenv("REDIS_GROUP", "ingest")
DEFAULT_CONSUMER = os.getenv("REDIS_CONSUMER", "ingest-1")
BATCH = 10
BACKOFF = [1, 2, 4, 8]


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest eventi da Redis Streams")
    p.add_argument("--host", default=DEFAULT_HOST)
    p.add_argument("--port", type=int, default=DEFAULT_PORT)
    p.add_argument("--stream", default=DEFAULT_STREAM)
    p.add_argument("--group", default=DEFAULT_GROUP)
    p.add_argument("--consumer", default=DEFAULT_CONSUMER)
    p.add_argument("--once", action="store_true", help="processa un batch e esci")
    p.add_argument("--count", type=int, default=BATCH)
    return p.parse_args(args)


async def ensure_group(r: aioredis.Redis, stream: str, group: str) -> None:
    """Crea il consumer group se non esiste (idempotente)."""
    try:
        await r.xgroup_create(stream, group, id="0", mkstream=True)
        log.info("consumer group %s creato su %s", group, stream)
    except aioredis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def normalize(raw: dict[str, str]) -> Event:
    """Normalizza un evento grezzo Redis nel modello canonico Event."""
    return Event.from_stream(raw)


async def process_batch(
    r: aioredis.Redis,
    stream: str,
    group: str,
    consumer: str,
    count: int,
) -> int:
    """Legge e processa un batch di eventi, ack ogni evento. Ritorna il numero."""
    resp = await r.xreadgroup(
        group, consumer, {stream: ">"}, count=count, block=1000
    )
    if not resp:
        return 0
    n = 0
    for _, entries in resp:
        for msg_id, fields in entries:
            try:
                ev = normalize(fields)
                log.info("evento %s (%s) seq=%s", ev.id, ev.event_type, ev.sequence)
                await r.xack(stream, group, msg_id)
                n += 1
            except Exception as e:  # noqa: BLE001
                log.error("errore su msg %s: %s", msg_id, e)
    return n


async def run(args: argparse.Namespace) -> int:
    r = aioredis.from_url(
        f"redis://{args.host}:{args.port}",
        decode_responses=True,
    )
    await ensure_group(r, args.stream, args.group)
    total = 0
    attempt = 0
    try:
        while True:
            try:
                n = await process_batch(
                    r, args.stream, args.group, args.consumer, args.count
                )
                total += n
                attempt = 0
                if args.once:
                    break
            except (aioredis.ConnectionError, OSError) as e:
                delay = BACKOFF[min(attempt, len(BACKOFF) - 1)]
                log.warning("connessione persa (%s), retry in %ss", e, delay)
                await asyncio.sleep(delay)
                attempt += 1
    finally:
        await r.aclose()
    log.info("ingest completato: %d eventi", total)
    return 0


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
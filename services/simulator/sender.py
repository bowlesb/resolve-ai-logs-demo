"""
Ingest Traffic Simulator

A small multi-process, async HTTP load generator for the log distributor's
/ingest endpoint. It spawns N worker processes; each worker runs an asyncio
loop that posts randomized log packets at a fixed QPS rate.

What it does
------------
- Spawns WORKERS processes (daemon=True).
- Each worker:
  * Builds a packet of K messages (K ~ U[PACKET_MIN, PACKET_MAX]).
  * Each message has: timestamp (local, '%Y-%m-%dT%H:%M:%S'), level='INFO',
    message=random ASCII text, attrs={} (empty dict).
  * POSTs JSON to TARGET with shape:
      {"source_id": "sim-<i>", "messages": [ ... ]}
  * Sleeps to maintain QPS_PER_WORKER requests/sec.

Environment variables
---------------------
- TARGET (str): URL to POST, default "http://distributor:8000/ingest".
- PACKET_MIN (int): Minimum messages per packet, default 5.
- PACKET_MAX (int): Maximum messages per packet, default 20.
- WORKERS (int): Number of OS processes to spawn, default 4.
- QPS_PER_WORKER (float): Requests per second per worker, default 25.0.

"""


import os, time, random, string, asyncio
import httpx
from multiprocessing import Process
import logging
import sys

TARGET = os.environ.get("TARGET", "http://distributor:8000/ingest")
PACKET_MIN = int(os.environ.get("PACKET_MIN", "5"))
PACKET_MAX = int(os.environ.get("PACKET_MAX", "20"))
WORKERS = int(os.environ.get("WORKERS", "4"))
QPS_PER_WORKER = float(os.environ.get("QPS_PER_WORKER", "25"))


logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    force=True,
)
logger = logging.getLogger(__name__)


def rand_msg():
    n = random.randint(20, 80)
    return "".join(random.choices(string.ascii_letters + string.digits + " ", k=n))


async def worker_loop(i):
    client = httpx.AsyncClient(timeout=5.0)
    interval = 1.0 / QPS_PER_WORKER
    try:
        while True:
            k = random.randint(PACKET_MIN, PACKET_MAX)
            msgs = [
                {
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                    "level": "INFO",
                    "message": rand_msg(),
                    "attrs": {},
                }
                for _ in range(k)
            ]
            try:
                await client.post(
                    TARGET, json={"source_id": f"sim-{i}", "messages": msgs}
                )
            except Exception:
                pass
            await asyncio.sleep(interval)
    finally:
        await client.aclose()


def worker(i):
    asyncio.run(worker_loop(i))


if __name__ == "__main__":
    procs = [Process(target=worker, args=(i,), daemon=True) for i in range(WORKERS)]
    logger.info("Starting %d workers with QPS %f", WORKERS, QPS_PER_WORKER)
    for p in procs:
        p.start()
    for p in procs:
        p.join()

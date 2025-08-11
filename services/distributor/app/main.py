"""
Log Distributor (FastAPI + gRPC)

This service accepts POSTed packets of log messages and forwards each packet to one
of several analyzer services over gRPC. Routing is *weighted* per analyzer and
adapts to failures via a per-analyzer circuit breaker. Weights are live-updated
from MongoDB so you can steer traffic without restarting the service.

Key capabilities
----------------
- Weighted routing: random.choices() using a weight per analyzer.
- Resilience: per-analyzer SimpleCircuitBreaker (closed/open/half_open).
- Async I/O: FastAPI + grpc.aio for high throughput.
- Live config: background task watches MongoDB for weight changes.
- Introspection: /health endpoint reports analyzers, weights, and breaker states.

"""

import os, asyncio, random
import logging, sys
from typing import List, Dict, Tuple
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import grpc
from grpc.aio import AioRpcError
import pymongo
from types import SimpleNamespace
from prometheus_client import Counter, CONTENT_TYPE_LATEST, generate_latest

from . import logs_pb2, logs_pb2_grpc
from .simple_circuit_breaker import SimpleCircuitBreaker
from .constants import (
    MONGO_URI,
    ANALYZERS_ENV,
    ANALYZER_TIMEOUT_MS,
    DEFAULT_WEIGHTS_ENV,
    WEIGHT_POLL_SECS,
    CB_FAILURE_THRESHOLD,
    CB_RECOVERY_TIMEOUT_SEC,
    CB_HALF_OPEN_SUCC_THRESHOLD,
    DEFAULT_ANALYZER_TO_WEIGHTS,
    WEIGHTS_COL,
)


SUCCESS = Counter("distributor_analyzer_success_total", "Total successful analyzer calls")
FAILURE = Counter("distributor_analyzer_failure_total", "Total failed analyzer calls")



logging.basicConfig(
    stream=sys.stdout,
    level="INFO",
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)
app = FastAPI()

app.state.ctx = SimpleNamespace(
    channels={},  # Dict[str, grpc.aio.Channel]
    stubs={},  # Dict[str, logs_pb2_grpc.AnalyzerStub]
    weight_map={},  # Dict[str, float]
    analyzer_hosts={},  # Dict[str, Tuple[str, int]]
    circuit_breakers={},  # Dict[str, SimpleCircuitBreaker]
)


class LogMessage(BaseModel):
    timestamp: str
    level: str = "INFO"
    message: str
    attrs: Dict[str, str] = Field(default_factory=dict)


class LogPacket(BaseModel):
    source_id: str = "sim"
    messages: List[LogMessage]


def init_analyzer_ports() -> None:
    """Initialize analyzer_hosts: the host / ports for each analyzer."""
    ctx = app.state.ctx
    for entry in ANALYZERS_ENV.split(","):
        entry = entry.strip()
        if not entry:
            continue
        assert ":" in entry, f"Invalid entry, : not present in {entry}"
        host, port = entry.split(":", 1)
        ctx.analyzer_hosts[host] = (host, int(port))


def init_circuit_breakers() -> None:
    """Assign each analyzer a circuit breaker."""
    ctx = app.state.ctx
    for name in ctx.analyzer_hosts.keys():
        ctx.circuit_breakers[name] = SimpleCircuitBreaker(
            name,
            failure_threshold=CB_FAILURE_THRESHOLD,
            recovery_timeout=CB_RECOVERY_TIMEOUT_SEC,
            half_open_success_threshold=CB_HALF_OPEN_SUCC_THRESHOLD,
        )


async def init_grpc() -> None:
    ctx = app.state.ctx
    for name, (host, port) in ctx.analyzer_hosts.items():
        ch = grpc.aio.insecure_channel(f"{host}:{port}")
        ctx.channels[name] = ch
        ctx.stubs[name] = logs_pb2_grpc.AnalyzerStub(ch)


async def poll_weights_updates() -> None:
    """Keep weight_map up to date async based on whats set in Web UI."""
    ctx = app.state.ctx
    while True:
        mongo_result = WEIGHTS_COL.find_one({"_id": "weights"})
        if mongo_result is not None:
            current_weights_mapping = mongo_result.get("values", {})
        else:
            logger.warning("No weights found in MongoDB, using default weights.")
            current_weights_mapping = DEFAULT_ANALYZER_TO_WEIGHTS

        logger.debug(f"Current weights mapping: {current_weights_mapping}")
        ctx.weight_map.update(current_weights_mapping)
        await asyncio.sleep(WEIGHT_POLL_SECS)


@app.on_event("startup")
async def startup() -> None:
    ctx = app.state.ctx
    init_analyzer_ports()
    ctx.weight_map.update(DEFAULT_ANALYZER_TO_WEIGHTS)
    init_circuit_breakers()
    await init_grpc()
    asyncio.create_task(poll_weights_updates())
    logger.info(
        "Distributor started. analyzers=%s weights=%s",
        list(ctx.analyzer_hosts.keys()),
        ctx.weight_map,
    )


def weighted_analyzer_choice(candidates: List[str]) -> str:
    ctx = app.state.ctx
    current_weights = [ctx.weight_map.get(candidate, 0.0) for candidate in candidates]
    sum_weights = sum(current_weights)

    if sum_weights <= 0:
        logger.warning(
            "Sum of all weights cant be less than zero. Using even distribution."
        )
        current_weights = [1.0] * len(candidates)

    # No need for current_weights to sum to 1
    return random.choices(candidates, weights=current_weights, k=1)[0]


@app.get("/health")
def health():
    """Endpoint to probe the circuit breaker states and weights."""
    ctx = app.state.ctx
    breakers = {n: ctx.circuit_breakers[n].snapshot() for n in ctx.circuit_breakers}
    return {
        "ok": True,
        "analyzers": list(ctx.analyzer_hosts.keys()),
        "weights": ctx.weight_map,
        "breakers": breakers,
    }


@app.post("/ingest")
async def ingest(packet: LogPacket):
    """
    Ingest a log packet and send it to an analyzer.
    Some analyzers may start failing as per simulations set in Web UI.
    We will adapt to success/failure rates via assigned Circuit Breaker.
    """
    ctx = app.state.ctx
    candidates = list(ctx.analyzer_hosts.keys())
    if not candidates:
        raise HTTPException(
            status_code=503, detail="analyzer_hosts not yet initialized?"
        )

    import random, json
    if random.random() < 0.05:
        msg = json.dumps([(c, ctx.circuit_breakers[c].state) for c in candidates])
        logger.info(f"Simulated circuit breaker states: {msg}")

    tried = set()
    while len(tried) < len(candidates):
        remaining = [c for c in candidates if c not in tried]
        target = weighted_analyzer_choice(remaining)
        tried.add(target)

        if not ctx.circuit_breakers[target].allow_request():
            continue

        stub = ctx.stubs[target]
        try:
            # send the log to the chosen analyzer here
            req = logs_pb2.LogPacket(
                source_id=packet.source_id,
                messages=[
                    logs_pb2.LogMessage(
                        timestamp=m.timestamp,
                        level=m.level,
                        message=m.message,
                        attrs=m.attrs,
                    )
                    for m in packet.messages
                ],
            )
            
            _ = await stub.Analyze(req, timeout=ANALYZER_TIMEOUT_MS / 1000.0)
            ctx.circuit_breakers[target].record_success()
            SUCCESS.inc()
            return {"accepted_by": target, "count": len(packet.messages)}
        except AioRpcError:
            # analyzer failed or unavailable
            FAILURE.inc()
            ctx.circuit_breakers[target].record_failure()
            continue

    raise HTTPException(
        status_code=503, detail="All analyzers are blocked by circuit breakers."
    )

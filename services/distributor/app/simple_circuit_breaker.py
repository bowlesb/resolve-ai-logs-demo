"""
SimpleCircuitBreaker

A tiny, thread-safe circuit breaker for guarding calls to downstream
services. It tracks consecutive failures and moves between three states:

States
------
- CLOSED:      All calls allowed. Consecutive failures are counted. When the
               count >= failure_threshold, the breaker opens.
- OPEN:        All calls blocked until recovery_timeout seconds elapse, then
               the next allow_request() transitions to HALF_OPEN and allows a probe.
- HALF_OPEN:   Probe calls are allowed. If half_open_success_threshold successes
               occur without a failure, the breaker closes. Any failure instantly opens.

Design notes
-----------
- Thread-safety: A per-instance Lock protects all state transitions and counters.
- Time source: time.monotonic() is used for cooldown timing.
- Logging:
    * On every recorded failure, an INFO log "circuit_fail" includes a snapshot.
    * When the breaker trips OPEN, an INFO log "circuit_trip" is emitted.
    * On any state change, a DEBUG log "circuit_state_change" is emitted.
- Snapshot: snapshot() returns a dict suitable for /health-style diagnostics.

Configuration
-------------
name : str
    Identifier for logs and diagnostics.
failure_threshold : int, default=3
    Failures in CLOSED before opening the circuit.
recovery_timeout : float, default=5.0
    Seconds to remain OPEN before allowing a HALF_OPEN probe.
half_open_success_threshold : int, default=1
    Number of consecutive successes in HALF_OPEN required to fully close.

"""


from __future__ import annotations
import time
import logging
import copy
from enum import Enum
from threading import Lock

logger = logging.getLogger(__name__)


class BreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class SimpleCircuitBreaker:
    def __init__(
        self,
        name,
        failure_threshold: int = 3,
        recovery_timeout: float = 5.0,
        half_open_success_threshold: int = 1,
    ) -> None:
        self.name = name

        if failure_threshold <= 0:
            raise ValueError("failure_threshold must be > 0")
        if recovery_timeout <= 0:
            raise ValueError("recovery_timeout must be > 0")
        if half_open_success_threshold <= 0:
            raise ValueError("half_open_success_threshold must be > 0")

        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_success_threshold = half_open_success_threshold

        self.state: BreakerState = BreakerState.CLOSED
        self.consecutive_failures: int = 0
        self.half_open_successes: int = 0
        self.opened_at: float | None = None

        self.lock = Lock()

    def allow_request(self) -> bool:
        """Return True if a request should be attempted."""
        with self.lock:
            now = time.monotonic()
            if self.state == BreakerState.OPEN:
                if (
                    self.opened_at is not None
                    and (now - self.opened_at) >= self.recovery_timeout
                ):
                    # transition to HALF_OPEN and allow a probe request
                    prev = self.state
                    self.transition(BreakerState.HALF_OPEN, reason="cooldown elapsed")
                    self.half_open_successes = 0
                    return True
                return False  # waiting longer after last failure
            # CLOSED and HALF_OPEN both allow attempts
            return True

    def record_success(self) -> None:
        with self.lock:
            if self.state == BreakerState.HALF_OPEN:
                self.half_open_successes += 1
                # Enough successes to close circuit breaker
                if self.half_open_successes >= self.half_open_success_threshold:
                    self.transition_to_closed(
                        reason="half-open success threshold reached"
                    )
            else:
                # keep breaker healthy in CLOSED / OPEN
                self.consecutive_failures = 0

    def record_failure(self) -> None:
        logger.info(
            "circuit_fail name=%s state=%s consec_fail=%d snapshot=%s",
            self.name,
            self.state.value,
            self.consecutive_failures,
            self.snapshot(),
        )
        with self.lock:
            if self.state == BreakerState.HALF_OPEN:
                # any failure during HALF_OPEN trips immediately
                logger.info(
                    "circuit_trip name=%s from=%s reason=%s",
                    self.name,
                    self.state.value,
                    "failure while half-open",
                )
                self.trip_open(reason="failure while half-open")
                return

            self.consecutive_failures += 1

            if (
                self.state == BreakerState.CLOSED
                and self.consecutive_failures >= self.failure_threshold
            ):
                # We were closed, but too many failures, so open up
                logger.info(
                    "circuit_trip name=%s from=%s reason=%s",
                    self.name,
                    self.state.value,
                    "failure threshold reached",
                )
                self.trip_open(reason="failure threshold reached")
            # If already OPEN, nothing more to do.

    def trip_open(self, *, reason: str) -> None:
        self.opened_at = time.monotonic()
        self.consecutive_failures = 0
        self.half_open_successes = 0
        self.transition(BreakerState.OPEN, reason=reason)

    def transition_to_closed(self, *, reason: str) -> None:
        self.opened_at = None
        self.consecutive_failures = 0
        self.half_open_successes = 0
        self.transition(BreakerState.CLOSED, reason=reason)

    def transition(self, new_state: BreakerState, reason: str) -> None:
        previous_state = copy.copy(self.state)
        self.state = new_state
        logger.info(
            "circuit_state_change name=%s from=%s to=%s reason=%s failure_threshold=%d "
            "recovery_timeout=%.3f half_open_success_threshold=%d",
            self.name,
            previous_state.value,
            new_state.value,
            reason,
            self.failure_threshold,
            self.recovery_timeout,
            self.half_open_success_threshold,
        )

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "name": self.name,
                "state": self.state.value,
                "consecutive_failures": self.consecutive_failures,
                "half_open_successes": self.half_open_successes,
                "opened_for_secs": (
                    -1
                    if self.opened_at is None
                    else (time.monotonic() - self.opened_at)
                ),
                "failure_threshold": self.failure_threshold,
                "recovery_timeout": self.recovery_timeout,
                "half_open_success_threshold": self.half_open_success_threshold,
            }

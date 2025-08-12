import time
import logging
import pytest

from app.simple_circuit_breaker import SimpleCircuitBreaker, BreakerState


def test_starts_closed_and_allows_requests():
    cb = SimpleCircuitBreaker("t", failure_threshold=3, recovery_timeout=0.05)
    assert cb.state == BreakerState.CLOSED
    assert cb.allow_request() is True
    snap = cb.snapshot()
    assert snap["state"] == "closed"
    assert snap["consecutive_failures"] == 0


def test_trips_open_after_threshold():
    cb = SimpleCircuitBreaker("t", failure_threshold=2, recovery_timeout=0.05)
    cb.record_failure()
    assert cb.state == BreakerState.CLOSED
    cb.record_failure()
    assert cb.state == BreakerState.OPEN
    assert cb.allow_request() is False  # still cooling


def test_moves_to_half_open_after_cooldown():
    cb = SimpleCircuitBreaker("t", failure_threshold=1, recovery_timeout=0.02)
    cb.record_failure()  # -> OPEN
    assert cb.state == BreakerState.OPEN
    assert cb.allow_request() is False  # not yet
    time.sleep(0.03)  # cooldown elapses
    assert cb.allow_request() is True  # moves to HALF_OPEN and allows a probe
    assert cb.state == BreakerState.HALF_OPEN


def test_half_open_success_threshold_then_close():
    cb = SimpleCircuitBreaker(
        "t", failure_threshold=1, recovery_timeout=0.01, half_open_success_threshold=2
    )
    cb.record_failure()  # -> OPEN
    time.sleep(0.02)  # allow HALF_OPEN
    assert cb.allow_request()  # -> HALF_OPEN
    cb.record_success()  # 1/2
    assert cb.state == BreakerState.HALF_OPEN
    cb.record_success()  # 2/2 threshold -> CLOSED
    assert cb.state == BreakerState.CLOSED
    snap = cb.snapshot()
    assert snap["consecutive_failures"] == 0
    assert snap["half_open_successes"] == 0


def test_half_open_failure_reopens():
    cb = SimpleCircuitBreaker("t", failure_threshold=2, recovery_timeout=0.02)
    cb.record_failure()
    cb.record_failure()  # -> OPEN
    time.sleep(0.03)
    assert cb.allow_request()  # -> HALF_OPEN
    cb.record_failure()  # any failure in HALF_OPEN -> OPEN
    assert cb.state == BreakerState.OPEN

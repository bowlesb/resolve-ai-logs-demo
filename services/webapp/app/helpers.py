"""
Helpers for the Log Distribution Dashboard.

- Loads/stores analyzer ON/OFF state and weights in MongoDB.
- Queries Graylog for recent message counts per analyzer.
- Fetches distributor /health and normalizes circuit breaker info.
- Renders small UI fragments used by the Dash app (e.g., breaker table, analyzer toggle).
"""

from __future__ import annotations

import os
import sys
import logging
from typing import Dict, Tuple, List

import requests
from requests.exceptions import RequestException
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from dash import html

MONGO_URI: str = os.environ.get("MONGO_URI", "mongodb://mongo:27017/")
GRAYLOG_API: str = os.environ.get("GRAYLOG_API", "http://graylog:9000/api").rstrip("/")
GRAYLOG_USER: str = os.environ.get("GRAYLOG_USER", "admin")
GRAYLOG_PASS: str = os.environ.get("GRAYLOG_PASS", "admin")
DISTRIBUTOR_API: str = os.environ.get(
    "DISTRIBUTOR_API", "http://distributor:8000"
).rstrip("/")
WINDOW_SECS: int = int(os.environ.get("WINDOW_SECS", "3"))  # search window for counts
REFRESH_MS: int = int(os.environ.get("REFRESH_MS", "1000"))  # UI refresh interval
ANALYZERS: List[str] = ["analyzer1", "analyzer2", "analyzer3", "analyzer4"]

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    force=True,
)
LOG = logging.getLogger("webapp.helpers")

MONGO_CLIENT = MongoClient(MONGO_URI)
CONTROL_DB = MONGO_CLIENT.control
ANALYZERS_COL = CONTROL_DB.analyzers  # {name: str, active: bool}
WEIGHTS_COL = CONTROL_DB.weights  # {_id: "weights", values: {analyzerN: float}}


def ensure_defaults() -> None:
    """Insert default analyzer ON states and default weights if the collections are empty."""
    try:
        if ANALYZERS_COL.count_documents({}) == 0:
            LOG.info("[INIT] inserting default analyzer states (all ON)")
            ANALYZERS_COL.insert_many(
                [{"name": name, "active": True} for name in ANALYZERS]
            )

        if WEIGHTS_COL.count_documents({"_id": "weights"}) == 0:
            LOG.info("[INIT] inserting default weights 0.4/0.3/0.2/0.1")
            WEIGHTS_COL.insert_one(
                {
                    "_id": "weights",
                    "values": {
                        "analyzer1": 0.4,
                        "analyzer2": 0.3,
                        "analyzer3": 0.2,
                        "analyzer4": 0.1,
                    },
                }
            )
    except PyMongoError as error:
        LOG.info("[MONGO] ensure_defaults failed: %s", error)


ensure_defaults()


def get_states_and_weights() -> Tuple[Dict[str, bool], Dict[str, float]]:
    """Return (states, weights) from Mongo; fall back to safe defaults on error."""
    try:
        states: Dict[str, bool] = {
            doc["name"]: bool(doc.get("active", True))
            for doc in ANALYZERS_COL.find({"name": {"$in": ANALYZERS}})
        }
        weights_doc = WEIGHTS_COL.find_one({"_id": "weights"}) or {"values": {}}
        weights: Dict[str, float] = {
            k: float(v) for k, v in (weights_doc.get("values") or {}).items()
        }

        # Ensure every analyzer has a value
        for analyzer_name in ANALYZERS:
            states.setdefault(analyzer_name, True)
            weights.setdefault(analyzer_name, 0.25)

        LOG.info("[MONGO] loaded states=%s weights=%s", states, weights)
        return states, weights
    except PyMongoError as error:
        LOG.info("[MONGO] get_states_and_weights failed: %s", error)
        return ({name: True for name in ANALYZERS}, {name: 0.25 for name in ANALYZERS})


def graylog_count(analyzer_prefix: str) -> int:
    """Count messages in last WINDOW_SECS where message contains '<analyzer_prefix>:'."""
    try:
        response = requests.get(
            f"{GRAYLOG_API}/search/universal/relative",
            params={"query": f'message:"{analyzer_prefix}:"', "range": WINDOW_SECS},
            auth=(GRAYLOG_USER, GRAYLOG_PASS),
            headers={"X-Requested-By": "webapp", "Accept": "application/json"},
            timeout=5,
        )
        if response.ok:
            data = response.json()  # ValueError if not JSON
            return int((data or {}).get("total_results", 0))
        LOG.info(
            "[GRAYLOG] %s returned %s: %s",
            response.url,
            response.status_code,
            response.text[:200],
        )
        return 0
    except RequestException as error:
        LOG.info("[GRAYLOG] request error for %s: %s", analyzer_prefix, error)
        return 0
    except ValueError as error:
        LOG.info("[GRAYLOG] bad JSON for %s: %s", analyzer_prefix, error)
        return 0


def fetch_breakers() -> Dict[str, Dict[str, int | float | str]]:
    """
    Fetch /health and normalize breaker info to:
    {name: {"state": str, "failures": int, "reopen_in": int}}
    """
    try:
        response = requests.get(f"{DISTRIBUTOR_API}/health", timeout=3)
        response.raise_for_status()
        raw_breaker_map = (response.json() or {}).get("breakers") or {}
        LOG.info("[HEALTH] breakers raw: %s", raw_breaker_map)

        normalized_breakers_data: Dict[str, Dict[str, int | float | str]] = {}
        for analyzer_name in ANALYZERS:
            # Try exact match; if not present, allow partial key containing the analyzer name.
            breaker_info = raw_breaker_map.get(analyzer_name) or next(
                (
                    value
                    for key, value in raw_breaker_map.items()
                    if analyzer_name in key
                ),
                None,
            )
            if not breaker_info:
                continue

            state_str = breaker_info.get("state", "unknown")
            consecutive_failures = int(
                breaker_info.get(
                    "consecutive_failures", breaker_info.get("failures", 0)
                )
            )
            opened_for_seconds = float(breaker_info.get("opened_for_secs", 0.0))
            timeout_seconds = float(breaker_info.get("recovery_timeout", 0.0))
            seconds_until_reopen = (
                int(max(0.0, timeout_seconds - opened_for_seconds))
                if state_str == "open" and timeout_seconds > 0
                else 0
            )

            normalized_breakers_data[analyzer_name] = {
                "state": state_str,
                "failures": consecutive_failures,
                "reopen_in": seconds_until_reopen,
            }
        return normalized_breakers_data
    except RequestException as error:
        LOG.info("[HEALTH] request error: %s", error)
        return {}
    except ValueError as error:
        LOG.info("[HEALTH] bad JSON: %s", error)
        return {}


def render_breaker_table(
    breakers_by_analyzer: Dict[str, Dict[str, int | float | str]]
) -> html.Table:
    """Render a small HTML table that displays circuit breaker status for each analyzer."""
    header_cell_style = {"textAlign": "center", "padding": "6px"}
    cell_style = {"textAlign": "center", "padding": "6px"}

    header_row = html.Tr(
        [
            html.Th("Analyzer", style=header_cell_style),
            html.Th("State", style=header_cell_style),
            html.Th("Failures", style=header_cell_style),
            html.Th("Reopen In (s)", style=header_cell_style),
        ]
    )

    data_rows = []
    for analyzer_name in ANALYZERS:
        breaker_info = breakers_by_analyzer.get(
            analyzer_name, {"state": "unknown", "failures": 0, "reopen_in": 0}
        )
        data_rows.append(
            html.Tr(
                [
                    html.Td(analyzer_name, style=cell_style),
                    html.Td(
                        str(breaker_info.get("state", "unknown")), style=cell_style
                    ),
                    html.Td(
                        str(int(breaker_info.get("failures", 0))), style=cell_style
                    ),
                    html.Td(
                        str(int(breaker_info.get("reopen_in", 0))), style=cell_style
                    ),
                ]
            )
        )

    return html.Table(
        [header_row] + data_rows,
        style={"width": "100%", "borderCollapse": "collapse", "fontSize": "14px"},
    )


def analyzer_state_control(analyzer_name: str, is_active: bool) -> html.Div:
    """Render a labeled ON/OFF radio control for a single analyzer."""
    return html.Div(
        style={
            "display": "flex",
            "alignItems": "center",
            "justifyContent": "space-between",
            "margin": "6px 0",
        },
        children=[
            html.Div(analyzer_name, style={"fontWeight": "600"}),
            # Dash RadioItems values are lowercase strings "on"/"off" for consistency in callbacks
            html.Div(
                [
                    html.Label(
                        "ON",
                        htmlFor=f"state-{analyzer_name}",
                        style={"marginRight": "6px"},
                    ),
                ]
            ),
            # The inline RadioItems itself
            html.Div(
                children=[],
                style={"display": "none"},  # label is shown above; actual control below
            ),
        ],
    )

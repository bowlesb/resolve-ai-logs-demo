import os
from typing import Dict, Optional
import pymongo


def parse_weights(env: Optional[str]) -> Dict[str, float]:
    """
    Parse weights from a comma-separated string of 'name:weight' pairs.

    E.g., "analyzer1:0.4,analyzer2:0.3" -> {"analyzer1": 0.4, "analyzer2": 0.3}
    """
    out: Dict[str, float] = {}
    if not env:
        return out
    for pair in env.split(","):
        pair = pair.strip()
        assert ":" in pair, f"Broken Pair detected: {pair}"
        name, weight = pair.split(":", 1)
        out[name.strip()] = float(weight)
    return out


MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017/")
MONGO_CLIENT = pymongo.MongoClient(MONGO_URI)
CONTROL_DB = MONGO_CLIENT.control
WEIGHTS_COL = CONTROL_DB.weights

# Analyzer host, port come like analyzer1:50051,analyzer2:50051,...
ANALYZERS_ENV: str = os.environ.get("ANALYZERS", "")

# Only allow this long to send to analyzer
ANALYZER_TIMEOUT_MS: int = int(os.environ.get("ANALYZER_TIMEOUT_MS", "200"))

# Default weights come in like: analyzer1:0.4,analyzer2:0.3,...
DEFAULT_WEIGHTS_ENV: str = os.environ.get("DEFAULT_WEIGHTS", "")

# Poll Mongo for weight updates on this frequency (seconds)
WEIGHT_POLL_SECS: int = int(os.environ.get("WEIGHT_POLL_SECS", "5"))

# Circuit breaker tuning
CB_FAILURE_THRESHOLD: int = int(os.environ.get("CB_FAILURE_THRESHOLD", "3"))
CB_RECOVERY_TIMEOUT_SEC: float = float(os.environ.get("CB_RECOVERY_TIMEOUT_SEC", "20"))
CB_HALF_OPEN_SUCC_THRESHOLD: int = int(
    os.environ.get("CB_HALF_OPEN_SUCC_THRESHOLD", "50")
)

# Default weights mapping, parsed from the environment variable
DEFAULT_ANALYZER_TO_WEIGHTS: Dict[str, float] = parse_weights(DEFAULT_WEIGHTS_ENV)


__all__ = [
    "parse_weights",
    "MONGO_URI",
    "ANALYZERS_ENV",
    "ANALYZER_TIMEOUT_MS",
    "DEFAULT_WEIGHTS_ENV",
    "WEIGHT_POLL_SECS",
    "CB_FAILURE_THRESHOLD",
    "CB_RECOVERY_TIMEOUT_SEC",
    "CB_HALF_OPEN_SUCC_THRESHOLD",
    "DEFAULT_ANALYZER_TO_WEIGHTS",
    "WEIGHTS_COL",
]

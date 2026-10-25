"""Runtime identity supplied by the container environment."""

import math
import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Settings:
    pod_id: str
    datacenter: str
    failure_rate: float = 0.0
    latency_min_ms: float = 0.0
    latency_max_ms: float = 0.0

    def __post_init__(self) -> None:
        if not self.pod_id.strip() or not self.datacenter.strip():
            raise ValueError("POD_ID and DATACENTER must not be empty")
        values = (self.failure_rate, self.latency_min_ms, self.latency_max_ms)
        if not all(math.isfinite(value) for value in values):
            raise ValueError("Failure rate and latency settings must be finite")
        if not 0.0 <= self.failure_rate <= 1.0:
            raise ValueError("FAILURE_RATE must be between 0 and 1")
        if not 0.0 <= self.latency_min_ms <= self.latency_max_ms:
            raise ValueError("Require 0 <= LATENCY_MIN_MS <= LATENCY_MAX_MS")

    @classmethod
    def from_environment(cls) -> "Settings":
        return cls(
            pod_id=os.getenv("POD_ID", "local"),
            datacenter=os.getenv("DATACENTER", "local"),
            failure_rate=float(os.getenv("FAILURE_RATE", "0")),
            latency_min_ms=float(os.getenv("LATENCY_MIN_MS", "0")),
            latency_max_ms=float(os.getenv("LATENCY_MAX_MS", "0")),
        )

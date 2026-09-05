"""Simple statistical anomaly detection (z-score) on metric series (P2-08)."""

from __future__ import annotations

import statistics
from dataclasses import dataclass


@dataclass
class Anomaly:
    index: int
    value: float
    zscore: float


def zscore(values: list[float]) -> list[float]:
    """Return the z-score of each value; all zeros when stddev is 0."""
    if len(values) < 2:
        return [0.0] * len(values)
    mean = statistics.mean(values)
    stdev = statistics.pstdev(values)
    if stdev == 0:
        return [0.0] * len(values)
    return [(v - mean) / stdev for v in values]


def detect_anomalies(
    values: list[float],
    threshold: float = 3.0,
    min_points: int = 3,
) -> list[Anomaly]:
    """Return points whose z-score exceeds the threshold."""
    if len(values) < min_points:
        return []
    scores = zscore(values)
    return [
        Anomaly(index=i, value=v, zscore=z)
        for i, (v, z) in enumerate(zip(values, scores, strict=True))
        if abs(z) > threshold
    ]

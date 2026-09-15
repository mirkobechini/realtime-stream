"""Modello dati canonico dell'evento real-time.

Definisce la struttura unica che ogni evento grezzo proveniente da un broker
viene normalizzato in. I campi canonici sono: event_type, payload, source,
timestamp, sequence.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class Event:
    """Evento normalizzato con campi canonici.

    Attributes:
        event_type: tipo di evento (es. "metric", "gps", "ticker", "status").
        payload: dati specifici dell'evento (dict arbitrario).
        source: identificativo della sorgente (es. "redis", "mqtt", "kafka").
        timestamp: epoch secondi (float) di quando l'evento è stato generato.
        sequence: numero di sequenza monotono crescente per ordinamento/replay.
        id: UUID univoco dell'evento.
    """

    event_type: str
    payload: dict[str, Any]
    source: str
    timestamp: float
    sequence: int
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    @classmethod
    def now(
        cls,
        event_type: str,
        payload: dict[str, Any],
        source: str,
        sequence: int,
    ) -> "Event":
        """Crea un evento con timestamp corrente."""
        return cls(
            event_type=event_type,
            payload=payload,
            source=source,
            timestamp=time.time(),
            sequence=sequence,
        )

    def to_stream(self) -> dict[str, str]:
        """Serializza l'evento in un dict di stringhe per Redis Streams.

        Redis Streams richiede che i campi siano stringhe, quindi il payload
        viene serializzato in JSON.
        """
        import json

        return {
            "id": self.id,
            "event_type": self.event_type,
            "payload": json.dumps(self.payload),
            "source": self.source,
            "timestamp": str(self.timestamp),
            "sequence": str(self.sequence),
        }

    @classmethod
    def from_stream(cls, data: dict[str, str]) -> "Event":
        """Deserializza un evento da un dict di stringhe Redis Streams."""
        import json

        return cls(
            id=data["id"],
            event_type=data["event_type"],
            payload=json.loads(data["payload"]),
            source=data["source"],
            timestamp=float(data["timestamp"]),
            sequence=int(data["sequence"]),
        )
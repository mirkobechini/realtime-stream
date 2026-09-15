"""Schema Pydantic per la validazione dell'ingest degli eventi.

Questo schema valida gli eventi grezzi in ingresso dal broker prima che
vengano normalizzati nel modello canonico `Event`.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class RawEvent(BaseModel):
    """Evento grezzo in ingresso dal broker.

    Attributes:
        event_type: tipo di evento, obbligatorio.
        payload: dati specifici dell'evento, obbligatorio.
        source: identificativo della sorgente, obbligatorio.
        timestamp: epoch secondi opzionale (default: ora corrente).
        sequence: numero di sequenza opzionale (default: 0).
    """

    model_config = ConfigDict(extra="forbid")

    event_type: str = Field(..., min_length=1, max_length=64)
    payload: dict[str, Any]
    source: str = Field(..., min_length=1, max_length=64)
    timestamp: float | None = Field(default=None, ge=0)
    sequence: int | None = Field(default=None, ge=0)
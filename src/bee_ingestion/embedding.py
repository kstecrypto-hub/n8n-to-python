from __future__ import annotations

from hashlib import sha256
import time
from typing import Any, Callable

import httpx

from src.bee_ingestion.settings import settings


class Embedder:
    def embed(
        self,
        texts: list[str],
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> list[list[float]]:
        if settings.embedding_provider == "dummy":
            return [self._dummy_embed(text) for text in texts]
        if settings.embedding_provider in {"openai_compatible", "openai"}:
            return self._openai_compatible_embed(texts, progress_callback=progress_callback)
        raise NotImplementedError(f"Unsupported embedding provider: {settings.embedding_provider}")

    @staticmethod
    def _openai_compatible_embed(
        texts: list[str],
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> list[list[float]]:
        # Batch calls so a large ingest does not turn into one HTTP request per
        # chunk.
        if not settings.embedding_api_key:
            raise ValueError("EMBEDDING_API_KEY is required for the openai_compatible embedding provider.")
        results: list[list[float]] = []
        batch_size = max(1, settings.embedding_batch_size)
        timeout = settings.embedding_timeout_seconds
        headers = {
            "Authorization": f"Bearer {settings.embedding_api_key}",
            "Content-Type": "application/json",
        }
        retryable_statuses = {408, 409, 429, 500, 502, 503, 504}
        max_retries = max(0, settings.embedding_max_retries)
        backoff = max(0.25, settings.embedding_retry_backoff_seconds)
        with httpx.Client(timeout=timeout) as client:
            for start in range(0, len(texts), batch_size):
                batch = texts[start : start + batch_size]
                last_error: Exception | None = None
                for attempt in range(max_retries + 1):
                    try:
                        response = client.post(
                            f"{settings.embedding_base_url.rstrip('/')}/embeddings",
                            headers=headers,
                            json={"model": settings.embedding_model, "input": batch},
                        )
                        if response.status_code in retryable_statuses and attempt < max_retries:
                            time.sleep(backoff * (2 ** attempt))
                            continue
                        response.raise_for_status()
                        payload = response.json()
                        break
                    except httpx.HTTPError as exc:
                        last_error = exc
                        if attempt >= max_retries:
                            raise
                        time.sleep(backoff * (2 ** attempt))
                else:  # pragma: no cover - defensive only
                    if last_error is not None:
                        raise last_error
                    raise RuntimeError("Embedding request failed without a response payload")
                vectors = _validated_embedding_vectors(payload, expected_count=len(batch))
                results.extend(vectors)
                if progress_callback is not None:
                    progress_callback(
                        {
                            "completed": len(results),
                            "total": len(texts),
                            "batch_size": len(batch),
                        }
                    )
        return results

    @staticmethod
    def _dummy_embed(text: str, dimensions: int = 16) -> list[float]:
        digest = sha256(text.encode("utf-8")).digest()
        values = []
        for idx in range(dimensions):
            byte = digest[idx % len(digest)]
            values.append((byte / 255.0) * 2 - 1)
        return values


def _validated_embedding_vectors(payload: dict, *, expected_count: int) -> list[list[float]]:
    rows = payload.get("data")
    if not isinstance(rows, list) or len(rows) != expected_count:
        raise RuntimeError("Embedding response did not return the expected number of vectors")
    ordered: list[list[float] | None] = [None] * expected_count
    for position, row in enumerate(rows):
        if not isinstance(row, dict):
            raise RuntimeError("Embedding response contained a non-object row")
        raw_index = row.get("index", position)
        if not isinstance(raw_index, int) or raw_index < 0 or raw_index >= expected_count:
            raise RuntimeError("Embedding response contained an invalid vector index")
        if ordered[raw_index] is not None:
            raise RuntimeError("Embedding response contained duplicate vector indexes")
        embedding = row.get("embedding")
        if not isinstance(embedding, list) or not embedding:
            raise RuntimeError("Embedding response contained an invalid vector payload")
        vector: list[float] = []
        for value in embedding:
            if not isinstance(value, (int, float)):
                raise RuntimeError("Embedding response contained a non-numeric vector value")
            vector.append(float(value))
        ordered[raw_index] = vector
    if any(vector is None for vector in ordered):
        raise RuntimeError("Embedding response was missing one or more vectors")
    return [vector for vector in ordered if vector is not None]

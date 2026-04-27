"""Application factory for the serving HTTP surface."""

from __future__ import annotations

from fastapi import FastAPI


def create_app() -> FastAPI:
    return FastAPI(title="Bee Ingestion API")

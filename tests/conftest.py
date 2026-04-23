import pytest

from src.bee_ingestion.settings import settings


@pytest.fixture(autouse=True)
def stable_test_settings():
    original = {
        "embedding_provider": settings.embedding_provider,
        "embedding_model": settings.embedding_model,
        "kg_extraction_provider": settings.kg_extraction_provider,
        "kg_model": settings.kg_model,
        "kg_base_url": settings.kg_base_url,
        "kg_api_key": settings.kg_api_key,
        "agent_provider": settings.agent_provider,
        "agent_model": settings.agent_model,
        "agent_base_url": settings.agent_base_url,
        "agent_api_key": settings.agent_api_key,
    }
    settings.embedding_provider = "dummy"
    settings.kg_extraction_provider = "heuristic"
    settings.kg_model = "gpt-5-mini"
    settings.kg_base_url = "https://api.openai.com/v1"
    settings.kg_api_key = None
    settings.agent_provider = "disabled"
    settings.agent_model = "gpt-5-mini"
    settings.agent_base_url = "https://api.openai.com/v1"
    settings.agent_api_key = None
    try:
        yield
    finally:
        for key, value in original.items():
            setattr(settings, key, value)

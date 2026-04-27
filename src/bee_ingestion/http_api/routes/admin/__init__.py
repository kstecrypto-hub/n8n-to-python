"""Admin route composition.

This package owns only HTTP route composition for the operator surface. It does
not own admin workflow behavior.
"""

from fastapi import APIRouter

from src.bee_ingestion.http_api.routes.admin.corpus_routes import router as corpus_router
from src.bee_ingestion.http_api.routes.admin.health_routes import router as health_router
from src.bee_ingestion.http_api.routes.admin.ingestion_routes import router as ingestion_router
from src.bee_ingestion.http_api.routes.admin.inspection_routes import router as inspection_router
from src.bee_ingestion.http_api.routes.admin.metrics_routes import router as metrics_router
from src.bee_ingestion.http_api.routes.admin.review_routes import router as review_router
from src.bee_ingestion.http_api.routes.admin.runtime_config_routes import router as runtime_config_router
from src.bee_ingestion.http_api.routes.admin.user_routes import router as user_router

router = APIRouter()
router.include_router(health_router)
router.include_router(inspection_router)
router.include_router(user_router)
router.include_router(corpus_router)
router.include_router(review_router)
router.include_router(runtime_config_router)
router.include_router(ingestion_router)
router.include_router(metrics_router)

__all__ = ["router"]


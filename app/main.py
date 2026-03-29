"""FastAPI application entry point."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.core.logging_config import setup_logging
from app.api.fb_routes import router as fb_router
from app.api.keitaro_routes import router as kt_router
from app.api.analytics_routes import router as analytics_router
from app.models.schemas import HealthResponse

setup_logging()

app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="FB + Keitaro analytics via file exports and Keitaro Admin API",
    root_path=settings.root_path,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins.split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(fb_router)
app.include_router(kt_router)
app.include_router(analytics_router)


@app.get("/", tags=["system"])
def root():
    return {"app": settings.app_name, "version": settings.app_version}


@app.get("/health", response_model=HealthResponse, tags=["system"])
def health():
    return HealthResponse(status="ok", version=settings.app_version)

"""Health and readiness endpoints."""

from fastapi import APIRouter, status


router = APIRouter()


@router.get("/health", status_code=status.HTTP_200_OK)
async def healthcheck() -> dict[str, str]:
    """Return a basic health indicator."""

    return {"status": "ok"}


"""Dashboard endpoint for visualizing feed processing and article filtering."""

from fastapi import APIRouter
from fastapi.responses import FileResponse
import os

router = APIRouter(
    tags=["dashboard"],
    responses={404: {"description": "Not found"}},
)


@router.get("/")
async def get_dashboard():
    """
    Serves the news processing dashboard.
    Shows feeds being processed and articles passing/failing filters.
    """
    dashboard_path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "static",
        "dashboard.html"
    )
    if not os.path.exists(dashboard_path):
        return {"error": f"Dashboard not found at {dashboard_path}"}
    return FileResponse(dashboard_path, media_type="text/html")

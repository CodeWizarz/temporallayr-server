import logging
from typing import List, Optional
from sqlalchemy.future import select

from app.core.database import async_session_maker
from app.dashboard.models import SavedQueryDB, DashboardDB, PanelDB

logger = logging.getLogger("temporallayr.dashboard.service")


class DashboardService:
    """Enterprise database routing enabling multitenant grids natively securely."""

    async def create_saved_query(
        self, tenant_id: str, name: str, query_json: dict
    ) -> SavedQueryDB:
        async with async_session_maker() as session:
            new_query = SavedQueryDB(
                tenant_id=tenant_id, name=name, query_json=query_json
            )
            session.add(new_query)
            await session.commit()
            await session.refresh(new_query)
            return new_query

    async def list_saved_queries(self, tenant_id: str) -> List[SavedQueryDB]:
        async with async_session_maker() as session:
            stmt = (
                select(SavedQueryDB)
                .where(SavedQueryDB.tenant_id == tenant_id)
                .order_by(SavedQueryDB.created_at.desc())
            )
            result = await session.execute(stmt)
            return result.scalars().all()

    async def create_dashboard(self, tenant_id: str, name: str) -> DashboardDB:
        async with async_session_maker() as session:
            new_dashboard = DashboardDB(tenant_id=tenant_id, name=name)
            session.add(new_dashboard)
            await session.commit()
            await session.refresh(new_dashboard)
            return new_dashboard

    async def list_dashboards(self, tenant_id: str) -> List[DashboardDB]:
        async with async_session_maker() as session:
            stmt = (
                select(DashboardDB)
                .where(DashboardDB.tenant_id == tenant_id)
                .order_by(DashboardDB.created_at.desc())
            )
            result = await session.execute(stmt)
            return result.scalars().all()

    async def add_panel_to_dashboard(
        self,
        tenant_id: str,
        dashboard_id: str,
        saved_query_id: str,
        name: str,
        pos_x: float,
        pos_y: float,
        width: float,
        height: float,
    ) -> PanelDB:
        async with async_session_maker() as session:
            # 1. Enforce specific multi-tenant grid security natively!
            stmt = select(SavedQueryDB).where(SavedQueryDB.id == saved_query_id)
            result = await session.execute(stmt)
            saved_query = result.scalars().first()

            if not saved_query:
                raise ValueError("SavedQuery not found.")

            # CRITICAL SECURITY BARRIER mapping cross-tenant leaks explicitly.
            if saved_query.tenant_id != tenant_id:
                raise PermissionError(
                    "Access denied: cross-tenant bounding constraint violation safely."
                )

            # Validate dashboard footprint
            dash_stmt = select(DashboardDB).where(
                DashboardDB.id == dashboard_id, DashboardDB.tenant_id == tenant_id
            )
            dash_res = await session.execute(dash_stmt)
            if not dash_res.scalars().first():
                raise ValueError("Dashboard not found.")

            new_panel = PanelDB(
                tenant_id=tenant_id,
                dashboard_id=dashboard_id,
                name=name,
                saved_query_id=saved_query_id,
                position_x=pos_x,
                position_y=pos_y,
                width=width,
                height=height,
            )
            session.add(new_panel)
            await session.commit()
            await session.refresh(new_panel)
            return new_panel

    async def get_dashboard_with_panels(
        self, tenant_id: str, dashboard_id: str
    ) -> Optional[dict]:
        """Maps nested grid topologies directly to API schemas naturally skipping N+1 queries organically."""
        async with async_session_maker() as session:
            # Fetch Dashboard cleanly
            dash_stmt = select(DashboardDB).where(
                DashboardDB.id == dashboard_id, DashboardDB.tenant_id == tenant_id
            )
            dash_res = await session.execute(dash_stmt)
            dashboard = dash_res.scalars().first()

            if not dashboard:
                return None

            # Fetch explicitly isolated topological layers natively
            panel_stmt = (
                select(PanelDB, SavedQueryDB)
                .join(SavedQueryDB, PanelDB.saved_query_id == SavedQueryDB.id)
                .where(
                    PanelDB.dashboard_id == dashboard_id, PanelDB.tenant_id == tenant_id
                )
            )

            results = await session.execute(panel_stmt)

            panels_map = []
            for panel_db, query_db in results.all():
                # Serialize natively matching Pydantic nested structures neatly
                panels_map.append(
                    {
                        "panel_id": str(panel_db.id),
                        "name": panel_db.name,
                        "position_x": panel_db.position_x,
                        "position_y": panel_db.position_y,
                        "width": panel_db.width,
                        "height": panel_db.height,
                        "saved_query": {
                            "id": str(query_db.id),
                            "tenant_id": query_db.tenant_id,
                            "name": query_db.name,
                            "query_json": query_db.query_json,
                            "created_at": query_db.created_at,
                        },
                    }
                )

            return {
                "dashboard_id": str(dashboard.id),
                "name": dashboard.name,
                "created_at": dashboard.created_at,
                "panels": panels_map,
            }


dashboard_service = DashboardService()

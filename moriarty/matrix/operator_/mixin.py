from __future__ import annotations

from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from moriarty.matrix.operator_.orm import AutoscalerORM, EndpointORM


class EndpointMixin:
    session: AsyncSession

    async def get_endpoint_orm(self, endpoint_name: str) -> EndpointORM | None:
        return (
            await self.session.execute(
                select(EndpointORM).where(EndpointORM.endpoint_name == endpoint_name)
            )
        ).scalar_one_or_none()

    async def get_avaliable_endpoints(self) -> list[str]:
        endpoint_names = (
            (
                await self.session.execute(
                    select(EndpointORM.endpoint_name).where(
                        EndpointORM.replicas > 0,
                    )
                )
            )
            .scalars()
            .all()
        )
        return endpoint_names


class AutoscaleMixin:
    async def get_autoscaler_orm(self, endpoint_name: str) -> AutoscalerORM | None:
        return (
            await self.session.execute(
                select(AutoscalerORM).where(AutoscalerORM.endpoint_name == endpoint_name)
            )
        ).scalar_one_or_none()

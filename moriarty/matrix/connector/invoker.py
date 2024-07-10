import os
import uuid

from fastapi import Depends

from moriarty.envs import get_bridge_name
from moriarty.log import logger
from moriarty.matrix.connector.params import InvokeParams, InvokeResponse
from moriarty.matrix.job_manager.bridge_wrapper import BridgeWrapper, get_bridge_wrapper
from moriarty.matrix.job_manager.params import InferenceJob


def get_invoker(
    bridge_name: str = Depends(get_bridge_name),
    bridge_wrapper: BridgeWrapper = Depends(get_bridge_wrapper),
):
    return Invoker(bridge_name=bridge_name, bridge_wrapper=bridge_wrapper)


class Invoker:
    def __init__(
        self,
        bridge_name: str,
        bridge_wrapper: BridgeWrapper,
    ):
        self.bridge_name = bridge_name
        self.bridge_wrapper = bridge_wrapper

    async def invoke(self, params: InvokeParams) -> InvokeResponse:
        inference_id = params.inference_id or uuid.uuid4().hex
        logger.debug(f"Invoke inference: {inference_id} with params: {params}")
        await self.bridge_wrapper.enqueue_job(
            bridge=self.bridge_name,
            endpoint_name=params.endpoint_name,
            job=InferenceJob(
                inference_id=inference_id,
                payload={
                    **params.invoke_params,
                    "inference_id": inference_id,  # Patch inference_id in payload
                },
                metadata=params.model_dump_metadata(),
            ),
            priority=params.priority,
        )
        return InvokeResponse(inference_id=inference_id)

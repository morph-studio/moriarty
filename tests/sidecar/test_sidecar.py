import asyncio
import os

from moriarty.sidecar.producer import JobProducer


async def send_stop_sig(pid, wait=2):
    await asyncio.sleep(wait)
    os.kill(pid, 15)


async def test_sidecar(async_redis_client, endpoint_name, inference_consumer):
    producer = JobProducer(redis_client=async_redis_client)

    invoke_params = {"inference_id": "hello world"}
    pid = os.getpid()
    asyncio.create_task(send_stop_sig(pid))
    await producer.invoke(endpoint_name, invoke_params)
    await inference_consumer.run_forever()
    assert inference_consumer._last_response == invoke_params

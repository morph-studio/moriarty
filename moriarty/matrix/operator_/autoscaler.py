import asyncio
import contextlib
import signal

from kubernetes_asyncio import client
from sqlalchemy.ext.asyncio import AsyncSession

from moriarty.log import logger
from moriarty.matrix.operator_.config import Config
from moriarty.matrix.operator_.dbutils import open_db_session
from moriarty.matrix.operator_.tools import load_kube_config


async def event_wait(evt, timeout):
    # suppress TimeoutError because we'll return False in case of timeout
    with contextlib.suppress(asyncio.TimeoutError):
        await asyncio.wait_for(evt.wait(), timeout)
    return evt.is_set()


class DaemonMixin:
    def __init__(self) -> None:
        self._stop_event = asyncio.Event()
        self._task: None | asyncio.Task = None

    async def initialize(self):
        pass

    async def run(self):
        pass

    async def cleanup(self):
        pass

    async def _start(self):
        if self._task is not None:
            raise RuntimeError("Already running")

        self._stop_event.clear()
        self._task = asyncio.create_task(self._task_wrapper())

    async def _stop(self):
        self._stop_event.set()
        if self._task is not None:
            logger.info("Waiting for task to finish...")
            await self._task

    async def _task_wrapper(self):
        try:
            await self.initialize()
        except Exception as e:
            # Unexpected error, stop the autoscaler
            logger.exception(e)
            exit(1)

        while not await event_wait(self._stop_event, 0.1):
            if self._stop_event.is_set():
                break
            try:
                await self.run()
            except Exception as e:
                logger.exception(e)

        await self.cleanup()

    async def run_forever(self, stop_signals: list = [signal.SIGINT, signal.SIGTERM]):
        loop = asyncio.get_event_loop()

        stop_event = asyncio.Event()

        async def _stop():
            logger.debug("Signal received")
            stop_event.set()

        for sig in stop_signals:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(_stop()))

        logger.info(f"Starting task...")
        await self._start()
        logger.info(f"Consumer started, waiting for signals {stop_signals}...")
        await stop_event.wait()

        logger.info(f"Terminating task...")
        await self._stop()
        logger.info(f"Autoscaler terminated")


class KubeAutoscaler(DaemonMixin):
    def __init__(self, config: Config) -> None:
        super().__init__()
        self.config = config

    async def initialize(self):
        await load_kube_config()

    async def run(self):
        async with open_db_session(self.config) as session:
            await self._scan_and_update(session)

    async def _scan_and_update(self, session: AsyncSession) -> None:
        pass

    async def cleanup(self):
        pass

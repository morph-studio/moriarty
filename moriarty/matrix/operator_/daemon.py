import asyncio
import contextlib
import signal

import redis.asyncio as redis
from sqlalchemy.ext.asyncio import AsyncSession

from moriarty.log import logger
from moriarty.matrix.envs import get_bridge_name
from moriarty.matrix.job_manager.bridge_wrapper import (
    get_bridge_manager,
    get_bridge_wrapper,
)
from moriarty.matrix.operator_.autoscaler import AutoscalerManager
from moriarty.matrix.operator_.config import Config
from moriarty.matrix.operator_.dbutils import open_db_session
from moriarty.matrix.operator_.operator_ import Bridger
from moriarty.matrix.operator_.rds import open_redis_client
from moriarty.matrix.operator_.spawner.impl.k8s import KubeSpawner
from moriarty.matrix.operator_.spawner.manager import (
    SpawnerManager,
    get_spawner,
    get_spawner_manager,
    get_spawner_name,
)
from moriarty.matrix.operator_.spawner.plugin import Spawner
from moriarty.matrix.operator_.tools import load_kube_config


async def event_wait(evt, timeout):
    # suppress TimeoutError because we'll return False in case of timeout
    with contextlib.suppress(asyncio.TimeoutError):
        await asyncio.wait_for(evt.wait(), timeout)
    return evt.is_set()


class DaemonMixin:
    interval = 0.1

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

        await self.run()
        while not await event_wait(self._stop_event, self.interval):
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

        logger.info(f"Starting daemon...")
        await self._start()
        logger.info(f"Daemon started, waiting for signals {stop_signals}...")
        await stop_event.wait()

        logger.info(f"Terminating daemon...")
        await self._stop()
        logger.info(f"Daemon terminated")


class KubeAutoscalerDaemon(DaemonMixin):
    interval = 60

    def __init__(self, config: Config) -> None:
        super().__init__()

        self.spawner: KubeSpawner = None
        self.spawner_manager = SpawnerManager()
        self.config = config

    async def initialize(self):
        self.spawner = self.spawner_manager.init(KubeSpawner.register_name)
        await self.spawner.prepare()

    async def run(self):
        logger.info(f"Triggering autoscaler...")
        async with open_redis_client(self.config) as redis_client:
            async with open_db_session(self.config) as session:
                await self._scan_and_update(session, redis_client)

    async def _scan_and_update(
        self, session: AsyncSession, redis_client: redis.Redis | redis.RedisCluster
    ) -> None:
        manager = AutoscalerManager(
            redis_client=redis_client, session=session, spawner=self.spawner
        )
        await manager.scan_and_update()


class BridgeDaemon(DaemonMixin):
    interval = 3

    def __init__(self, config: Config) -> None:
        super().__init__()
        self.bridge_name = get_bridge_name()
        self.bridge_wrapper = get_bridge_wrapper(bridge_manager=get_bridge_manager())
        self.config = config
        self.spawner: Spawner = None

    async def initialize(self):
        self.spawner = await get_spawner(
            spawner_manager=get_spawner_manager(),
            spawner_name=get_spawner_name(),
        )

    async def run(self):
        logger.debug(f"Triggering bridge...")

        async with open_redis_client(self.config) as redis_client:
            async with open_db_session(self.config) as session:
                bridger = Bridger(
                    spawner=self.spawner,
                    bridge_name=self.bridge_name,
                    bridge_wrapper=self.bridge_wrapper,
                    redis_client=redis_client,
                    session=session,
                )
                await bridger.bridge_all()

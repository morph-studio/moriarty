from kubernetes_asyncio import config

from moriarty.log import logger

_config_loaded = False


async def load_kube_config() -> None:
    global _config_loaded
    if _config_loaded:
        return
    try:
        logger.debug("Try to load in-cluster config")
        config.load_incluster_config()
    except config.config_exception.ConfigException:
        logger.info("No in-cluster config, using default config location")
        await config.load_kube_config()
    _config_loaded = True

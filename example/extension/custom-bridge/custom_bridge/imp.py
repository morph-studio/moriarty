from moriarty.matrix.job_manager.bridge.plugin import QueueBridge, hookimpl


class CustomBridge(QueueBridge):
    register_name = "example"


@hookimpl
def register(manager):
    manager.register(CustomBridge)

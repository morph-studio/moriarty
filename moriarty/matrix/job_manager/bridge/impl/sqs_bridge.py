from moriarty.matrix.job_manager.bridge.plugin import QueueBridge, hookimpl


class SQSBridge(QueueBridge):
    register_name = "sqs"


@hookimpl
def register(manager):
    manager.register(SQSBridge)

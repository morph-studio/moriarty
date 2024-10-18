from moriarty.matrix.job_manager.bridge.manager import BridgeManager


def test_bridge_manager():
    registed = ["sqs", "pubsub"]
    manager = BridgeManager()

    assert sorted(registed) == sorted(manager.registed_cls.keys())

    # sqs_bridge = manager.init("sqs")
    # assert sqs_bridge == manager.init("sqs")

    pubsub_bridge = manager.init("pubsub", "morgana-sm-dev", "storage-transfer-dev-436409")
    assert pubsub_bridge == manager.init("pubsub", "morgana-sm-dev", "storage-transfer-dev-436409")

from moriarty.matrix.job_manager.bridge.manager import BridgeManager


def test_bridge_manager():
    registed = ["sqs"]
    manager = BridgeManager()

    assert sorted(registed) == sorted(manager.registed_cls.keys())

    sqs_bridge = manager.init("sqs")
    assert sqs_bridge == manager.init("sqs")

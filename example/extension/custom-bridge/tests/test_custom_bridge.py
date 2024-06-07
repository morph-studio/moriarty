from custom_bridge.imp import CustomBridge

from moriarty.matrix.job_manager.bridge.manager import BridgeManager


def test_registed():
    manager = BridgeManager()

    assert CustomBridge.register_name in manager.registed_cls.keys()

from moriarty.matrix.operator_.spawner.manager import SpawnerManager


def test_spawner_manager():
    registed = ["kube"]
    manager = SpawnerManager()

    assert sorted(registed) == sorted(manager.registed_cls.keys())

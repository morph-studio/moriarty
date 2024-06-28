from custom_spawner.imp import CustomSpawner

from moriarty.matrix.operator_.spawner.manager import SpawnerManager


def test_registed():
    manager = SpawnerManager()

    assert CustomSpawner.register_name in manager.registed_cls.keys()

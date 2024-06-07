from __future__ import annotations

import pluggy

project_name = "moriarty-matrix-bridge"
"""
The entry-point name of this extension.

Should be used in ``pyproject.toml`` as ``[project.entry-points."{project_name}"]``
"""
hookimpl = pluggy.HookimplMarker(project_name)
"""
Hookimpl marker for this extension, extension module should use this marker

Example:

    .. code-block:: python

        @hookimpl
        def register(manager):
            ...
"""

hookspec = pluggy.HookspecMarker(project_name)


@hookspec
def register(manager):
    """
    For more information about this function, please check the :ref:`manager`

    We provided an example package for you in ``{project_root}/example/extension/custom-bridge``.

    Example:

    .. code-block:: python

        class CustomBridge(QueueBridge):
            register_name = "example"

        from moriarty.matrix.job_manager.bridge.plugin import hookimpl

        @hookimpl
        def register(manager):
            manager.register(CustomBridge)


    Config ``project.entry-points`` so that we can find it

    .. code-block:: toml

        [project.entry-points.moriarty-matrix-bridge]
        {whatever-name} = "{package}.{path}.{to}.{file-with-hookimpl-function}"


    You can verify it by `moriarty-matrix list-plugins`.
    """


class QueueBridge:
    register_name: str

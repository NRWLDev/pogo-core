import dataclasses
from importlib.metadata import entry_points
from pathlib import Path


@dataclasses.dataclass
class Plugin:
    migrations: Path
    schema: str | None = None  # Inherit parent schema


def discover_migrations() -> list[tuple[str, Plugin]]:
    plugins = entry_points(group="pogo")

    ret = []
    for plugin in plugins:
        p = plugin.load()
        if hasattr(p, "plugin") and isinstance(p.plugin, Plugin):
            ret.append((plugin.name, p.plugin))

    return sorted(ret)

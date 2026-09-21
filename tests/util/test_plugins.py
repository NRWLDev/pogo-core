from unittest import mock

from pogo_core.util import plugins


class EntryPoint:
    def __init__(self, name: str) -> None:
        self.name = name

    def load(self) -> object:
        return self


class PluginEntryPoint:
    def __init__(self, name: str, plugin: plugins.Plugin) -> None:
        self.name = name
        self.plugin = plugin

    def load(self) -> object:
        return mock.Mock(
            plugin=self.plugin,
        )


def test_invalid_plugins_ignored(monkeypatch):
    monkeypatch.setattr(
        plugins,
        "entry_points",
        mock.Mock(
            return_value=[
                EntryPoint("fail"),
                PluginEntryPoint("invalid_type", mock.Mock()),
            ],
        ),
    )

    assert plugins.discover_migrations() == []


def test_plugins_discovered(monkeypatch):
    monkeypatch.setattr(
        plugins,
        "entry_points",
        mock.Mock(
            return_value=[
                PluginEntryPoint("no_schema", plugins.Plugin("./")),
                PluginEntryPoint("schema", plugins.Plugin("./", schema="schema")),
            ],
        ),
    )

    assert plugins.discover_migrations() == [
        ("no_schema", plugins.Plugin("./", schema=None)),
        ("schema", plugins.Plugin("./", schema="schema")),
    ]

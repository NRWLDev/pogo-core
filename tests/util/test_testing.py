from unittest import mock

import pytest

from pogo_core.util import testing


async def test_apply_no_db_info(cwd):
    with pytest.raises(ValueError, match="One of db or database_dsn are required to apply migrations"):
        await testing.apply(cwd / "migrations")


async def test_apply(monkeypatch, db_session, cwd):
    monkeypatch.setattr(testing.migrate, "apply", mock.AsyncMock())
    await testing.apply(cwd / "migrations", db_session)

    assert testing.migrate.apply.call_args == mock.call(
        db_session,
        cwd / "migrations",
    )


async def test_apply_loads_db(monkeypatch, cwd):
    monkeypatch.setattr(testing.migrate, "apply", mock.AsyncMock())
    mock_session = mock.Mock(close=mock.AsyncMock(return_value=None))
    monkeypatch.setattr(testing.sql, "get_connection", mock.AsyncMock(return_value=mock_session))
    await testing.apply(cwd / "migrations", database_dsn="postgres_dsn")

    assert testing.migrate.apply.call_args == mock.call(
        mock_session,
        cwd / "migrations",
    )
    assert mock_session.close.call_count == 1


async def test_rollback_no_db_info(cwd):
    with pytest.raises(ValueError, match="One of db or database_dsn are required to rollback migrations"):
        await testing.rollback(cwd / "migrations")


async def test_rollback(monkeypatch, db_session, cwd):
    monkeypatch.setattr(testing.migrate, "rollback", mock.AsyncMock())
    await testing.rollback(cwd / "migrations", db_session)

    assert testing.migrate.rollback.call_args == mock.call(
        db_session,
        cwd / "migrations",
    )


async def test_rollback_loads_db(monkeypatch, cwd):
    monkeypatch.setattr(testing.migrate, "rollback", mock.AsyncMock())
    mock_session = mock.Mock(close=mock.AsyncMock(return_value=None))
    monkeypatch.setattr(testing.sql, "get_connection", mock.AsyncMock(return_value=mock_session))
    await testing.rollback(cwd / "migrations", database_dsn="postgres_dsn")

    assert testing.migrate.rollback.call_args == mock.call(
        mock_session,
        cwd / "migrations",
    )
    assert mock_session.close.call_count == 1

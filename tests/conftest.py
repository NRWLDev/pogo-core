import os
import pathlib

import asyncpg
import pytest

from pogo_core import types  # noqa: F401 Shut up test coverage on the imports in this module
from pogo_core.migration import Migration
from pogo_core.util import sql


@pytest.fixture(autouse=True)
def cwd(tmp_path):
    orig = pathlib.Path.cwd()

    try:
        os.chdir(str(tmp_path))
        yield tmp_path
    finally:
        os.chdir(orig)


@pytest.fixture
def postgres_dsn():
    return os.environ["POSTGRES_DSN"]


@pytest.fixture(autouse=True)
async def db_session(request, postgres_dsn):
    conn = await asyncpg.connect(postgres_dsn)
    tr = conn.transaction()
    await tr.start()
    if request.node.get_closest_marker("nosync") is None:
        await sql.ensure_pogo_sync(conn)
    try:
        yield conn
    finally:
        await tr.rollback()
        await conn.close()


@pytest.fixture
def migrations(cwd):
    p = cwd / "migrations"
    p.mkdir()

    return p


@pytest.fixture
def migration_file_factory(migrations):
    def factory(mig_id, format_, contents):
        p = migrations / f"{mig_id}.{format_}"
        with p.open("w") as f:
            f.write(contents)

        return p

    return factory


@pytest.fixture(autouse=True)
def _clear_migration_tracking():
    # ClassVar remembers history across tests. Clear it every test.
    try:
        yield
    except:  # noqa: E722, S110
        pass
    finally:
        Migration._Migration__migrations = {}

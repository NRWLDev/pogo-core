from unittest import mock

import pytest

from pogo_core.util import sql


async def assert_tables(db_session, tables):
    stmt = """
    SELECT tablename
    FROM pg_tables
    WHERE  schemaname = 'public'
    ORDER BY tablename
    """
    results = await db_session.fetch(stmt)

    assert [r["tablename"] for r in results] == tables


async def assert_schemas(db_session, schemas):
    stmt = """
    SELECT schema_name
    FROM information_schema.schemata
    WHERE  schema_name NOT IN ('pg_toast', 'pg_catalog', 'information_schema')
    ORDER BY schema_name
    """
    results = await db_session.fetch(stmt)

    assert [r["schema_name"] for r in results] == schemas


@pytest.fixture(autouse=True)
def connect_patch_(db_session, monkeypatch):
    monkeypatch.setattr(sql.asyncpg, "connect", mock.AsyncMock(return_value=db_session))


@pytest.mark.nosync
async def test_get_connection_syncs_tables(db_session):
    db = await sql.get_connection(db_session)

    await assert_tables(db_session, ["_pogo_migration", "_pogo_version"])
    assert db == db_session


@pytest.mark.nosync
async def test_get_connection_creates_schema(db_session):
    db = await sql.get_connection(db_session, schema_name="unit", schema_create=True)

    await assert_schemas(db_session, ["public", "unit"])
    assert db == db_session


@pytest.mark.nosync
@pytest.mark.parametrize("schema", [None, "unit"])
async def test_ensure_pogo_sync_creates_tables(db_session, schema):
    if schema:
        db_session = await sql.get_connection("", schema_name=schema, schema_create=True)

    await sql.ensure_pogo_sync(db_session)

    await assert_tables(db_session, ["_pogo_migration", "_pogo_version"])


@pytest.mark.nosync
@pytest.mark.parametrize("schema", [None, "unit"])
async def test_ensure_pogo_sync_handles_existing_tables(db_session, schema):
    if schema:
        db_session = await sql.get_connection("", schema_name=schema, schema_create=True)

    await sql.ensure_pogo_sync(db_session)
    await sql.ensure_pogo_sync(db_session)

    await assert_tables(db_session, ["_pogo_migration", "_pogo_version"])


@pytest.mark.parametrize("schema", [None, "unit"])
async def test_migration_applied(db_session, schema):
    if schema:
        db_session = await sql.get_connection("", schema_name=schema, schema_create=True)

    schema = schema or "public"
    await sql.migration_applied(db_session, "migration_id", "migration_hash", schema_name=schema)

    ids = await sql.get_applied_migrations(db_session, schema_name=schema)
    assert ids == {"migration_id"}


@pytest.mark.parametrize("schema", [None, "unit"])
async def test_migration_unapplied(db_session, schema):
    if schema:
        db_session = await sql.get_connection("", schema_name=schema, schema_create=True)

    schema = schema or "public"

    await sql.migration_applied(db_session, "migration_id", "migration_hash", schema_name=schema)
    await sql.migration_unapplied(db_session, "migration_id", schema_name=schema)

    ids = await sql.get_applied_migrations(db_session, schema_name=schema)
    assert ids == set()


async def test_create_schema(db_session):
    await sql.create_schema(db_session, schema_name="unit")

    await assert_schemas(db_session, ["public", "unit"])


async def test_drop_schema(db_session):
    await db_session.execute("CREATE SCHEMA unit")

    await sql.drop_schema(db_session, schema_name="unit")

    await assert_schemas(db_session, ["public"])

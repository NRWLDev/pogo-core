from unittest import mock

import asyncpg
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


async def get_pogo_version(db_session):
    return await db_session.fetchval(
        "SELECT version FROM public._pogo_version ORDER BY version DESC LIMIT 1;",
    )


async def get_primary_key_columns(db_session, table_name):
    stmt = """
    SELECT a.attname
    FROM pg_index i
    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
    WHERE i.indrelid = $1::regclass AND i.indisprimary
    ORDER BY array_position(i.indkey, a.attnum)
    """
    results = await db_session.fetch(stmt, f"public.{table_name}")
    return [r["attname"] for r in results]


async def has_column(db_session, table_name, column_name):
    stmt = """
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2
    )
    """
    return await db_session.fetchval(stmt, table_name, column_name)


async def create_v0_tables(db_session, migration_rows=None):
    """Create pogo tables in v0 schema (single-column PK, no schema_name)."""
    await db_session.execute("""
        CREATE TABLE public._pogo_migration (
            migration_hash VARCHAR(64),
            migration_id VARCHAR(255),
            applied TIMESTAMPTZ,
            PRIMARY KEY (migration_hash)
        );
    """)
    await db_session.execute("""
        CREATE TABLE public._pogo_version (
            version INT NOT NULL PRIMARY KEY,
            installed TIMESTAMPTZ
        );
    """)
    await db_session.execute(
        "INSERT INTO public._pogo_version (version, installed) VALUES (0, now());",
    )

    if migration_rows:
        for hash_, id_ in migration_rows:
            await db_session.execute(
                "INSERT INTO public._pogo_migration (migration_hash, migration_id, applied) VALUES ($1, $2, now());",
                hash_,
                id_,
            )


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


@pytest.mark.nosync
async def test_fresh_install_creates_v1_schema(db_session):
    await sql.ensure_pogo_sync(db_session)

    pk_cols = await get_primary_key_columns(db_session, "_pogo_migration")
    assert pk_cols == ["migration_hash", "schema_name"]

    version = await get_pogo_version(db_session)
    assert version == 1

    versions = await db_session.fetch("SELECT version FROM public._pogo_version ORDER BY version")
    assert [r["version"] for r in versions] == [0, 1]


@pytest.mark.nosync
async def test_v0_to_v1_upgrade_with_existing_migrations(db_session):
    await create_v0_tables(
        db_session,
        migration_rows=[
            ("hash_aaa", "20240101_01_abcde-first"),
            ("hash_bbb", "20240102_01_fghij-second"),
        ],
    )

    pk_cols_before = await get_primary_key_columns(db_session, "_pogo_migration")
    assert pk_cols_before == ["migration_hash"]

    await sql.ensure_pogo_sync(db_session)

    pk_cols = await get_primary_key_columns(db_session, "_pogo_migration")
    assert pk_cols == ["migration_hash", "schema_name"]

    version = await get_pogo_version(db_session)
    assert version == 1

    rows = await db_session.fetch(
        "SELECT migration_hash, migration_id, schema_name FROM public._pogo_migration ORDER BY migration_id",
    )
    assert len(rows) == 2
    for row in rows:
        assert row["schema_name"] == "public"


@pytest.mark.nosync
async def test_v0_to_v1_upgrade_empty_table(db_session):
    await create_v0_tables(db_session)

    await sql.ensure_pogo_sync(db_session)

    pk_cols = await get_primary_key_columns(db_session, "_pogo_migration")
    assert pk_cols == ["migration_hash", "schema_name"]

    version = await get_pogo_version(db_session)
    assert version == 1


@pytest.mark.nosync
async def test_v0_to_v1_upgrade_rolls_back_on_failure(db_session):
    await create_v0_tables(
        db_session,
        migration_rows=[("hash_aaa", "20240101_01_abcde-first")],
    )

    await db_session.execute("""
        CREATE FUNCTION _pogo_test_fail_update() RETURNS TRIGGER AS $$
        BEGIN RAISE EXCEPTION 'simulated update failure'; END;
        $$ LANGUAGE plpgsql;
    """)
    await db_session.execute("""
        CREATE TRIGGER _pogo_test_block_update
        BEFORE UPDATE ON public._pogo_migration
        FOR EACH ROW EXECUTE FUNCTION _pogo_test_fail_update();
    """)

    with pytest.raises(asyncpg.exceptions.RaiseError, match="simulated update failure"):
        await sql.ensure_pogo_sync(db_session)

    pk_cols = await get_primary_key_columns(db_session, "_pogo_migration")
    assert pk_cols == ["migration_hash"]

    assert not await has_column(db_session, "_pogo_migration", "schema_name")

    version = await get_pogo_version(db_session)
    assert version == 0


@pytest.mark.nosync
async def test_ensure_pogo_sync_idempotent_at_v1(db_session):
    await sql.ensure_pogo_sync(db_session)
    await sql.ensure_pogo_sync(db_session)
    await sql.ensure_pogo_sync(db_session)

    await assert_tables(db_session, ["_pogo_migration", "_pogo_version"])

    pk_cols = await get_primary_key_columns(db_session, "_pogo_migration")
    assert pk_cols == ["migration_hash", "schema_name"]

    version = await get_pogo_version(db_session)
    assert version == 1


@pytest.mark.nosync
async def test_fresh_install_with_preexisting_migration_table(db_session):
    """Simulate race: another process created _pogo_migration but _pogo_version
    doesn't exist yet when this process checks. IF NOT EXISTS prevents failure."""
    await db_session.execute("""
        CREATE TABLE public._pogo_migration (
            migration_hash VARCHAR(64),
            migration_id VARCHAR(255),
            applied TIMESTAMPTZ,
            PRIMARY KEY (migration_hash)
        );
    """)

    await sql.ensure_pogo_sync(db_session)

    await assert_tables(db_session, ["_pogo_migration", "_pogo_version"])
    pk_cols = await get_primary_key_columns(db_session, "_pogo_migration")
    assert pk_cols == ["migration_hash", "schema_name"]
    version = await get_pogo_version(db_session)
    assert version == 1


@pytest.mark.nosync
async def test_fresh_install_with_fully_preexisting_v0_tables(db_session):
    """Simulate race: another process completed the entire fresh install (both
    tables + version 0 row) before this process enters the fresh install path.
    IF NOT EXISTS + ON CONFLICT DO NOTHING prevents failure."""
    await create_v0_tables(db_session)

    # Re-execute the same DDL that ensure_pogo_sync's fresh install path uses,
    # simulating a second process that passed the existence check before tables existed.
    await db_session.execute("""
        CREATE TABLE IF NOT EXISTS public._pogo_migration (
            migration_hash VARCHAR(64),
            migration_id VARCHAR(255),
            applied TIMESTAMPTZ,
            PRIMARY KEY (migration_hash)
        );
    """)
    await db_session.execute("""
        CREATE TABLE IF NOT EXISTS public._pogo_version (
            version INT NOT NULL PRIMARY KEY,
            installed TIMESTAMPTZ
        );
    """)
    await db_session.execute("""
        INSERT INTO public._pogo_version (version, installed) VALUES (0, now())
        ON CONFLICT DO NOTHING;
    """)

    versions = await db_session.fetch("SELECT version FROM public._pogo_version ORDER BY version")
    assert [r["version"] for r in versions] == [0]

    await sql.ensure_pogo_sync(db_session)

    pk_cols = await get_primary_key_columns(db_session, "_pogo_migration")
    assert pk_cols == ["migration_hash", "schema_name"]
    version = await get_pogo_version(db_session)
    assert version == 1


@pytest.mark.nosync
async def test_concurrent_upgrade_second_caller_skips(db_session, postgres_dsn):
    """Simulate two processes both reaching the upgrade transaction. The first
    upgrades v0→v1; the second reads v1 (via FOR UPDATE) and skips."""
    await create_v0_tables(db_session)

    # First call: upgrades v0 → v1
    await sql.ensure_pogo_sync(db_session)
    version = await get_pogo_version(db_session)
    assert version == 1

    # Second call on same connection: should see v1 and do nothing
    await sql.ensure_pogo_sync(db_session)
    version = await get_pogo_version(db_session)
    assert version == 1

    pk_cols = await get_primary_key_columns(db_session, "_pogo_migration")
    assert pk_cols == ["migration_hash", "schema_name"]

    versions = await db_session.fetch("SELECT version FROM public._pogo_version ORDER BY version")
    assert [r["version"] for r in versions] == [0, 1]

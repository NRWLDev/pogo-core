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
async def test_v0_to_v1_upgrade_rolls_back_and_recovers(db_session):
    """Verify the v0->v1 upgrade is atomic and recoverable after a failure."""
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

    assert await get_primary_key_columns(db_session, "_pogo_migration") == ["migration_hash"]
    assert not await has_column(db_session, "_pogo_migration", "schema_name")
    assert await get_pogo_version(db_session) == 0

    await db_session.execute(
        "DROP TRIGGER _pogo_test_block_update ON public._pogo_migration;",
    )
    await db_session.execute("DROP FUNCTION _pogo_test_fail_update();")

    await sql.ensure_pogo_sync(db_session)

    assert await get_pogo_version(db_session) == 1
    assert await get_primary_key_columns(db_session, "_pogo_migration") == [
        "migration_hash",
        "schema_name",
    ]
    assert await has_column(db_session, "_pogo_migration", "schema_name")


@pytest.mark.nosync
async def test_v0_to_v1_upgrade_with_update_publication(db_session):
    """Reproduces Google Cloud CloudSQL v0->v1 upgrade failure.

    Running pogo against a CloudSQL Postgres instance
    with a Datastream CDC publication attached:

        ERROR: cannot update table "_pogo_migration" because it does not have
        a replica identity and publishes updates
        HINT: To enable updating the table, set REPLICA IDENTITY using ALTER TABLE.
        STATEMENT: UPDATE public._pogo_migration SET schema_name = 'public';

    The error is raised by Postgres whenever a table belongs to a publication
    that publishes UPDATE and the table has no replica identity.
    https://www.postgresql.org/docs/current/logical-replication-publication.html

    CloudSQL Datastream CDC configures exactly such a publication:

        CREATE PUBLICATION ... FOR TABLE ...

    https://cloud.google.com/datastream/docs/configure-cloudsql-psql
    """
    await create_v0_tables(
        db_session,
        migration_rows=[("hash_aaa", "20240101_01_abcde-first")],
    )

    await db_session.execute(
        "CREATE PUBLICATION _pogo_test_pub "
        "FOR TABLE public._pogo_migration WITH (publish = 'update');",
    )

    await sql.ensure_pogo_sync(db_session)

    assert await get_pogo_version(db_session) == 1
    assert await get_primary_key_columns(db_session, "_pogo_migration") == [
        "migration_hash",
        "schema_name",
    ]

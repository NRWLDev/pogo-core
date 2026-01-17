from __future__ import annotations

import typing as t

import asyncpg

from pogo_core.migration import Migration

if t.TYPE_CHECKING:
    from pathlib import Path


async def get_connection(
    connection_string: str,
    *,
    schema_name: str = "public",
    schema_create: bool = False,
) -> asyncpg.Connection:
    db = await asyncpg.connect(connection_string)

    if schema_create:
        await create_schema(db, schema_name=schema_name)

    await db.execute(f"SET search_path TO {schema_name}")
    await ensure_pogo_sync(db)

    return db


async def read_migrations(
    migrations_location: Path,
    db: asyncpg.Connection | None,
    *,
    schema_name: str,
) -> list[Migration]:
    applied_migrations = await get_applied_migrations(db, schema_name=schema_name) if db else set()
    return [
        Migration(path.stem, path, applied_migrations)
        for path in migrations_location.iterdir()
        if path.suffix in {".py", ".sql"}
    ]


async def get_applied_migrations(db: asyncpg.Connection, *, schema_name: str) -> set[str]:
    stmt = """
    SELECT
        migration_id
    FROM public._pogo_migration
    WHERE schema_name = $1
    """
    results = await db.fetch(stmt, schema_name)

    return {r["migration_id"] for r in results}


async def ensure_pogo_sync(db: asyncpg.Connection) -> None:
    stmt = """
    SELECT exists (
        SELECT FROM pg_tables
        WHERE  schemaname = 'public'
        AND    tablename  = '_pogo_version'
    );
    """
    r = await db.fetchrow(stmt)
    if r is not None and not r["exists"]:
        stmt = """
        CREATE TABLE public._pogo_migration (
            migration_hash VARCHAR(64),  -- sha256 hash of the migration id
            migration_id VARCHAR(255),   -- The migration id (ie path basename without extension)
            applied TIMESTAMPTZ,         -- When this id was applied
            PRIMARY KEY (migration_hash)
        );
        """
        await db.execute(stmt)

        stmt = """
        CREATE TABLE public._pogo_version (
            version INT NOT NULL PRIMARY KEY,
            installed TIMESTAMPTZ
        );
        """
        await db.execute(stmt)

        stmt = """
        INSERT INTO public._pogo_version (version, installed) VALUES (0, now());
        """
        await db.execute(stmt)

    stmt = "SELECT version FROM public._pogo_version ORDER BY version DESC LIMIT 1;"
    version = await db.fetchval(stmt)

    if version == 0:
        stmt = """
        ALTER TABLE public._pogo_migration
        ADD COLUMN schema_name VARCHAR(64),      -- Host schema for this set of migrations.
        DROP CONSTRAINT _pogo_migration_pkey;
        """
        await db.execute(stmt)

        stmt = """
        UPDATE public._pogo_migration SET schema_name = 'public';
        """
        await db.execute(stmt)

        stmt = """
        ALTER TABLE public._pogo_migration
        ALTER COLUMN schema_name SET NOT NULL,
        ADD PRIMARY KEY (migration_hash, schema_name);
        """
        await db.execute(stmt)

        stmt = """
        INSERT INTO public._pogo_version (version, installed) VALUES (1, now());
        """
        await db.execute(stmt)


async def migration_applied(
    db: asyncpg.Connection,
    migration_id: str,
    migration_hash: str,
    *,
    schema_name: str,
) -> None:
    stmt = """
    INSERT INTO public._pogo_migration (
        migration_hash,
        migration_id,
        schema_name,
        applied
    ) VALUES (
        $1, $2, $3, now()
    )
    """
    await db.execute(stmt, migration_hash, migration_id, schema_name)


async def migration_unapplied(db: asyncpg.Connection, migration_id: str, *, schema_name: str) -> None:
    stmt = """
    DELETE FROM public._pogo_migration
    WHERE migration_id = $1 AND schema_name = $2
    """
    await db.execute(stmt, migration_id, schema_name)


async def create_schema(db: asyncpg.Connection, *, schema_name: str) -> None:
    stmt = f"""
    CREATE SCHEMA IF NOT EXISTS "{schema_name}";
    """

    await db.execute(stmt)


async def drop_schema(db: asyncpg.Connection, *, schema_name: str) -> None:
    stmt = f"""
    DROP SCHEMA IF EXISTS "{schema_name}";
    """

    await db.execute(stmt)

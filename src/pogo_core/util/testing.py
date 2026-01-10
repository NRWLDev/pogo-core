from __future__ import annotations

import typing as t

from pogo_core.util import migrate, sql

if t.TYPE_CHECKING:
    from pathlib import Path

    import asyncpg


async def apply(migrations: Path, db: asyncpg.Connection | None = None, database_dsn: str | None = None) -> None:
    if db is None and database_dsn is None:
        msg = "One of db or database_dsn are required to apply migrations."
        raise ValueError(msg)

    db_ = db if db is not None else await sql.get_connection(database_dsn)
    try:
        await migrate.apply(db_, migrations)
    finally:
        if db is None:
            await db_.close()


async def rollback(migrations: Path, db: asyncpg.Connection | None = None, database_dsn: str | None = None) -> None:
    if db is None and database_dsn is None:
        msg = "One of db or database_dsn are required to rollback migrations."
        raise ValueError(msg)

    db_ = db if db is not None else await sql.get_connection(database_dsn)
    try:
        await migrate.rollback(db_, migrations)
    finally:
        if db is None:
            await db_.close()

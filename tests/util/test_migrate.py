from unittest import mock

import pytest

from pogo_core import error
from pogo_core.util import migrate, sql


@pytest.fixture
def _migration_one(migrations):
    p = migrations / "20240317_01_abcde-initial-migration.sql"

    with p.open("w") as f:
        f.write("""
-- initial migration
-- depends:

-- migrate: apply
CREATE TABLE table_one();

-- migrate: rollback
DROP TABLE table_one;
""")


@pytest.fixture
def _migration_two(migrations, _migration_one):
    p = migrations / "20240317_02_12345-second-migration.sql"

    with p.open("w") as f:
        f.write("""
-- second migration
-- depends: 20240317_01_abcde-initial-migration
-- transaction: false

-- migrate: apply
CREATE TABLE table_two();

-- Auto generated
-- From psql

-- migrate: rollback

-- Auto generated
-- From psql
DROP TABLE table_two;
""")


@pytest.fixture
def _broken_apply(migrations, _migration_two):
    p = migrations / "20240318_01_12345-broken-apply.sql"

    with p.open("w") as f:
        f.write("""
-- broker migration
-- depends: 20240317_02_12345-second-migration

-- migrate: apply
CREATE TABLE table_three;

-- migrate: rollback
DROP TABLE table_two;
""")


@pytest.fixture
def _broken_rollback(migrations, _migration_two):
    p = migrations / "20240318_01_12345-broken-rollback.sql"

    with p.open("w") as f:
        f.write("""
-- broker migration
-- depends: 20240317_02_12345-second-migration

-- migrate: apply
CREATE TABLE table_three();

-- migrate: rollback
DROP TABLE table_four;
""")


class Base:
    async def assert_tables(self, db_session, tables):
        stmt = """
        SELECT schemaname, tablename
        FROM pg_tables
        WHERE  schemaname = 'public' or schemaname = 'pogo'
        ORDER BY schemaname, tablename
        """
        results = await db_session.fetch(stmt)

        assert [f"{r['schemaname']}.{r['tablename']}" for r in results] == tables


@pytest.fixture(autouse=True)
def connect_patch_(db_session, monkeypatch):
    monkeypatch.setattr(sql.asyncpg, "connect", mock.AsyncMock(return_value=db_session))


class TestApply(Base):
    @pytest.mark.usefixtures("migrations")
    async def test_no_migrations_applies_pogo_tables(self, migrations, db_session):
        await migrate.apply(db_session, migrations, schema_name="public")

        await self.assert_tables(db_session, ["public._pogo_migration", "public._pogo_version"])

    @pytest.mark.usefixtures("_migration_two")
    async def test_migrations_applied(self, migrations, db_session):
        await migrate.apply(db_session, migrations, schema_name="public")

        await self.assert_tables(
            db_session,
            ["public._pogo_migration", "public._pogo_version", "public.table_one", "public.table_two"],
        )

    @pytest.mark.usefixtures("_migration_two")
    async def test_already_applied_skips(self, migrations, db_session):
        await migrate.apply(db_session, migrations, schema_name="public")
        await migrate.apply(db_session, migrations, schema_name="public")

        await self.assert_tables(
            db_session,
            ["public._pogo_migration", "public._pogo_version", "public.table_one", "public.table_two"],
        )

    @pytest.mark.usefixtures("_migration_two")
    async def test_apply_multiple_schemas(self, migrations, db_session):
        await migrate.apply(db_session, migrations, schema_name="public")
        db_session = await sql.get_connection("", schema_name="pogo", schema_create=True)
        await migrate.apply(db_session, migrations, schema_name="pogo")

        await self.assert_tables(
            db_session,
            [
                "pogo.table_one",
                "pogo.table_two",
                "public._pogo_migration",
                "public._pogo_version",
                "public.table_one",
                "public.table_two",
            ],
        )

    @pytest.mark.usefixtures("_broken_apply")
    async def test_broken_migration_not_applied(self, migrations, db_session):
        with pytest.raises(error.BadMigrationError) as e:
            await migrate.apply(db_session, migrations, schema_name="public")

        await self.assert_tables(
            db_session,
            ["public._pogo_migration", "public._pogo_version", "public.table_one", "public.table_two"],
        )
        assert str(e.value) == "Failed to apply 20240318_01_12345-broken-apply"


class TestRollback(Base):
    @pytest.mark.usefixtures("migrations")
    async def test_no_migrations_applies_pogo_tables(self, migrations, db_session):
        await migrate.rollback(db_session, migrations, schema_name="public")

        await self.assert_tables(db_session, ["public._pogo_migration", "public._pogo_version"])

    @pytest.mark.usefixtures("_migration_two")
    async def test_latest_removed(self, migrations, db_session):
        await migrate.apply(db_session, migrations, schema_name="public")
        await migrate.rollback(db_session, migrations, schema_name="public", count=1)

        await self.assert_tables(db_session, ["public._pogo_migration", "public._pogo_version", "public.table_one"])

    @pytest.mark.usefixtures("_migration_two")
    async def test_all_removed(self, migrations, db_session):
        await migrate.apply(db_session, migrations, schema_name="public")
        await migrate.rollback(db_session, migrations, schema_name="public")

        await self.assert_tables(db_session, ["public._pogo_migration", "public._pogo_version"])

    @pytest.mark.usefixtures("_migration_two")
    async def test_only_specified_schema_rolledback(self, migrations, db_session):
        await migrate.apply(db_session, migrations, schema_name="public")
        db_session = await sql.get_connection("", schema_name="pogo", schema_create=True)
        await migrate.apply(db_session, migrations, schema_name="pogo")

        await migrate.rollback(db_session, migrations, schema_name="pogo", count=1)

        await self.assert_tables(
            db_session,
            [
                "pogo.table_one",
                "public._pogo_migration",
                "public._pogo_version",
                "public.table_one",
                "public.table_two",
            ],
        )

    @pytest.mark.usefixtures("_broken_rollback")
    async def test_broken_rollback_rollsback(self, migrations, db_session):
        await migrate.apply(db_session, migrations, schema_name="public")
        with pytest.raises(error.BadMigrationError) as e:
            await migrate.rollback(db_session, migrations, schema_name="public", count=1)

        await self.assert_tables(
            db_session,
            [
                "public._pogo_migration",
                "public._pogo_version",
                "public.table_one",
                "public.table_three",
                "public.table_two",
            ],
        )
        assert str(e.value) == "Failed to rollback 20240318_01_12345-broken-rollback"

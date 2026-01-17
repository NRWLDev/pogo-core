[1mdiff --git a/src/pogo_core/util/migrate.py b/src/pogo_core/util/migrate.py[m
[1mindex c958262..40c8350 100644[m
[1m--- a/src/pogo_core/util/migrate.py[m
[1m+++ b/src/pogo_core/util/migrate.py[m
[36m@@ -39,12 +39,13 @@[m [masync def transaction(db: asyncpg.Connection, migration: Migration) -> t.AsyncIt[m
 async def apply([m
     db: asyncpg.Connection,[m
     migrations_dir: Path,[m
[32m+[m[32m    *,[m
     schema_name: str,[m
     logger: Logger | logging.Logger | None = None,[m
 ) -> None:[m
     logger = logger or logger_[m
     await sql.ensure_pogo_sync(db)[m
[31m-    migrations = await sql.read_migrations(migrations_dir, db, schema_name)[m
[32m+[m[32m    migrations = await sql.read_migrations(migrations_dir, db, schema_name=schema_name)[m
     migrations = topological_sort([m.load() for m in migrations])[m
 [m
     for migration in migrations:[m
[36m@@ -54,7 +55,7 @@[m [masync def apply([m
                 logger.warning("Applying %s", migration.id)[m
                 async with transaction(db, migration):[m
                     await migration.apply(db)[m
[31m-                    await sql.migration_applied(db, migration.id, migration.hash, schema_name)[m
[32m+[m[32m                    await sql.migration_applied(db, migration.id, migration.hash, schema_name=schema_name)[m
         except Exception as e:  # noqa: PERF203[m
             msg = f"Failed to apply {migration.id}"[m
             raise error.BadMigrationError(msg) from e[m
[36m@@ -63,13 +64,14 @@[m [masync def apply([m
 async def rollback([m
     db: asyncpg.Connection,[m
     migrations_dir: Path,[m
[32m+[m[32m    *,[m
     schema_name: str,[m
     count: int | None = None,[m
     logger: Logger | logging.Logger | None = None,[m
 ) -> None:[m
     logger = logger or logger_[m
     await sql.ensure_pogo_sync(db)[m
[31m-    migrations = await sql.read_migrations(migrations_dir, db, schema_name)[m
[32m+[m[32m    migrations = await sql.read_migrations(migrations_dir, db, schema_name=schema_name)[m
     migrations = reversed(list(topological_sort([m.load() for m in migrations])))[m
 [m
     i = 0[m
[36m@@ -81,7 +83,7 @@[m [masync def rollback([m
 [m
                 async with transaction(db, migration):[m
                     await migration.rollback(db)[m
[31m-                    await sql.migration_unapplied(db, migration.id, schema_name)[m
[32m+[m[32m                    await sql.migration_unapplied(db, migration.id, schema_name=schema_name)[m
                     i += 1[m
         except Exception as e:  # noqa: PERF203[m
             msg = f"Failed to rollback {migration.id}"[m
[1mdiff --git a/src/pogo_core/util/sql.py b/src/pogo_core/util/sql.py[m
[1mindex 08e64c4..9f9ec39 100644[m
[1m--- a/src/pogo_core/util/sql.py[m
[1m+++ b/src/pogo_core/util/sql.py[m
[36m@@ -12,14 +12,14 @@[m [mif t.TYPE_CHECKING:[m
 [m
 async def get_connection([m
     connection_string: str,[m
[31m-    schema_name: str = "public",[m
     *,[m
[32m+[m[32m    schema_name: str = "public",[m
     schema_create: bool = False,[m
 ) -> asyncpg.Connection:[m
     db = await asyncpg.connect(connection_string)[m
 [m
     if schema_create:[m
[31m-        await create_schema(db, schema_name)[m
[32m+[m[32m        await create_schema(db, schema_name=schema_name)[m
 [m
     await db.execute(f"SET search_path TO {schema_name}")[m
     await ensure_pogo_sync(db)[m
[36m@@ -30,9 +30,10 @@[m [masync def get_connection([m
 async def read_migrations([m
     migrations_location: Path,[m
     db: asyncpg.Connection | None,[m
[32m+[m[32m    *,[m
     schema_name: str,[m
 ) -> list[Migration]:[m
[31m-    applied_migrations = await get_applied_migrations(db, schema_name) if db else set()[m
[32m+[m[32m    applied_migrations = await get_applied_migrations(db, schema_name=schema_name) if db else set()[m
     return [[m
         Migration(path.stem, path, applied_migrations)[m
         for path in migrations_location.iterdir()[m
[36m@@ -40,7 +41,7 @@[m [masync def read_migrations([m
     ][m
 [m
 [m
[31m-async def get_applied_migrations(db: asyncpg.Connection, schema_name: str) -> set[str]:[m
[32m+[m[32masync def get_applied_migrations(db: asyncpg.Connection, *, schema_name: str) -> set[str]:[m
     stmt = """[m
     SELECT[m
         migration_id[m
[36m@@ -68,7 +69,7 @@[m [masync def ensure_pogo_sync(db: asyncpg.Connection) -> None:[m
             migration_id VARCHAR(255),   -- The migration id (ie path basename without extension)[m
             applied TIMESTAMPTZ,         -- When this id was applied[m
             PRIMARY KEY (migration_hash)[m
[31m-        )[m
[32m+[m[32m        );[m
         """[m
         await db.execute(stmt)[m
 [m
[36m@@ -76,32 +77,45 @@[m [masync def ensure_pogo_sync(db: asyncpg.Connection) -> None:[m
         CREATE TABLE public._pogo_version ([m
             version INT NOT NULL PRIMARY KEY,[m
             installed TIMESTAMPTZ[m
[31m-        )[m
[32m+[m[32m        );[m
         """[m
         await db.execute(stmt)[m
 [m
         stmt = """[m
[31m-        INSERT INTO public._pogo_version (version, installed) VALUES (0, now())[m
[32m+[m[32m        INSERT INTO public._pogo_version (version, installed) VALUES (0, now());[m
         """[m
         await db.execute(stmt)[m
 [m
[31m-    stmt = "SELECT version FROM public._pogo_version ORDER BY version DESC LIMIT 1"[m
[32m+[m[32m    stmt = "SELECT version FROM public._pogo_version ORDER BY version DESC LIMIT 1;"[m
     version = await db.fetchval(stmt)[m
 [m
     if version == 0:[m
         stmt = """[m
         ALTER TABLE public._pogo_migration[m
[31m-        ADD COLUMN schema_name VARCHAR(64)    -- Host schema for this set of migrations.[m
[32m+[m[32m        ADD COLUMN schema_name VARCHAR(64),      -- Host schema for this set of migrations.[m
[32m+[m[32m        DROP CONSTRAINT _pogo_migration_pkey;[m
[32m+[m[32m        """[m
[32m+[m[32m        await db.execute(stmt)[m
[32m+[m
[32m+[m[32m        stmt = """[m
[32m+[m[32m        UPDATE public._pogo_migration SET schema_name = 'public';[m
[32m+[m[32m        """[m
[32m+[m[32m        await db.execute(stmt)[m
[32m+[m
[32m+[m[32m        stmt = """[m
[32m+[m[32m        ALTER TABLE public._pogo_migration[m
[32m+[m[32m        ALTER COLUMN schema_name SET NOT NULL,[m
[32m+[m[32m        ADD PRIMARY KEY (migration_hash, schema_name);[m
         """[m
         await db.execute(stmt)[m
 [m
         stmt = """[m
[31m-        INSERT INTO public._pogo_version (version, installed) VALUES (1, now())[m
[32m+[m[32m        INSERT INTO public._pogo_version (version, installed) VALUES (1, now());[m
         """[m
         await db.execute(stmt)[m
 [m
 [m
[31m-async def migration_applied(db: asyncpg.Connection, migration_id: str, migration_hash: str, schema_name: str) -> None:[m
[32m+[m[32masync def migration_applied(db: asyncpg.Connection, migration_id: str, migration_hash: str, *, schema_name: str) -> None:[m
     stmt = """[m
     INSERT INTO public._pogo_migration ([m
         migration_hash,[m
[36m@@ -115,7 +129,7 @@[m [masync def migration_applied(db: asyncpg.Connection, migration_id: str, migration[m
     await db.execute(stmt, migration_hash, migration_id, schema_name)[m
 [m
 [m
[31m-async def migration_unapplied(db: asyncpg.Connection, migration_id: str, schema_name: str) -> None:[m
[32m+[m[32masync def migration_unapplied(db: asyncpg.Connection, migration_id: str, *, schema_name: str) -> None:[m
     stmt = """[m
     DELETE FROM public._pogo_migration[m
     WHERE migration_id = $1 AND schema_name = $2[m
[36m@@ -123,7 +137,7 @@[m [masync def migration_unapplied(db: asyncpg.Connection, migration_id: str, schema_[m
     await db.execute(stmt, migration_id, schema_name)[m
 [m
 [m
[31m-async def create_schema(db: asyncpg.Connection, schema_name: str) -> None:[m
[32m+[m[32masync def create_schema(db: asyncpg.Connection, *, schema_name: str) -> None:[m
     stmt = f"""[m
     CREATE SCHEMA IF NOT EXISTS "{schema_name}";[m
     """[m
[36m@@ -131,7 +145,7 @@[m [masync def create_schema(db: asyncpg.Connection, schema_name: str) -> None:[m
     await db.execute(stmt)[m
 [m
 [m
[31m-async def drop_schema(db: asyncpg.Connection, schema_name: str) -> None:[m
[32m+[m[32masync def drop_schema(db: asyncpg.Connection, *, schema_name: str) -> None:[m
     stmt = f"""[m
     DROP SCHEMA IF EXISTS "{schema_name}";[m
     """[m
[1mdiff --git a/tests/conftest.py b/tests/conftest.py[m
[1mindex 9757119..d979260 100644[m
[1m--- a/tests/conftest.py[m
[1m+++ b/tests/conftest.py[m
[36m@@ -27,9 +27,7 @@[m [mdef postgres_dsn():[m
 [m
 @pytest.fixture(autouse=True)[m
 async def db_session(request, postgres_dsn):[m
[31m-    schema_name = "pogo"[m
     conn = await asyncpg.connect(postgres_dsn)[m
[31m-    await conn.execute(f"SET search_path TO {schema_name}")[m
     tr = conn.transaction()[m
     await tr.start()[m
     if request.node.get_closest_marker("nosync") is None:[m
[1mdiff --git a/tests/util/test_migrate.py b/tests/util/test_migrate.py[m
[1mindex 4ed1fd0..4b2033c 100644[m
[1m--- a/tests/util/test_migrate.py[m
[1m+++ b/tests/util/test_migrate.py[m
[36m@@ -1,7 +1,9 @@[m
[32m+[m[32mfrom unittest import mock[m
[32m+[m
 import pytest[m
 [m
 from pogo_core import error[m
[31m-from pogo_core.util import migrate[m
[32m+[m[32mfrom pogo_core.util import migrate, sql[m
 [m
 [m
 @pytest.fixture[m
[36m@@ -14,7 +16,6 @@[m [mdef _migration_one(migrations):[m
 -- depends:[m
 [m
 -- migrate: apply[m
[31m-CREATE SCHEMA pogo[m
 CREATE TABLE table_one();[m
 [m
 -- migrate: rollback[m
[36m@@ -83,74 +84,97 @@[m [mDROP TABLE table_four;[m
 class Base:[m
     async def assert_tables(self, db_session, tables):[m
         stmt = """[m
[31m-        SELECT tablename[m
[32m+[m[32m        SELECT schemaname, tablename[m
         FROM pg_tables[m
         WHERE  schemaname = 'public' or schemaname = 'pogo'[m
[31m-        ORDER BY tablename[m
[32m+[m[32m        ORDER BY schemaname, tablename[m
         """[m
         results = await db_session.fetch(stmt)[m
 [m
[31m-        assert [r["tablename"] for r in results] == tables[m
[32m+[m[32m        assert [f'{r["schemaname"]}.{r["tablename"]}' for r in results] == tables[m
[32m+[m
[32m+[m
[32m+[m[32m@pytest.fixture(autouse=True)[m
[32m+[m[32mdef connect_patch_(db_session, monkeypatch):[m
[32m+[m[32m    monkeypatch.setattr(sql.asyncpg, "connect", mock.AsyncMock(return_value=db_session))[m
 [m
 [m
 class TestApply(Base):[m
     @pytest.mark.usefixtures("migrations")[m
     async def test_no_migrations_applies_pogo_tables(self, migrations, db_session):[m
[31m-        await migrate.apply(db_session, migrations, "pogo")[m
[32m+[m[32m        await migrate.apply(db_session, migrations, schema_name="public")[m
 [m
[31m-        await self.assert_tables(db_session, ["_pogo_migration", "_pogo_version"])[m
[32m+[m[32m        await self.assert_tables(db_session, ["public._pogo_migration", "public._pogo_version"])[m
 [m
     @pytest.mark.usefixtures("_migration_two")[m
     async def test_migrations_applied(self, migrations, db_session):[m
[31m-        await migrate.apply(db_session, migrations, "pogo")[m
[32m+[m[32m        await migrate.apply(db_session, migrations, schema_name="public")[m
 [m
[31m-        await self.assert_tables(db_session, ["_pogo_migration", "_pogo_version", "table_one", "table_two"])[m
[32m+[m[32m        await self.assert_tables(db_session, ["public._pogo_migration", "public._pogo_version", "public.table_one", "public.table_two"])[m
 [m
     @pytest.mark.usefixtures("_migration_two")[m
     async def test_already_applied_skips(self, migrations, db_session):[m
[31m-        await migrate.apply(db_session, migrations, "pogo")[m
[31m-        await migrate.apply(db_session, migrations, "pogo")[m
[32m+[m[32m        await migrate.apply(db_session, migrations, schema_name="public")[m
[32m+[m[32m        await migrate.apply(db_session, migrations, schema_name="public")[m
[32m+[m
[32m+[m[32m        await self.assert_tables(db_session, ["public._pogo_migration", "public._pogo_version", "public.table_one", "public.table_two"])[m
[32m+[m
[32m+[m[32m    @pytest.mark.usefixtures("_migration_two")[m
[32m+[m[32m    async def test_apply_multiple_schemas(self, migrations, db_session):[m
[32m+[m[32m        await migrate.apply(db_session, migrations, schema_name="public")[m
[32m+[m[32m        db_session = await sql.get_connection("", schema_name="pogo", schema_create=True)[m
[32m+[m[32m        await migrate.apply(db_session, migrations, schema_name="pogo")[m
 [m
[31m-        await self.assert_tables(db_session, ["_pogo_migration", "_pogo_version", "table_one", "table_two"])[m
[32m+[m[32m        await self.assert_tables(db_session, ["pogo.table_one", "pogo.table_two", "public._pogo_migration", "public._pogo_version", "public.table_one", "public.table_two"])[m
 [m
     @pytest.mark.usefixtures("_broken_apply")[m
     async def test_broken_migration_not_applied(self, migrations, db_session):[m
         with pytest.raises(error.BadMigrationError) as e:[m
[31m-            await migrate.apply(db_session, migrations, "pogo")[m
[32m+[m[32m            await migrate.apply(db_session, migrations, schema_name="public")[m
 [m
[31m-        await self.assert_tables(db_session, ["_pogo_migration", "_pogo_version", "table_one", "table_two"])[m
[32m+[m[32m        await self.assert_tables(db_session, ["public._pogo_migration", "public._pogo_version", "public.table_one", "public.table_two"])[m
         assert str(e.value) == "Failed to apply 20240318_01_12345-broken-apply"[m
 [m
 [m
 class TestRollback(Base):[m
     @pytest.mark.usefixtures("migrations")[m
     async def test_no_migrations_applies_pogo_tables(self, migrations, db_session):[m
[31m-        await migrate.rollback(db_session, migrations, "pogo")[m
[32m+[m[32m        await migrate.rollback(db_session, migrations, schema_name="public")[m
 [m
[31m-        await self.assert_tables(db_session, ["_pogo_migration", "_pogo_version"])[m
[32m+[m[32m        await self.assert_tables(db_session, ["public._pogo_migration", "public._pogo_version"])[m
 [m
     @pytest.mark.usefixtures("_migration_two")[m
     async def test_latest_removed(self, migrations, db_session):[m
[31m-        await migrate.apply(db_session, migrations, "pogo")[m
[31m-        await migrate.rollback(db_session, migrations, "pogo", count=1)[m
[32m+[m[32m        await migrate.apply(db_session, migrations, schema_name="public")[m
[32m+[m[32m        await migrate.rollback(db_session, migrations, schema_name="public", count=1)[m
 [m
[31m-        await self.assert_tables(db_session, ["_pogo_migration", "_pogo_version", "table_one"])[m
[32m+[m[32m        await self.assert_tables(db_session, ["public._pogo_migration", "public._pogo_version", "public.table_one"])[m
 [m
     @pytest.mark.usefixtures("_migration_two")[m
     async def test_all_removed(self, migrations, db_session):[m
[31m-        await migrate.apply(db_session, migrations, "pogo")[m
[31m-        await migrate.rollback(db_session, migrations, "pogo")[m
[32m+[m[32m        await migrate.apply(db_session, migrations, schema_name="public")[m
[32m+[m[32m        await migrate.rollback(db_session, migrations, schema_name="public")[m
[32m+[m
[32m+[m[32m        await self.assert_tables(db_session, ["public._pogo_migration", "public._pogo_version"])[m
[32m+[m
[32m+[m[32m    @pytest.mark.usefixtures("_migration_two")[m
[32m+[m[32m    async def test_only_specified_schema_rolledback(self, migrations, db_session):[m
[32m+[m[32m        await migrate.apply(db_session, migrations, schema_name="public")[m
[32m+[m[32m        db_session = await sql.get_connection("", schema_name="pogo", schema_create=True)[m
[32m+[m[32m        await migrate.apply(db_session, migrations, schema_name="pogo")[m
[32m+[m
[32m+[m[32m        await migrate.rollback(db_session, migrations, schema_name="pogo", count=1)[m
 [m
[31m-        await self.assert_tables(db_session, ["_pogo_migration", "_pogo_version"])[m
[32m+[m[32m        await self.assert_tables(db_session, ["pogo.table_one", "public._pogo_migration", "public._pogo_version", "public.table_one", "public.table_two"])[m
 [m
     @pytest.mark.usefixtures("_broken_rollback")[m
     async def test_broken_rollback_rollsback(self, migrations, db_session):[m
[31m-        await migrate.apply(db_session, migrations, "pogo")[m
[32m+[m[32m        await migrate.apply(db_session, migrations, schema_name="public")[m
         with pytest.raises(error.BadMigrationError) as e:[m
[31m-            await migrate.rollback(db_session, migrations, "pogo", count=1)[m
[32m+[m[32m            await migrate.rollback(db_session, migrations, schema_name="public", count=1)[m
 [m
         await self.assert_tables([m
             db_session,[m
[31m-            ["_pogo_migration", "_pogo_version", "table_one", "table_three", "table_two"],[m
[32m+[m[32m            ["public._pogo_migration", "public._pogo_version", "public.table_one", "public.table_three", "public.table_two"],[m
         )[m
         assert str(e.value) == "Failed to rollback 20240318_01_12345-broken-rollback"[m
[1mdiff --git a/tests/util/test_sql.py b/tests/util/test_sql.py[m
[1mindex 597bbfa..7e8d0a4 100644[m
[1m--- a/tests/util/test_sql.py[m
[1m+++ b/tests/util/test_sql.py[m
[36m@@ -29,10 +29,13 @@[m [masync def assert_schemas(db_session, schemas):[m
     assert [r["schema_name"] for r in results] == schemas[m
 [m
 [m
[31m-@pytest.mark.nosync[m
[31m-async def test_get_connection_syncs_tables(db_session, monkeypatch):[m
[32m+[m[32m@pytest.fixture(autouse=True)[m
[32m+[m[32mdef connect_patch_(db_session, monkeypatch):[m
     monkeypatch.setattr(sql.asyncpg, "connect", mock.AsyncMock(return_value=db_session))[m
 [m
[32m+[m
[32m+[m[32m@pytest.mark.nosync[m
[32m+[m[32masync def test_get_connection_syncs_tables(db_session):[m
     db = await sql.get_connection(db_session)[m
 [m
     await assert_tables(db_session, ["_pogo_migration", "_pogo_version"])[m
[36m@@ -40,9 +43,7 @@[m [masync def test_get_connection_syncs_tables(db_session, monkeypatch):[m
 [m
 [m
 @pytest.mark.nosync[m
[31m-async def test_get_connection_creates_schema(db_session, monkeypatch):[m
[31m-    monkeypatch.setattr(sql.asyncpg, "connect", mock.AsyncMock(return_value=db_session))[m
[31m-[m
[32m+[m[32masync def test_get_connection_creates_schema(db_session):[m
     db = await sql.get_connection(db_session, schema_name="unit", schema_create=True)[m
 [m
     await assert_schemas(db_session, ["public", "unit"])[m
[36m@@ -53,8 +54,7 @@[m [masync def test_get_connection_creates_schema(db_session, monkeypatch):[m
 @pytest.mark.parametrize("schema", [None, "unit"])[m
 async def test_ensure_pogo_sync_creates_tables(db_session, schema):[m
     if schema:[m
[31m-        await db_session.execute(f"CREATE SCHEMA {schema}")[m
[31m-        await db_session.execute(f"SET search_path = '{schema}'")[m
[32m+[m[32m        db_session = await sql.get_connection("", schema_name=schema, schema_create=True)[m
 [m
     await sql.ensure_pogo_sync(db_session)[m
 [m
[36m@@ -65,8 +65,7 @@[m [masync def test_ensure_pogo_sync_creates_tables(db_session, schema):[m
 @pytest.mark.parametrize("schema", [None, "unit"])[m
 async def test_ensure_pogo_sync_handles_existing_tables(db_session, schema):[m
     if schema:[m
[31m-        await db_session.execute(f"CREATE SCHEMA {schema}")[m
[31m-        await db_session.execute(f"SET search_path = '{schema}'")[m
[32m+[m[32m        db_session = await sql.get_connection("", schema_name=schema, schema_create=True)[m
 [m
     await sql.ensure_pogo_sync(db_session)[m
     await sql.ensure_pogo_sync(db_session)[m
[36m@@ -77,33 +76,31 @@[m [masync def test_ensure_pogo_sync_handles_existing_tables(db_session, schema):[m
 @pytest.mark.parametrize("schema", [None, "unit"])[m
 async def test_migration_applied(db_session, schema):[m
     if schema:[m
[31m-        await db_session.execute(f"CREATE SCHEMA {schema}")[m
[31m-        await db_session.execute(f"SET search_path = '{schema}'")[m
[32m+[m[32m        db_session = await sql.get_connection("", schema_name=schema, schema_create=True)[m
 [m
[31m-    schema = schema or "pogo"[m
[31m-    await sql.migration_applied(db_session, "migration_id", "migration_hash", schema)[m
[32m+[m[32m    schema = schema or "public"[m
[32m+[m[32m    await sql.migration_applied(db_session, "migration_id", "migration_hash", schema_name=schema)[m
 [m
[31m-    ids = await sql.get_applied_migrations(db_session, schema)[m
[32m+[m[32m    ids = await sql.get_applied_migrations(db_session, schema_name=schema)[m
     assert ids == {"migration_id"}[m
 [m
 [m
 @pytest.mark.parametrize("schema", [None, "unit"])[m
 async def test_migration_unapplied(db_session, schema):[m
     if schema:[m
[31m-        await db_session.execute(f"CREATE SCHEMA {schema}")[m
[31m-        await db_session.execute(f"SET search_path = '{schema}'")[m
[32m+[m[32m        db_session = await sql.get_connection("", schema_name=schema, schema_create=True)[m
 [m
[31m-    schema = schema or "pogo"[m
[32m+[m[32m    schema = schema or "public"[m
 [m
[31m-    await sql.migration_applied(db_session, "migration_id", "migration_hash", schema)[m
[31m-    await sql.migration_unapplied(db_session, "migration_id", schema)[m
[32m+[m[32m    await sql.migration_applied(db_session, "migration_id", "migration_hash", schema_name=schema)[m
[32m+[m[32m    await sql.migration_unapplied(db_session, "migration_id", schema_name=schema)[m
 [m
[31m-    ids = await sql.get_applied_migrations(db_session, schema)[m
[32m+[m[32m    ids = await sql.get_applied_migrations(db_session, schema_name=schema)[m
     assert ids == set()[m
 [m
 [m
 async def test_create_schema(db_session):[m
[31m-    await sql.create_schema(db_session, "unit")[m
[32m+[m[32m    await sql.create_schema(db_session, schema_name="unit")[m
 [m
     await assert_schemas(db_session, ["public", "unit"])[m
 [m
[36m@@ -111,6 +108,6 @@[m [masync def test_create_schema(db_session):[m
 async def test_drop_schema(db_session):[m
     await db_session.execute("CREATE SCHEMA unit")[m
 [m
[31m-    await sql.drop_schema(db_session, "unit")[m
[32m+[m[32m    await sql.drop_schema(db_session, schema_name="unit")[m
 [m
     await assert_schemas(db_session, ["public"])[m

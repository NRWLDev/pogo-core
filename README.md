# Pogo migrate core - asyncpg migration tooling
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![image](https://img.shields.io/pypi/v/pogo_core.svg)](https://pypi.org/project/pogo_core/)
[![image](https://img.shields.io/pypi/l/pogo_core.svg)](https://pypi.org/project/pogo_core/)
[![image](https://img.shields.io/pypi/pyversions/pogo_core.svg)](https://pypi.org/project/pogo_core/)
![style](https://github.com/NRWLDev/pogo-core/actions/workflows/style.yml/badge.svg)
![tests](https://github.com/NRWLDev/pogo-core/actions/workflows/tests.yml/badge.svg)
[![codecov](https://codecov.io/gh/NRWLDev/pogo-core/branch/main/graph/badge.svg)](https://codecov.io/gh/NRWLDev/pogo-core)

`pogo-core` is a migration tool intended for use with `asyncpg` and assists
with maintaining your database schema (and data if required) as it evolves.
Pogo supports migrations written in raw sql, as well as python files (useful
when data needs to be migrated).

A migration can be as simple as:

```sql
-- a descriptive message
-- depends: 20210101_01_abcdef-previous-migration

-- migrate: apply
CREATE TABLE foo (id INT, bar VARCHAR(20), PRIMARY KEY (id));

-- migrate: rollback
DROP TABLE foo;
```

Pogo manages these migration scripts and provides utility functions to apply
and rollback migrations.

For a user interface, install `pogo-migrate` instead.

See the [docs](https://nrwldev.github.io/pogo-migrate/) for more details.

## Thanks and Credit

Inspiration for this tool is drawn from
[yoyo](https://ollycope.com/software/yoyo/latest/) and
[dbmate](https://github.com/amacneil/dbmate).

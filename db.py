from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"


@dataclass(frozen=True)
class Migration:
    version: str
    name: str
    sql: str


def connect_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def migration_files() -> list[Migration]:
    migrations: list[Migration] = []
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        version, _, slug = path.stem.partition("_")
        if not version:
            raise ValueError(f"invalid migration filename: {path.name}")
        migrations.append(Migration(version=version, name=slug or path.stem, sql=path.read_text()))
    return migrations


def ensure_migration_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at TEXT NOT NULL
        )
        """
    )


def applied_versions(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT version FROM schema_migrations").fetchall()
    return {row[0] for row in rows}


def existing_tables(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
          AND name NOT LIKE 'sqlite_%'
        """
    ).fetchall()
    return {row[0] for row in rows}


def bootstrap_legacy_schema(conn: sqlite3.Connection) -> None:
    applied = applied_versions(conn)
    if applied:
        return

    tables = existing_tables(conn)
    if {"repositories", "repository_topics"}.issubset(tables):
        record_migration(conn, "001", "initial")

    if "crawls" in tables:
        record_migration(conn, "002", "crawls")

    if {"crawl_runs", "crawl_assets", "crawl_pages", "crawl_findings"}.issubset(tables):
        record_migration(conn, "003", "site_crawl_models")


def record_migration(conn: sqlite3.Connection, version: str, name: str) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO schema_migrations (version, name, applied_at)
        VALUES (?, ?, ?)
        """,
        (version, name, datetime.now(timezone.utc).isoformat()),
    )


def migrate_db(conn: sqlite3.Connection) -> None:
    ensure_migration_table(conn)
    bootstrap_legacy_schema(conn)

    for migration in migration_files():
        if migration.version in applied_versions(conn):
            continue

        with conn:
            conn.executescript(migration.sql)
            record_migration(conn, migration.version, migration.name)

"""
Применяет DDL из clickhouse/init при старте: у существующего volume init-скрипты не выполняются повторно.
Файлы выполняются в алфавитном порядке: 01-schema.sql, 02-alter-*.sql, …
"""

from __future__ import annotations

import logging
from pathlib import Path

from src.clickhouse_client import get_client

log = logging.getLogger(__name__)


def _strip_line_comment(line: str) -> str:
    return line.split("--", 1)[0].rstrip()


def _split_statements(sql: str) -> list[str]:
    statements: list[str] = []
    buf: list[str] = []
    for raw in sql.splitlines():
        line = _strip_line_comment(raw)
        if not line.strip():
            continue
        buf.append(line)
        if line.endswith(";"):
            stmt = "\n".join(buf).strip().rstrip(";").strip()
            buf = []
            if stmt:
                statements.append(stmt)
    if buf:
        stmt = "\n".join(buf).strip().rstrip(";").strip()
        if stmt:
            statements.append(stmt)
    return statements


def ensure_clickhouse_schema() -> None:
    root = Path(__file__).resolve().parent.parent
    init_dir = root / "clickhouse" / "init"
    if not init_dir.is_dir():
        log.warning("ClickHouse init не найден: %s", init_dir)
        return
    ch = get_client()
    total = 0
    for sql_path in sorted(init_dir.glob("*.sql")):
        text = sql_path.read_text(encoding="utf-8")
        stmts = _split_statements(text)
        for stmt in stmts:
            ch.command(stmt)
            total += 1
        if stmts:
            log.info("ClickHouse: %s операций из %s", len(stmts), sql_path.name)
    if total:
        log.info("ClickHouse: всего применено %s DDL-операций", total)

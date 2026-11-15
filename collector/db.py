"""SQLite 持久层。

关键的"不漏对、不重对"由 :func:`commit_pair` 的单事务保证：
只有当两端偏移都成功前推、配对记录落库后，事务才提交；
任何一步失败则整体回滚，下次轮询会从旧偏移重新拉取、重新配对。
"""
from __future__ import annotations

import json
import os
import sqlite3
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id        TEXT PRIMARY KEY,
    base_url      TEXT NOT NULL,
    status        TEXT NOT NULL,            -- running|completed|error
    r1_offset     INTEGER NOT NULL DEFAULT 0,
    r2_offset     INTEGER NOT NULL DEFAULT 0,
    pairs         INTEGER NOT NULL DEFAULT 0,
    error         TEXT,
    created_at    REAL NOT NULL,
    updated_at    REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS pairs (
    run_id     TEXT NOT NULL,
    seq        INTEGER NOT NULL,             -- 配对顺序号，从 0 开始
    name       TEXT NOT NULL,
    r1_record  BLOB NOT NULL,
    r2_record  BLOB NOT NULL,
    PRIMARY KEY (run_id, seq),
    UNIQUE (run_id, name)
);
CREATE TABLE IF NOT EXISTS consensus (
    run_id     TEXT PRIMARY KEY,
    status     TEXT NOT NULL,            -- running|completed|error
    total      INTEGER NOT NULL DEFAULT 0,   -- 触发时运行内的总对数
    merged     INTEGER NOT NULL DEFAULT 0,   -- 成功合成对数
    unmerged   INTEGER NOT NULL DEFAULT 0,   -- 未合成对数
    error      TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
"""


def connect(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


def create_run(conn: sqlite3.Connection, run_id: str, base_url: str, now: float) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO runs(run_id, base_url, status, created_at, updated_at)"
        " VALUES (?, ?, 'running', ?, ?)",
        (run_id, base_url, now, now),
    )
    conn.commit()


def get_run(conn: sqlite3.Connection, run_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()


def list_runs(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute("SELECT * FROM runs ORDER BY created_at").fetchall()


def mark_status(
    conn: sqlite3.Connection, run_id: str, status: str, now: float, error: dict | None = None
) -> None:
    conn.execute(
        "UPDATE runs SET status=?, error=?, updated_at=? WHERE run_id=?",
        (status, json.dumps(error, ensure_ascii=False) if error else None, now, run_id),
    )
    conn.commit()


def commit_pair(
    conn: sqlite3.Connection,
    run_id: str,
    seq: int,
    name: str,
    r1_record: bytes,
    r2_record: bytes,
    r1_offset: int,
    r2_offset: int,
    now: float,
) -> None:
    """单事务写入一条配对并前推两端字节进度。"""
    with conn:  # 自动 BEGIN/COMMIT，异常自动 ROLLBACK
        conn.execute(
            "INSERT INTO pairs(run_id, seq, name, r1_record, r2_record)"
            " VALUES (?, ?, ?, ?, ?)",
            (run_id, seq, name, r1_record, r2_record),
        )
        conn.execute(
            "UPDATE runs SET r1_offset=?, r2_offset=?, pairs=pairs+1, updated_at=?"
            " WHERE run_id=?",
            (r1_offset, r2_offset, now, run_id),
        )


def iter_records(conn: sqlite3.Connection, run_id: str, side: str):
    """按配对顺序流式产出某一端的完整 FASTQ 记录字节。"""
    col = "r1_record" if side == 1 else "r2_record"
    cur = conn.execute(
        f"SELECT {col} AS rec FROM pairs WHERE run_id=? ORDER BY seq", (run_id,)
    )
    for row in cur:
        yield row["rec"]


def snapshot(row: sqlite3.Row) -> dict[str, Any]:
    error = json.loads(row["error"]) if row["error"] else None
    return {
        "run_id": row["run_id"],
        "base_url": row["base_url"],
        "status": row["status"],
        "r1_offset": row["r1_offset"],
        "r2_offset": row["r2_offset"],
        "pairs": row["pairs"],
        "error": error,
    }


# ---------------------------------------------------------------- 共识任务

def consensus_get(conn: sqlite3.Connection, run_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM consensus WHERE run_id=?", (run_id,)
    ).fetchone()


def consensus_reset(conn: sqlite3.Connection, run_id: str, total: int, now: float) -> None:
    """把共识任务（重）置为 running：新建或覆盖旧结果，计数清零。"""
    conn.execute(
        "INSERT INTO consensus(run_id, status, total, merged, unmerged, error,"
        " created_at, updated_at)"
        " VALUES (?, 'running', ?, 0, 0, NULL, ?, ?)"
        " ON CONFLICT(run_id) DO UPDATE SET"
        " status='running', total=excluded.total, merged=0, unmerged=0,"
        " error=NULL, updated_at=excluded.updated_at",
        (run_id, total, now, now),
    )
    conn.commit()


def consensus_progress(
    conn: sqlite3.Connection, run_id: str, merged: int, unmerged: int, now: float
) -> None:
    conn.execute(
        "UPDATE consensus SET merged=?, unmerged=?, updated_at=? WHERE run_id=?",
        (merged, unmerged, now, run_id),
    )
    conn.commit()


def consensus_complete(
    conn: sqlite3.Connection, run_id: str, merged: int, unmerged: int, now: float
) -> None:
    conn.execute(
        "UPDATE consensus SET status='completed', merged=?, unmerged=?,"
        " error=NULL, updated_at=? WHERE run_id=?",
        (merged, unmerged, now, run_id),
    )
    conn.commit()


def consensus_fail(conn: sqlite3.Connection, run_id: str, error: str, now: float) -> None:
    conn.execute(
        "UPDATE consensus SET status='error', error=?, updated_at=? WHERE run_id=?",
        (error, now, run_id),
    )
    conn.commit()


def consensus_drop_running(conn: sqlite3.Connection) -> list[str]:
    """删除所有 running 状态的共识任务（进程重启后清理中断残留）。

    返回被清理的 run_id 列表，调用方据此删除对应的临时文件。
    """
    rows = conn.execute(
        "SELECT run_id FROM consensus WHERE status='running'"
    ).fetchall()
    ids = [r["run_id"] for r in rows]
    if ids:
        conn.execute("DELETE FROM consensus WHERE status='running'")
        conn.commit()
    return ids


def fetch_pairs(
    conn: sqlite3.Connection, run_id: str, min_seq: int, limit: int
) -> list[sqlite3.Row]:
    """按配对顺序取一批配对记录（seq 为主键一部分，范围扫描高效）。"""
    return conn.execute(
        "SELECT seq, name, r1_record, r2_record FROM pairs"
        " WHERE run_id=? AND seq>=? ORDER BY seq LIMIT ?",
        (run_id, min_seq, limit),
    ).fetchall()


def consensus_snapshot(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "run_id": row["run_id"],
        "status": row["status"],
        "total": row["total"],
        "merged": row["merged"],
        "unmerged": row["unmerged"],
        "error": row["error"],
    }

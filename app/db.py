"""SQLite 持久化层：池版本、保留子网、已分配子网与原子提交。

并发模型
========
* 数据库以 WAL 模式打开；
* 每个请求使用独立短连接（check_same_thread=False 仅为线程兼容性）；
* 读取接口（池状态 / 预演）在显式读事务内完成“版本 + 子网”两次查询，
  二者始终来自同一 SQLite 快照，并发提交不可能插入两次查询之间；
* 提交接口在单条 ``BEGIN IMMEDIATE`` 事务内完成“版本检查 + 方案可用性
  复检 + 插入 + 版本自增”，并在提交前于同一事务内读回版本与分配结果，
  响应只绑定本次写事务；任何一步失败即整体回滚，绝不部分分配。
"""
from __future__ import annotations

import ipaddress
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass

from .config import Settings

_SCHEMA = """
CREATE TABLE IF NOT EXISTS pool_meta (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    parent_cidr TEXT NOT NULL,
    version    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS subnets (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    cidr       TEXT NOT NULL UNIQUE,
    kind       TEXT NOT NULL CHECK (kind IN ('reserved', 'allocated')),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
"""


@dataclass(frozen=True)
class PoolState:
    parent: ipaddress._BaseNetwork
    version: int
    reserved: tuple[ipaddress._BaseNetwork, ...]
    allocated: tuple[ipaddress._BaseNetwork, ...]


class ConflictError(Exception):
    """提交冲突：池版本已变化，或方案在当前池状态下不再可用。"""


def _state_from_rows(meta: sqlite3.Row, rows: list[sqlite3.Row]) -> PoolState:
    """由同一事务内读出的 pool_meta 行与 subnets 行构建池状态。"""
    parent = ipaddress.ip_network(meta["parent_cidr"], strict=True)
    reserved = tuple(
        ipaddress.ip_network(r["cidr"], strict=True)
        for r in rows
        if r["kind"] == "reserved"
    )
    allocated = tuple(
        ipaddress.ip_network(r["cidr"], strict=True)
        for r in rows
        if r["kind"] == "allocated"
    )
    return PoolState(parent, meta["version"], reserved, allocated)


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


class Database:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._db_path = settings.db_path
        self._init_lock = threading.Lock()
        self.initialize()

    @contextmanager
    def _conn(self):
        conn = _connect(self._db_path)
        try:
            yield conn
        finally:
            conn.close()

    def initialize(self) -> None:
        """建表并在首次启动时写入父网段与保留子网（幂等）。"""
        import os

        directory = os.path.dirname(self._db_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with self._init_lock, self._conn() as conn:
            conn.executescript(_SCHEMA)
            row = conn.execute(
                "SELECT parent_cidr FROM pool_meta WHERE id = 1"
            ).fetchone()
            if row is None:
                parent = self._settings.parent
                reserved = self._settings.reserved
                conn.execute(
                    "INSERT INTO pool_meta (id, parent_cidr, version) "
                    "VALUES (1, ?, 1)",
                    (str(parent),),
                )
                conn.executemany(
                    "INSERT OR IGNORE INTO subnets (cidr, kind) VALUES (?, 'reserved')",
                    [(str(net),) for net in reserved],
                )
            conn.commit()

    # -- 读取 --------------------------------------------------------------

    def get_state(self) -> PoolState:
        with self._conn() as conn:
            return self._read_state(conn)

    @staticmethod
    def _read_state(conn: sqlite3.Connection) -> PoolState:
        """在单个显式读事务（同一 SQLite 快照）内读取版本与子网。

        若不开启显式事务，每条 SELECT 各自独立快照，并发提交可能插入
        两次查询之间，导致“旧版本 + 新占用集”的错位状态。
        """
        conn.execute("BEGIN")
        try:
            meta = conn.execute(
                "SELECT parent_cidr, version FROM pool_meta WHERE id = 1"
            ).fetchone()
            rows = conn.execute(
                "SELECT cidr, kind FROM subnets ORDER BY cidr"
            ).fetchall()
        finally:
            # 只读事务：回滚即释放快照，与 COMMIT 等价且不会产生写。
            conn.rollback()
        return _state_from_rows(meta, rows)

    # -- 原子提交 ----------------------------------------------------------

    def commit_plan(
        self,
        expected_version: int,
        networks: tuple[ipaddress._BaseNetwork, ...],
    ) -> PoolState:
        """在版本未变且方案仍可用时原子落库，否则抛 ConflictError。"""
        cidr_strs = [str(net) for net in networks]
        with self._conn() as conn:
            try:
                # 立即获取写锁，与并发提交互斥。
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT parent_cidr, version FROM pool_meta WHERE id = 1"
                ).fetchone()
                current_version = row["version"]
                if current_version != expected_version:
                    conn.rollback()
                    raise ConflictError(
                        f"pool version changed: expected {expected_version}, "
                        f"current {current_version}"
                    )

                existing = {
                    r["cidr"]
                    for r in conn.execute(
                        "SELECT cidr FROM subnets"
                    ).fetchall()
                }
                collisions = [c for c in cidr_strs if c in existing]
                if collisions:
                    conn.rollback()
                    raise ConflictError(
                        f"allocation no longer available, conflicts: {collisions}"
                    )

                # 复检方案内部互不重叠（预演结果天然满足，防御性校验）。
                _assert_disjoint(networks)
                # 复检方案与父网段及所有现存子网（保留 + 已分配）相容。
                parent = ipaddress.ip_network(row["parent_cidr"], strict=True)
                occupied = [
                    ipaddress.ip_network(c, strict=True) for c in existing
                ]
                for net in networks:
                    if net.version != parent.version or not net.subnet_of(parent):
                        conn.rollback()
                        raise ConflictError(
                            f"allocation {net} outside of pool {parent}"
                        )
                    if any(net.overlaps(other) for other in occupied):
                        conn.rollback()
                        raise ConflictError(
                            f"allocation {net} overlaps existing subnet"
                        )
                    occupied.append(net)

                conn.executemany(
                    "INSERT INTO subnets (cidr, kind) VALUES (?, 'allocated')",
                    [(c,) for c in cidr_strs],
                )
                conn.execute(
                    "UPDATE pool_meta SET version = version + 1 WHERE id = 1"
                )
                # 关键：在同一写事务内读回版本与子网。写锁保证此刻不会有
                # 其它提交插入，响应严格绑定本次事务的分配结果。
                meta = conn.execute(
                    "SELECT parent_cidr, version FROM pool_meta WHERE id = 1"
                ).fetchone()
                rows = conn.execute(
                    "SELECT cidr, kind FROM subnets ORDER BY cidr"
                ).fetchall()
                conn.commit()
            except ConflictError:
                raise
            except sqlite3.Error as exc:
                conn.rollback()
                raise ConflictError(f"commit failed: {exc}") from exc
        return _state_from_rows(meta, rows)


def _assert_disjoint(networks: tuple[ipaddress._BaseNetwork, ...]) -> None:
    ordered = sorted(networks, key=lambda n: int(n.network_address))
    for a, b in zip(ordered, ordered[1:]):
        if a.overlaps(b):
            raise ConflictError(f"plan contains overlapping subnets {a}, {b}")

"""SQLite 持久化层：池版本、保留子网、已分配子网与原子提交。

并发模型
========
* 数据库以 WAL 模式打开；
* 每个请求使用独立短连接（check_same_thread=False 仅为线程兼容性）；
* 读取（池状态 / 预演）在单条 ``BEGIN`` 读事务内读取版本与子网，
  整个查询基于同一 SQLite 快照，不会拼出“旧版本 + 新占用集”的
  错位状态；
* 提交接口在单条 ``BEGIN IMMEDIATE`` 事务内完成“版本检查 + 方案可用性
  复检 + 插入 + 版本自增”，任何一步失败即整体回滚，绝不部分分配；
  提交响应使用的版本与分配结果同样取自该写事务内部，不会混入其它
  并发提交之后的状态。
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


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


class Database:
    # --- 测试钩子（生产环境默认为空操作）--------------------------------
    # 并发回归测试通过替换这两个钩子来精确控制读写事务的交错时刻：
    # _after_meta_select: 读事务内版本查询完成、子网查询开始之前；
    # _after_commit:      写事务 COMMIT 完成之后（结果状态已在提交前
    #                     于同一事务内读取，不受交错影响）。
    _after_meta_select = staticmethod(lambda conn: None)
    _after_commit = staticmethod(lambda conn: None)

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

    @staticmethod
    def _load_state(
        conn: sqlite3.Connection, between_reads=None
    ) -> PoolState:
        """在**当前事务的快照**内读取版本与全部子网并组装 PoolState。

        调用方必须已开启事务（``BEGIN`` / ``BEGIN IMMEDIATE``），保证
        版本与子网来自同一快照、写入对自身连接可见。``between_reads``
        为测试钩子，在两次查询之间调用。
        """
        meta = conn.execute(
            "SELECT parent_cidr, version FROM pool_meta WHERE id = 1"
        ).fetchone()
        if between_reads is not None:
            between_reads(conn)
        rows = conn.execute(
            "SELECT cidr, kind FROM subnets ORDER BY cidr"
        ).fetchall()
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

    def get_state(self) -> PoolState:
        """读取池状态。

        版本与子网在同一条读事务内获取：SQLite WAL 下 ``BEGIN`` 后
        事务固定在开启时的快照，两条查询之间即使有并发提交落库，也
        不会返回“旧版本配新占用集”的错位结果。
        """
        with self._conn() as conn:
            conn.execute("BEGIN")
            state = self._load_state(conn, self._after_meta_select)
            conn.commit()
        return state

    # -- 原子提交 ----------------------------------------------------------

    def commit_plan(
        self,
        expected_version: int,
        networks: tuple[ipaddress._BaseNetwork, ...],
    ) -> PoolState:
        """在版本未变且方案仍可用时原子落库，否则抛 ConflictError。

        返回的 PoolState 在**本次写事务内部**、插入与版本自增之后读取，
        因此响应中的版本、占用集严格对应本次提交；不会在提交后另开
        连接而读到其它并发提交的结果。
        """
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
                # 在同一写事务内读取提交后的状态：版本与占用集必然对应
                # 本次写入（其它提交被写锁挡在事务之外）。
                state = self._load_state(conn)
                conn.commit()
                # 测试钩子：COMMIT 完成之后。修复前的实现会在此刻另开
                # 连接读取状态，另一笔提交可能恰好落库；修复后结果已在
                # 本事务内确定，钩子不再能造成版本/内容错位。
                self._after_commit(conn)
            except ConflictError:
                raise
            except sqlite3.Error as exc:
                conn.rollback()
                raise ConflictError(f"commit failed: {exc}") from exc
        return state


def _assert_disjoint(networks: tuple[ipaddress._BaseNetwork, ...]) -> None:
    ordered = sorted(networks, key=lambda n: int(n.network_address))
    for a, b in zip(ordered, ordered[1:]):
        if a.overlaps(b):
            raise ConflictError(f"plan contains overlapping subnets {a}, {b}")

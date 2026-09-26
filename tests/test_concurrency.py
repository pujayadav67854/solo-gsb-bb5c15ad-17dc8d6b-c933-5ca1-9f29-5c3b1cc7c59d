"""并发交错回归测试：读快照一致性与提交响应的事务绑定。

通过“连接代理 + 一次性语句钩子”在精确受控的时机插入另一笔并发提交：

* 在 ``get_state`` 读版本与读子网两次查询之间插入提交 —— 读事务必须
  仍返回插入前的同一快照（版本与子网不错位）；
* 在 ``commit_plan`` 写事务落库之后插入提交 —— 响应必须仍绑定本次
  写事务自己的版本与分配结果。

另附一个多线程压测：并发“预演 -> 提交”全程校验快照不变式。
"""
import ipaddress
import os
import sqlite3
import tempfile
import threading

import app.db as db_mod
from app.allocator import CidrAllocationSolver
from app.config import Settings
from app.db import ConflictError, Database


# ---------------------------------------------------------------------------
# 连接代理：在 execute / commit 之后触发一次性钩子，用于制造受控交错
# ---------------------------------------------------------------------------

class ConnectionProxy:
    """包装 sqlite3.Connection，把钩子挂在指定语句事件之后。"""

    def __init__(self, real: sqlite3.Connection, hooks: list) -> None:
        object.__setattr__(self, "_real", real)
        object.__setattr__(self, "_hooks", hooks)

    def __getattr__(self, name):
        return getattr(self._real, name)

    def __setattr__(self, name, value):
        setattr(self._real, name, value)

    def execute(self, sql, parameters=()):
        cur = self._real.execute(sql, parameters)
        self._fire("execute", sql)
        return cur

    def commit(self):
        self._real.commit()
        self._fire("commit", None)

    def _fire(self, event, sql):
        for hook in list(self._hooks):
            hook(event, sql)


def _install_proxy(hooks: list):
    """猴子补丁 app.db._connect，使新连接均被代理；返回原函数。"""
    real_connect = db_mod._connect

    def factory(db_path: str):
        return ConnectionProxy(real_connect(db_path), hooks)

    db_mod._connect = factory
    return real_connect


def _make_db(name: str, parent: str = "10.30.0.0/16") -> Database:
    path = os.path.join(tempfile.mkdtemp(), name)
    return Database(
        Settings(db_path=path, parent_cidr=parent, reserved_cidrs_raw="")
    )


# ---------------------------------------------------------------------------
# 回归 1：预演 / 池状态读取的版本与子网必须来自同一快照
# ---------------------------------------------------------------------------

def test_get_state_reads_single_snapshot():
    db = _make_db("snapshot.db")
    net = ipaddress.ip_network("10.30.0.0/24")

    hooks: list = []
    real_connect = _install_proxy(hooks)
    fired = []

    def inject_commit_between_selects(event, sql):
        # 在“读版本”之后、“读子网”之前，从另一连接完成一笔并发提交。
        if event == "execute" and "FROM pool_meta" in sql:
            hooks.remove(inject_commit_between_selects)  # 一次性，防递归
            db.commit_plan(1, (net,))  # version 1 -> 2，并占用 net
            fired.append(True)

    hooks.append(inject_commit_between_selects)
    try:
        state = db.get_state()
    finally:
        db_mod._connect = real_connect

    assert fired, "钩子未触发，测试未覆盖目标交错"
    # 读事务快照：版本与子网必须同属一个快照（此处为并发提交前的 v1）。
    assert state.version == 1, state
    assert net not in state.allocated, state
    # 干扰提交本身已生效：无干扰的后续读取应看到 v2 且包含 net。
    after = db.get_state()
    assert after.version == 2 and net in after.allocated
    print("snapshot isolation between SELECTs ok")


# ---------------------------------------------------------------------------
# 回归 2：提交响应必须绑定本次写事务内的版本与分配结果
# ---------------------------------------------------------------------------

def test_commit_response_bound_to_own_transaction():
    db = _make_db("commit_binding.db")
    net_a = ipaddress.ip_network("10.30.1.0/24")
    net_b = ipaddress.ip_network("10.30.2.0/24")

    hooks: list = []
    real_connect = _install_proxy(hooks)
    fired = []

    def inject_commit_after_write(event, sql):
        # A 的写事务落库之后、响应定型之前：另一笔提交 B 抢先完成。
        if event == "commit":
            hooks.remove(inject_commit_after_write)  # 一次性，防递归
            db.commit_plan(2, (net_b,))  # version 2 -> 3
            fired.append(True)

    hooks.append(inject_commit_after_write)
    try:
        state_a = db.commit_plan(1, (net_a,))  # A：version 1 -> 2
    finally:
        db_mod._connect = real_connect

    assert fired, "钩子未触发，测试未覆盖目标交错"
    # 响应必须绑定 A 自己的写事务：版本 2，且不含 B 的分配。
    assert state_a.version == 2, state_a
    assert net_a in state_a.allocated
    assert net_b not in state_a.allocated, state_a
    # 两笔提交最终都已落库。
    final = db.get_state()
    assert final.version == 3
    assert net_a in final.allocated and net_b in final.allocated
    print("commit response bound to own write transaction ok")


# ---------------------------------------------------------------------------
# 压测：多线程并发“预演 -> 提交”，全程校验快照与响应不变式
# ---------------------------------------------------------------------------

def test_concurrent_preview_commit_invariants():
    db = _make_db("stress.db", parent="10.40.0.0/16")
    threads_n, commits_per_thread = 8, 5
    total = threads_n * commits_per_thread
    errors: list[str] = []
    returned_versions: list[int] = []
    lock = threading.Lock()

    def worker():
        done = 0
        while done < commits_per_thread:
            state = db.get_state()
            # 快照不变式：v1 起步，每笔提交恰好 +1 版本、+1 个子网。
            if len(state.allocated) != state.version - 1:
                errors.append(
                    f"torn read: version={state.version} "
                    f"allocated={len(state.allocated)}"
                )
                return
            occupied = list(state.reserved) + list(state.allocated)
            plan = CidrAllocationSolver(state.parent, occupied, (28,)).solve()
            try:
                committed = db.commit_plan(state.version, plan.networks)
            except ConflictError:
                continue  # 版本冲突：重新预演后重试
            # 响应不变式：绑定本次事务，且自身仍是同一快照。
            if (
                plan.networks[0] not in committed.allocated
                or len(committed.allocated) != committed.version - 1
            ):
                errors.append(f"unbound commit response: {committed}")
                return
            with lock:
                returned_versions.append(committed.version)
            done += 1

    threads = [threading.Thread(target=worker) for _ in range(threads_n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, errors
    # 每笔成功提交的响应版本必须互不重复且恰好覆盖 2..total+1。
    assert sorted(returned_versions) == list(range(2, total + 2))
    final = db.get_state()
    assert final.version == total + 1
    assert len(final.allocated) == total
    print(f"concurrent stress ok: {total} commits, version {final.version}")


def main():
    test_get_state_reads_single_snapshot()
    test_commit_response_bound_to_own_transaction()
    test_concurrent_preview_commit_invariants()
    print("ALL CONCURRENCY TESTS PASSED")


if __name__ == "__main__":
    main()

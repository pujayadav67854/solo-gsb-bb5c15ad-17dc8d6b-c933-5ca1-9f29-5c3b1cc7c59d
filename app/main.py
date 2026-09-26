"""FastAPI 接口：池状态查询、预演与原子提交。"""
from __future__ import annotations

import ipaddress
from contextlib import asynccontextmanager
from typing import Literal

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field, field_validator

from .allocator import CidrAllocationSolver, InfeasibleError
from .config import settings
from .db import ConflictError, Database, PoolState

_db: Database | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 容器启动即完成建表与初始地址池写入（幂等）。
    get_db()
    yield


app = FastAPI(
    title="内网 CIDR 批量分配 API",
    version="1.0.0",
    lifespan=lifespan,
    description=(
        "在“父网段 + 保留子网”的地址池内批量预演并原子提交对齐子网分配。"
        "优化目标：先最小化分配后剩余空闲 CIDR 数量，再取按请求顺序的网络"
        "地址序列字典序最小者。"
    ),
)


def get_db() -> Database:
    global _db
    if _db is None:
        _db = Database(settings)
    return _db


# ---------------------------------------------------------------------------
# 请求 / 响应模型
# ---------------------------------------------------------------------------

class PreviewRequest(BaseModel):
    prefix_lengths: list[int] = Field(
        ...,
        min_length=1,
        max_length=8,
        description="按请求顺序排列的前缀长度，1～8 个。",
    )

    @field_validator("prefix_lengths")
    @classmethod
    def _validate_prefixes(cls, values: list[int]) -> list[int]:
        for p in values:
            if isinstance(p, bool) or not isinstance(p, int) or p < 0 or p > 128:
                raise ValueError("prefix length must be an integer in [0, 128]")
        return values


class CommitRequest(BaseModel):
    pool_version: int = Field(..., ge=1, description="预演返回的池版本号。")
    allocations: list[str] = Field(
        ...,
        min_length=1,
        max_length=8,
        description="预演返回的方案 CIDR，顺序与请求一致。",
    )

    @field_validator("allocations")
    @classmethod
    def _validate_cidrs(cls, values: list[str]) -> list[str]:
        # strict=True：主机位非零的非对齐写法直接报错。
        parsed: list[ipaddress._BaseNetwork] = [
            ipaddress.ip_network(raw, strict=True) for raw in values
        ]
        versions = {n.version for n in parsed}
        if len(versions) > 1:
            raise ValueError("allocations must all be IPv4 or all be IPv6")
        ordered = sorted(parsed, key=lambda n: int(n.network_address))
        for a, b in zip(ordered, ordered[1:]):
            if a.overlaps(b):
                raise ValueError("allocations contain overlapping CIDRs")
        return [str(n) for n in parsed]


class PoolInfo(BaseModel):
    parent_cidr: str
    version: int
    reserved_cidrs: list[str]
    allocated_cidrs: list[str]
    free_cidrs: list[str]
    free_cidr_count: int


class PreviewResponse(BaseModel):
    pool_version: int
    feasible: bool
    allocations: list[str]
    free_cidr_count_after: int
    free_cidrs_after: list[str]
    reason: str | None = None


class CommitResponse(BaseModel):
    status: Literal["committed"]
    pool_version: int
    allocations: list[str]
    free_cidrs: list[str]


# ---------------------------------------------------------------------------
# 辅助
# ---------------------------------------------------------------------------

def _free_cidrs(state: PoolState) -> tuple[list[ipaddress._BaseNetwork], int]:
    reserved_all = list(state.reserved) + list(state.allocated)
    solver = CidrAllocationSolver(state.parent, reserved_all, ())
    return solver.free_cidrs, len(solver.free_cidrs)


def _pool_info(state: PoolState) -> PoolInfo:
    free, count = _free_cidrs(state)
    return PoolInfo(
        parent_cidr=str(state.parent),
        version=state.version,
        reserved_cidrs=[str(n) for n in state.reserved],
        allocated_cidrs=[str(n) for n in state.allocated],
        free_cidrs=[str(n) for n in free],
        free_cidr_count=count,
    )


# ---------------------------------------------------------------------------
# 路由
# ---------------------------------------------------------------------------

@app.get("/healthz", tags=["meta"])
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/pool", response_model=PoolInfo, tags=["pool"])
def get_pool(db: Database = Depends(get_db)) -> PoolInfo:
    """返回当前地址池配置、版本与剩余空闲 CIDR。"""
    return _pool_info(db.get_state())


@app.post("/api/allocations/preview", response_model=PreviewResponse, tags=["allocations"])
def preview(
    body: PreviewRequest, db: Database = Depends(get_db)
) -> PreviewResponse:
    """预演：不修改任何状态，返回池版本与最优可行方案。"""
    state = db.get_state()
    occupied = list(state.reserved) + list(state.allocated)
    try:
        plan = CidrAllocationSolver(
            state.parent, occupied, tuple(body.prefix_lengths)
        ).solve()
    except InfeasibleError as exc:
        return PreviewResponse(
            pool_version=state.version,
            feasible=False,
            allocations=[],
            free_cidr_count_after=0,
            free_cidrs_after=[],
            reason=str(exc),
        )
    return PreviewResponse(
        pool_version=state.version,
        feasible=True,
        allocations=[str(n) for n in plan.networks],
        free_cidr_count_after=len(plan.free_cidrs_after),
        free_cidrs_after=[str(n) for n in plan.free_cidrs_after],
    )


@app.post(
    "/api/allocations/commit",
    response_model=CommitResponse,
    tags=["allocations"],
)
def commit(
    body: CommitRequest, db: Database = Depends(get_db)
) -> CommitResponse:
    """提交：仅当版本未变化且方案仍可用时原子落库，否则 409 冲突。"""
    networks = tuple(
        ipaddress.ip_network(c, strict=True) for c in body.allocations
    )
    try:
        state = db.commit_plan(body.pool_version, networks)
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    free, _ = _free_cidrs(state)
    return CommitResponse(
        status="committed",
        pool_version=state.version,
        allocations=[str(n) for n in networks],
        free_cidrs=[str(n) for n in free],
    )

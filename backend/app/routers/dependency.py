"""依赖解析接口：维护依赖关系，覆盖提交关系评审、生效关系、解除关系等动作。

受影响依赖清单：与ETL作业计算共用同一份受影响依赖数据快照，
两侧采用同一批次号取数；确认后的受影响依赖不会被后续重算回退。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.changeimpact import change_impact_service
from app.services.dependency import DependencyService

router = APIRouter(prefix="/api/dependency", tags=["依赖解析"])

service = DependencyService()
impact_service = change_impact_service

LIST_FIELDS = ["关系编号", "上游作业", "下游作业", "依赖类型", "偏移量分钟", "触发条件", "等待超时", "跳过标志"]
STATUSES = ["草稿", "关系评审中", "已生效", "已解除"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按关系编号检索"),
    status: str | None = Query(default=None, description="草稿、关系评审中、已生效、已解除"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按关系编号与状态过滤依赖解析列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


# ---------- 受影响依赖清单（与ETL作业计算共用同一批次号取数） ----------

@router.get("/impact-list", response_model=PageResult[dict])
def list_impact(
    batch_no: str | None = Query(default=None, description="按批次编号过滤受影响依赖快照"),
    review: str | None = Query(default=None, description="待复核、已确认"),
    keyword: str | None = Query(default=None, description="按关系编号检索"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """依赖解析查询侧取数：受影响依赖数据快照，含变更前后表字段与作业版本。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = impact_service.list_snapshots(
        batch_no=batch_no, review=review, relation_no=keyword, page=page, size=size
    )
    return PageResult(items=items, total=total, page=page, size=size)


@router.post("/impact-list/{snapshot_id}/confirm", response_model=ActionResult)
def confirm_impact(snapshot_id: int) -> ActionResult:
    """确认一条受影响依赖；已确认记录不会被后续批次重算回退。"""
    row, message = impact_service.confirm_snapshot(snapshot_id)
    if row is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=row)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出依赖解析清单：返回当前过滤条件下的全量数据。

    注册在 /{entry_id} 之前，否则 "export" 会被当作 entry_id 解析而报 422。
    """
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "dependency", "total": total, "items": items}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条依赖关系明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"依赖关系 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条依赖关系，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="依赖关系已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条依赖关系执行提交关系评审、生效关系、解除关系；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)

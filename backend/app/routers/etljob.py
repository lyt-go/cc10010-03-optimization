"""ETL作业接口：维护作业定义，覆盖提交作业评审、发布作业、下线作业等动作。

变更批次与影响重算：从ETL作业变更批次按实际关联字段重算影响范围，
生成只包含真实受影响对象的变更导出文件，支持下载、打包与超时重试。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.changeimpact import change_impact_service
from app.services.etljob import EtljobService

router = APIRouter(prefix="/api/etljob", tags=["ETL作业"])

service = EtljobService()
impact_service = change_impact_service

LIST_FIELDS = ["作业编号", "作业名称", "源端数据表", "目标数据表", "执行SQL", "调度表达式", "超时分钟", "失败重试次数"]
STATUSES = ["草稿", "作业评审中", "已发布", "已下线"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按作业编号检索"),
    status: str | None = Query(default=None, description="草稿、作业评审中、已发布、已下线"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按作业编号与状态过滤ETL作业列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


# ---------- 变更批次与影响重算（放在 /{entry_id} 之前注册） ----------

@router.get("/change-batches", response_model=PageResult[dict])
def list_change_batches(
    keyword: str | None = Query(default=None, description="按批次编号检索"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """变更批次列表：附受影响依赖数与待复核数。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = impact_service.list_batches(keyword=keyword, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.post("/change-batches", response_model=ActionResult)
def submit_change_batch(payload: EntryPayload) -> ActionResult:
    """接收ETL作业变更批次并按实际关联字段重算影响范围。

    相同批次反复提交幂等跳过；批次写入中断后再次提交只补写未完成对象；
    较晚到达的旧批次携带过期表字段时标记为已过期，不影响当前影响清单。
    """
    result, message = impact_service.submit_batch(payload.values)
    if result is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=result)


@router.get("/change-batches/{batch_no}", response_model=dict)
def get_change_batch(batch_no: str) -> dict:
    """批次明细：变更项、导出文件与受影响依赖统计。"""
    detail = impact_service.get_batch(batch_no)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"变更批次 {batch_no} 不存在")
    return detail


@router.post("/change-batches/{batch_no}/recalc", response_model=ActionResult)
def recalc_change_batch(batch_no: str) -> ActionResult:
    """按批次内已写入变更项重算影响范围，只保留真实受影响对象。"""
    stats, message = impact_service.recalc_batch(batch_no)
    if stats is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=stats)


@router.get("/change-batches/{batch_no}/impact", response_model=PageResult[dict])
def batch_impact_list(
    batch_no: str,
    page: int = 1,
    size: int = 200,
) -> PageResult[dict]:
    """ETL作业计算侧取数：按批次号读取受影响依赖数据快照。"""
    if impact_service.get_batch(batch_no) is None:
        raise HTTPException(status_code=404, detail=f"变更批次 {batch_no} 不存在")
    items, total = impact_service.list_snapshots(batch_no=batch_no, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.post("/change-batches/{batch_no}/export", response_model=ActionResult)
def generate_export(batch_no: str) -> ActionResult:
    """生成变更导出文件（CSV清单 + ZIP打包），内容与影响清单逐条对应。"""
    record, message = impact_service.generate_export(batch_no)
    if record is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=record)


@router.get("/change-batches/{batch_no}/export/download")
def download_export(batch_no: str) -> FileResponse:
    """下载变更导出CSV；文件缺失时按快照重新生成，下载超时可直接重试。"""
    path, message = impact_service.ensure_file(batch_no, "csv")
    if path is None:
        raise HTTPException(status_code=404, detail=message)
    return FileResponse(path, media_type="text/csv; charset=utf-8", filename=path.name)


@router.get("/change-batches/{batch_no}/export/package")
def download_package(batch_no: str) -> FileResponse:
    """打包下载：ZIP内含变更导出CSV与清单manifest，支持超时重试。"""
    path, message = impact_service.ensure_file(batch_no, "zip")
    if path is None:
        raise HTTPException(status_code=404, detail=message)
    return FileResponse(path, media_type="application/zip", filename=path.name)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出ETL作业清单：返回当前过滤条件下的全量数据（含作业版本）。

    注册在 /{entry_id} 之前，否则 "export" 会被当作 entry_id 解析而报 422。
    """
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "etljob", "total": total, "items": items}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条作业定义明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"作业定义 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条作业定义，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="作业定义已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条作业定义执行提交作业评审、发布作业、下线作业；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)

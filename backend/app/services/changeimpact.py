"""ETL作业变更影响重算：变更批次接收、冲突裁决、受影响依赖快照与变更导出文件。

设计要点（选型）：
- 受影响依赖数据快照只存一份（impactsnapshot 表），依赖解析查询与ETL作业计算
  两处一起取数，跨模块两侧用同一批次号生成各自结果，避免两边各算一遍口径漂移。
- 快照行按（批次编号, 关系编号）唯一：反复提交相同变更批次只生成一次待复核记录；
  批次写入中断后再次提交只补写未完成对象，已确认的受影响依赖不回退。
- 同一作业连续多次变更拆在多个批次时按轮次序号裁决：较晚到达的旧批次携带过期
  表字段，标记为已过期不参与重算；影响清单保留轮次较新的一轮表字段，下游重算
  范围以新轮次为准，同时承接既有历史依赖记录的复核状态。
- 兼容历史数据：既有依赖记录没有快照也能正常查询；历史快照行（如 HIST 批次）
  在重算时作为承接来源保留其复核状态。
"""
from __future__ import annotations

import csv
import hashlib
import json
import re
import threading
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any

from app.store import store

BATCH_MODULE = "changebatch"
ITEM_MODULE = "changeitem"
SNAPSHOT_MODULE = "impactsnapshot"
FILE_MODULE = "changefile"
ETLJOB_MODULE = "etljob"
DEPENDENCY_MODULE = "dependency"

REQUIRED_ITEM_FIELDS = ["作业编号", "变更后源端数据表", "变更后目标数据表", "变更后作业版本"]

# 变更导出文件的列：与受影响依赖清单一一对应，文件中的作业版本与清单条目逐条对应
SNAPSHOT_COLUMNS = [
    "批次编号", "关系编号", "上游作业", "下游作业", "命中方向",
    "作业编号", "作业名称", "变更前源端数据表", "变更后源端数据表",
    "变更前目标数据表", "变更后目标数据表", "作业版本",
    "复核状态", "来源", "承接批次", "生成时间",
]

EXPORT_DIR = Path(__file__).resolve().parents[2] / "exports"

_lock = threading.Lock()


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _text(value: Any) -> str:
    return str(value or "").strip()


def _fingerprint(batch_no: str, items: list[dict[str, Any]]) -> str:
    canonical = json.dumps(
        {"批次编号": batch_no, "items": items},
        ensure_ascii=False,
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def _safe_name(batch_no: str) -> str:
    return re.sub(r"[^0-9A-Za-z_-]+", "_", batch_no) or "batch"


def _next_id(module: str) -> int:
    return max((int(row.get("id", 0)) for row in store.rows(module)), default=0) + 1


class ChangeImpactService:
    """变更批次、影响重算与快照导出的业务规则。"""

    # ---------- 批次接收（幂等 + 断点补写） ----------
    def submit_batch(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        batch_no = _text(values.get("批次编号"))
        if not batch_no:
            return None, "缺少必填字段：批次编号"
        try:
            round_no = int(values.get("轮次序号") or 0)
        except (TypeError, ValueError):
            return None, "轮次序号必须是整数"
        raw_items = values.get("items")
        if not isinstance(raw_items, list) or not raw_items:
            return None, "变更批次至少需要一个变更项（items）"
        items: list[dict[str, Any]] = []
        for index, raw in enumerate(raw_items, start=1):
            if not isinstance(raw, dict):
                return None, f"第 {index} 个变更项格式不正确，应为对象"
            missing = [field for field in REQUIRED_ITEM_FIELDS if not _text(raw.get(field))]
            if missing:
                return None, f"第 {index} 个变更项缺少必填字段：{'、'.join(missing)}"
            item = {
                field: _text(raw.get(field))
                for field in (
                    "作业编号", "作业名称",
                    "变更前源端数据表", "变更后源端数据表",
                    "变更前目标数据表", "变更后目标数据表",
                    "变更前作业版本", "变更后作业版本",
                )
            }
            try:
                item["轮次序号"] = int(raw.get("轮次序号") or round_no)
            except (TypeError, ValueError):
                return None, f"第 {index} 个变更项的轮次序号必须是整数"
            items.append(item)
        cut_raw = values.get("中断位置")
        try:
            cut = int(cut_raw) if cut_raw is not None and str(cut_raw).strip() != "" else None
        except (TypeError, ValueError):
            return None, "中断位置必须是整数"
        remark = _text(values.get("备注"))

        with _lock:
            fingerprint = _fingerprint(batch_no, items)
            batch = self._find_batch(batch_no)
            resumed = False
            if batch is not None:
                if batch.get("指纹") == fingerprint and batch.get("状态") != "部分写入":
                    result = {"batch": dict(batch), "重复提交": True, "新增变更项": 0}
                    result.update(self._snapshot_stats(batch_no))
                    return result, "相同变更批次已存在，本次提交幂等跳过，未重复生成待复核记录"
                if batch.get("状态") != "部分写入":
                    return None, f"变更批次 {batch_no} 已存在且内容不一致，请更换批次编号"
                resumed = True
            else:
                batch = {
                    "id": _next_id(BATCH_MODULE),
                    "批次编号": batch_no,
                    "轮次序号": round_no,
                    "到达序号": self._next_arrival(),
                    "状态": "部分写入",
                    "变更项数": 0,
                    "指纹": fingerprint,
                    "提交时间": _now(),
                    "备注": remark,
                }
                store.rows(BATCH_MODULE).append(batch)

            written_keys = {
                (_text(row.get("作业编号")), int(row.get("轮次序号", 0) or 0))
                for row in store.rows(ITEM_MODULE)
                if row.get("批次编号") == batch_no
            }
            queue = [
                item for item in items
                if (item["作业编号"], item["轮次序号"]) not in written_keys
            ]
            interrupted = cut is not None and 0 <= cut < len(queue)
            if interrupted:
                queue = queue[:cut]

            applied = expired = 0
            for item in queue:
                status = self._apply_item(batch_no, item)
                if status == "已写入":
                    applied += 1
                else:
                    expired += 1

            batch["变更项数"] = sum(
                1 for row in store.rows(ITEM_MODULE) if row.get("批次编号") == batch_no
            )
            batch["状态"] = "部分写入" if interrupted else "重算完成"
            if not interrupted:
                batch["指纹"] = fingerprint
            stats = self._recalc_locked(batch_no)

            result = {
                "batch": dict(batch),
                "重复提交": False,
                "断点补写": resumed,
                "新增变更项": len(queue),
                "已应用": applied,
                "已过期": expired,
            }
            result.update(stats)
            result.update(self._snapshot_stats(batch_no))
            if interrupted:
                message = f"批次写入在 {cut} 个变更项后中断，已写入部分可断点补写"
            elif resumed:
                message = "中断批次已补写未完成对象并完成影响重算，已确认记录保持不回退"
            else:
                message = "变更批次已接收并完成影响重算"
            return result, message

    def list_batches(
        self,
        *,
        keyword: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = [dict(row) for row in store.rows(BATCH_MODULE)]
        if keyword:
            rows = [row for row in rows if keyword in _text(row.get("批次编号"))]
        for row in rows:
            row.update(self._snapshot_stats(_text(row.get("批次编号"))))
        rows.sort(key=lambda row: int(row.get("到达序号", 0) or 0), reverse=True)
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_batch(self, batch_no: str) -> dict[str, Any] | None:
        batch = self._find_batch(batch_no)
        if batch is None:
            return None
        detail = dict(batch)
        detail["变更项"] = [
            dict(row) for row in store.rows(ITEM_MODULE) if row.get("批次编号") == batch_no
        ]
        detail["导出文件"] = [
            dict(row) for row in store.rows(FILE_MODULE) if row.get("批次编号") == batch_no
        ]
        detail.update(self._snapshot_stats(batch_no))
        return detail

    # ---------- 影响重算 ----------
    def recalc_batch(self, batch_no: str) -> tuple[dict[str, Any] | None, str]:
        with _lock:
            batch = self._find_batch(batch_no)
            if batch is None:
                return None, f"变更批次 {batch_no} 不存在"
            stats = self._recalc_locked(batch_no)
            stats.update(self._snapshot_stats(batch_no))
            return stats, "影响范围已按实际关联字段重算，仅保留真实受影响对象"

    def _recalc_locked(self, batch_no: str) -> dict[str, int]:
        """按批次内已写入变更项重算影响：只命中实际变更了关联字段的依赖。"""
        items = [
            row for row in store.rows(ITEM_MODULE)
            if row.get("批次编号") == batch_no and row.get("写入状态") == "已写入"
        ]
        # 同一作业在同一批次内的多次变更，只保留轮次最新的一轮表字段参与重算
        latest_items: dict[str, dict[str, Any]] = {}
        for row in items:
            key = _text(row.get("作业编号"))
            if key not in latest_items or int(row.get("轮次序号", 0) or 0) > int(
                latest_items[key].get("轮次序号", 0) or 0
            ):
                latest_items[key] = row

        dependencies = store.rows(DEPENDENCY_MODULE)
        snapshots = store.rows(SNAPSHOT_MODULE)
        existing_keys = {
            (_text(row.get("批次编号")), _text(row.get("关系编号"))) for row in snapshots
        }
        created = inherited = skipped = 0
        for item in latest_items.values():
            job_keys = {_text(item.get("作业编号")), _text(item.get("作业名称"))} - {""}
            src_changed = _text(item.get("变更前源端数据表")) != _text(item.get("变更后源端数据表"))
            tgt_changed = _text(item.get("变更前目标数据表")) != _text(item.get("变更后目标数据表"))
            if not (src_changed or tgt_changed):
                continue
            for dep in dependencies:
                directions: list[str] = []
                if tgt_changed and _text(dep.get("上游作业")) in job_keys:
                    directions.append("上游目标表变更")
                if src_changed and _text(dep.get("下游作业")) in job_keys:
                    directions.append("下游源表变更")
                if not directions:
                    continue
                key = (batch_no, _text(dep.get("关系编号")))
                if key in existing_keys:
                    skipped += 1
                    continue
                latest = self._latest_snapshot(_text(dep.get("关系编号")), exclude_batch=batch_no)
                if latest is not None and latest.get("复核状态") == "已确认":
                    review, origin, carried = "已确认", "历史承接", _text(latest.get("批次编号"))
                    inherited += 1
                elif latest is not None:
                    review, origin, carried = "待复核", "历史承接", _text(latest.get("批次编号"))
                    inherited += 1
                else:
                    review, origin, carried = "待复核", "本批重算", ""
                snapshots.append({
                    "id": _next_id(SNAPSHOT_MODULE),
                    "批次编号": batch_no,
                    "关系编号": dep.get("关系编号"),
                    "上游作业": dep.get("上游作业"),
                    "下游作业": dep.get("下游作业"),
                    "命中方向": "、".join(directions),
                    "作业编号": item.get("作业编号"),
                    "作业名称": item.get("作业名称"),
                    "变更前源端数据表": item.get("变更前源端数据表"),
                    "变更后源端数据表": item.get("变更后源端数据表"),
                    "变更前目标数据表": item.get("变更前目标数据表"),
                    "变更后目标数据表": item.get("变更后目标数据表"),
                    "作业版本": item.get("变更后作业版本"),
                    "复核状态": review,
                    "来源": origin,
                    "承接批次": carried,
                    "生成时间": _now(),
                })
                existing_keys.add(key)
                created += 1
        return {"新增快照": created, "承接历史": inherited, "幂等跳过": skipped}

    # ---------- 受影响依赖快照（两处共用取数） ----------
    def list_snapshots(
        self,
        *,
        batch_no: str | None = None,
        review: str | None = None,
        relation_no: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = [dict(row) for row in store.rows(SNAPSHOT_MODULE)]
        if batch_no:
            rows = [row for row in rows if _text(row.get("批次编号")) == batch_no]
        if review:
            rows = [row for row in rows if row.get("复核状态") == review]
        if relation_no:
            rows = [row for row in rows if relation_no in _text(row.get("关系编号"))]
        rows.sort(key=lambda row: int(row.get("id", 0) or 0))
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def confirm_snapshot(self, snapshot_id: int) -> tuple[dict[str, Any] | None, str]:
        with _lock:
            row = store.find(SNAPSHOT_MODULE, snapshot_id)
            if row is None:
                return None, f"受影响依赖记录 {snapshot_id} 不存在"
            if row.get("复核状态") == "已确认":
                return dict(row), "该记录已是已确认状态，确认结果保持不回退"
            row["复核状态"] = "已确认"
            row["确认时间"] = _now()
            return dict(row), "受影响依赖已确认复核"

    # ---------- 变更导出文件 ----------
    def generate_export(self, batch_no: str) -> tuple[dict[str, Any] | None, str]:
        with _lock:
            if self._find_batch(batch_no) is None:
                return None, f"变更批次 {batch_no} 不存在"
            rows, _ = self.list_snapshots(batch_no=batch_no, page=1, size=100000)
            if not rows:
                return None, f"变更批次 {batch_no} 没有受影响依赖快照，无可导出内容"
            csv_path, zip_path = self._write_files(batch_no, rows)
            record = self._upsert_file_record(batch_no, csv_path, zip_path, len(rows))
            return record, f"变更导出文件已生成，共 {len(rows)} 条，与影响清单条目逐条对应"

    def ensure_file(self, batch_no: str, kind: str) -> tuple[Path | None, str]:
        """下载/打包前确保文件存在：缺失时按快照重新生成，下载超时重试总能拿到文件。"""
        with _lock:
            if self._find_batch(batch_no) is None:
                return None, f"变更批次 {batch_no} 不存在"
            record = self._find_file_record(batch_no)
            name = _safe_name(batch_no)
            suffix = "csv" if kind == "csv" else "zip"
            path = EXPORT_DIR / f"change-impact-{name}.{suffix}"
            if record is None or not path.exists():
                rows, _ = self.list_snapshots(batch_no=batch_no, page=1, size=100000)
                if not rows:
                    return None, f"变更批次 {batch_no} 没有受影响依赖快照，请先在批次内完成影响重算"
                self._write_files(batch_no, rows)
                self._upsert_file_record(batch_no, EXPORT_DIR / f"change-impact-{name}.csv",
                                         EXPORT_DIR / f"change-impact-{name}.zip", len(rows))
            return path, "文件已就绪"

    def _write_files(self, batch_no: str, rows: list[dict[str, Any]]) -> tuple[Path, Path]:
        EXPORT_DIR.mkdir(parents=True, exist_ok=True)
        name = _safe_name(batch_no)
        csv_path = EXPORT_DIR / f"change-impact-{name}.csv"
        with csv_path.open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=SNAPSHOT_COLUMNS)
            writer.writeheader()
            for row in rows:
                writer.writerow({col: row.get(col, "") for col in SNAPSHOT_COLUMNS})
        manifest = {
            "批次编号": batch_no,
            "生成时间": _now(),
            "条目数": len(rows),
            "条目": [
                {
                    "关系编号": row.get("关系编号"),
                    "作业编号": row.get("作业编号"),
                    "作业版本": row.get("作业版本"),
                    "复核状态": row.get("复核状态"),
                }
                for row in rows
            ],
        }
        zip_path = EXPORT_DIR / f"change-impact-{name}.zip"
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.write(csv_path, arcname=csv_path.name)
            zf.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2))
        return csv_path, zip_path

    def _upsert_file_record(
        self, batch_no: str, csv_path: Path, zip_path: Path, count: int
    ) -> dict[str, Any]:
        record = self._find_file_record(batch_no)
        if record is None:
            record = {"id": _next_id(FILE_MODULE), "批次编号": batch_no}
            store.rows(FILE_MODULE).append(record)
        record.update({
            "CSV文件": csv_path.name,
            "打包文件": zip_path.name,
            "条目数": count,
            "状态": "已生成",
            "生成时间": _now(),
        })
        return dict(record)

    # ---------- 内部工具 ----------
    def _apply_item(self, batch_no: str, item: dict[str, Any]) -> str:
        """写入变更项并按轮次序号裁决：晚到的旧批次携带过期表字段，不参与重算。"""
        job = self._find_job(item["作业编号"])
        note = ""
        if job is None:
            status = "已过期"
            note = f"作业 {item['作业编号']} 不存在，变更项未应用"
        else:
            current_round = int(job.get("当前轮次", 0) or 0)
            existing = self._find_applied_item(item["作业编号"], item["轮次序号"])
            if item["轮次序号"] < current_round:
                status = "已过期"
                note = (
                    f"晚到的旧批次：作业当前轮次为 {current_round}，本项轮次 {item['轮次序号']}，"
                    "携带的过期表字段不参与重算，影响清单保留较新轮次"
                )
            elif existing is not None:
                if self._item_content(existing) == self._item_content(item):
                    status = "已过期"
                    note = "相同轮次的变更已应用，本次幂等跳过"
                else:
                    status = "已过期"
                    note = f"轮次 {item['轮次序号']} 与已应用变更内容冲突，保留先到达批次的表字段"
            else:
                status = "已写入"
                job["源端数据表"] = item["变更后源端数据表"]
                job["目标数据表"] = item["变更后目标数据表"]
                job["作业版本"] = item["变更后作业版本"]
                job["当前轮次"] = item["轮次序号"]
        store.rows(ITEM_MODULE).append({
            "id": _next_id(ITEM_MODULE),
            "批次编号": batch_no,
            **item,
            "写入状态": status,
            "冲突说明": note,
        })
        return status

    def _find_batch(self, batch_no: str) -> dict[str, Any] | None:
        for row in store.rows(BATCH_MODULE):
            if _text(row.get("批次编号")) == batch_no:
                return row
        return None

    def _find_job(self, job_no: str) -> dict[str, Any] | None:
        for row in store.rows(ETLJOB_MODULE):
            if _text(row.get("作业编号")) == job_no:
                return row
        return None

    def _find_applied_item(self, job_no: str, round_no: int) -> dict[str, Any] | None:
        for row in store.rows(ITEM_MODULE):
            if (
                _text(row.get("作业编号")) == job_no
                and int(row.get("轮次序号", 0) or 0) == round_no
                and row.get("写入状态") == "已写入"
            ):
                return row
        return None

    @staticmethod
    def _item_content(item: dict[str, Any]) -> tuple[str, str, str, str, str]:
        return (
            _text(item.get("变更后源端数据表")),
            _text(item.get("变更后目标数据表")),
            _text(item.get("变更后作业版本")),
            _text(item.get("变更前源端数据表")),
            _text(item.get("变更前目标数据表")),
        )

    def _latest_snapshot(self, relation_no: str, *, exclude_batch: str) -> dict[str, Any] | None:
        """既有历史依赖记录：取该关系在其他批次里最新的一条快照作为承接来源。"""
        candidates = [
            row for row in store.rows(SNAPSHOT_MODULE)
            if _text(row.get("关系编号")) == relation_no and _text(row.get("批次编号")) != exclude_batch
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda row: int(row.get("id", 0) or 0))

    def _find_file_record(self, batch_no: str) -> dict[str, Any] | None:
        for row in store.rows(FILE_MODULE):
            if _text(row.get("批次编号")) == batch_no:
                return row
        return None

    def _next_arrival(self) -> int:
        return max(
            (int(row.get("到达序号", 0) or 0) for row in store.rows(BATCH_MODULE)),
            default=0,
        ) + 1

    @staticmethod
    def _snapshot_stats(batch_no: str) -> dict[str, int]:
        rows = [row for row in store.rows(SNAPSHOT_MODULE) if _text(row.get("批次编号")) == batch_no]
        return {
            "受影响依赖数": len(rows),
            "待复核数": sum(1 for row in rows if row.get("复核状态") == "待复核"),
        }


# 单例：依赖解析查询与ETL作业计算两处一起取数，共用同一份受影响依赖数据快照
change_impact_service = ChangeImpactService()

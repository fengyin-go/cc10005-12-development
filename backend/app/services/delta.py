"""增量采集业务规则：状态流转、字段校验与筛选口径都收在这里。

游标水位只有一条推进路径：引用一份「已交付」的封箱单，取它的候选水位。
上传进度、已收分片数、提交篮的完成度都不算采集完成，不能用来推断水位。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.storage import StorageService
from app.store import store

MODULE = "delta"
REQUIRED_FIELDS = ["游标编号", "目标站点", "增量字段"]
STATUS_ORDER = ["草稿", "待校验", "运行中", "已暂停"]
ACTION_RULES = {"提交校验": "待校验", "启动增量": "运行中", "暂停增量": "已暂停"}
NEGATIVE_ACTIONS = []

storage_service = StorageService()


class DeltaService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("游标编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"增量游标 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于增量采集可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"增量游标已{action}"

    def promote_watermark(
        self, entry_id: int, values: dict[str, Any]
    ) -> tuple[dict[str, Any] | None, str]:
        """从一份已交付封箱单推进水位；封箱单必须已交付，且候选水位不能低于当前水位。"""
        receipt_ref = values.get("封箱单号") or values.get("receipt_id")
        receipt, message = storage_service.resolve_delivered_receipt(receipt_ref)
        if receipt is None:
            return None, message
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"增量游标 {entry_id} 不存在或已归档"

        watermark = receipt.get("候选水位")
        if watermark in (None, ""):
            return None, f"封箱单 {receipt['封箱单号']} 没有候选水位，不能推进游标"
        comparison = _compare_watermark(watermark, entry.get("当前水位"))
        confirmed = _truthy(values.get("确认跨类型推进") or values.get("confirm_type_change"))
        if comparison is None:
            if not confirmed:
                return (
                    None,
                    "候选水位与当前水位格式不同（例如日期与占位文本），无法自动比较；"
                    "请人工确认后带「确认跨类型推进」再提交",
                )
            comparison = 0
            entry["水位推进说明"] = f"跨类型推进：{entry.get('当前水位')} → {watermark}"
        if comparison < 0:
            return None, f"候选水位「{watermark}」低于当前水位「{entry.get('当前水位')}」，游标不倒退"

        entry["当前水位"] = watermark
        entry["水位来源封箱单"] = receipt["封箱单号"]
        entry["水位推进时间"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        return entry, f"游标水位已推进到 {watermark}（来源封箱单 {receipt['封箱单号']}）"


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value if value is not None else "").strip().lower() in {"1", "true", "yes", "是", "确认"}


def _as_number(value: Any) -> float | None:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _as_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip().replace("Z", "+00:00")
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _compare_watermark(left: Any, right: Any) -> int | None:
    """比较两个水位。

    只有两边同属一种可比较格式才给结论：都像数字按数值比，都像 ISO 时间按时间比，
    字符串完全相同算相等。格式对不上（比如日期和占位文本）返回 None，交给人工确认，
    不靠字符串大小去猜。
    """
    if right in (None, ""):
        return 1
    left_text = str(left if left is not None else "").strip()
    right_text = str(right if right is not None else "").strip()
    if not left_text:
        return None
    if left_text == right_text:
        return 0

    left_number, right_number = _as_number(left_text), _as_number(right_text)
    if left_number is not None and right_number is not None:
        return (left_number > right_number) - (left_number < right_number)

    left_stamp, right_stamp = _as_datetime(left_text), _as_datetime(right_text)
    if left_stamp is not None and right_stamp is not None:
        return (left_stamp > right_stamp) - (left_stamp < right_stamp)

    return None

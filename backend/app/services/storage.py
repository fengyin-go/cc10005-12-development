"""数据落盘业务规则：存储配置、批次提交篮、封箱单交付与分区落地都收在这里。

几条硬约束（都在本文件里守，路由层只做参数转发）：

* 提交篮创建时复制一份当时的存储配置（输出路径、文件格式、分区策略、覆写模式、
  压缩算法）。之后编辑配置不影响在途封箱单。
* 分片按「分片序号」入篮，重复序号且摘要一致视为同一个分片的重复上报，返回原记录；
  重复序号但摘要不同说明两份内容都可能对，登记为冲突，要求人工裁决，不覆盖。
* 只有 1..N 号分片全部到齐、且没有待裁决冲突时才允许封箱。从上传进度推断不了采集完成，
  所以缺片一律拒绝封箱。
* 封箱单一旦交付就不可变。重复交付同一封箱单只返回原回执（receipt），不重建内容。
* 两个封箱单写同一分区时，按提交篮里冻结的覆写模式决定：追加 / 替换 / 拒绝。
* 增量游标只接受「已交付封箱单」里的候选水位，且候选水位不能低于游标当前水位；
  上传进度、已收分片数都不能当作采集完成的证据。
"""
from __future__ import annotations

import hashlib
import threading
from datetime import datetime, timezone
from typing import Any, Iterable

from app.store import store

MODULE = "storage"
REQUIRED_FIELDS = ["配置编号", "存储类型", "输出路径"]
OPTIONAL_FIELDS = ["文件格式", "分区策略", "压缩算法", "保留天数", "覆写模式"]
STATUS_ORDER = ["草稿", "待校验", "已启用", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "启用配置": "已启用", "停用配置": "已停用"}
NEGATIVE_ACTIONS = ["停用配置"]

# 提交篮生命周期
BASKET_OPEN = "收分片中"
BASKET_SEALED = "已封箱"
BASKET_REJECTED = "已拒绝"

# 封箱单交付状态
RECEIPT_PENDING = "待交付"
RECEIPT_DELIVERED = "已交付"
RECEIPT_SUPERSEDED = "已被替换"

# 分片状态
SHARD_RECEIVED = "已收到"
SHARD_CONFLICT = "待裁决"

# 分区写入决策
WRITE_MODES = {"追加": "append", "替换": "replace", "拒绝": "reject"}
MODE_ALIASES = {
    "append": "追加",
    "覆盖": "替换",
    "overwrite": "替换",
    "replace": "替换",
    "reject": "拒绝",
    "fail": "拒绝",
}

# 一份封箱单最多允许的分片数，避免用超大序号把校验拖垮
MAX_SHARD_INDEX = 10_000

TABLES = {
    "baskets": "storage_baskets",
    "shards": "storage_basket_shards",
    "receipts": "storage_receipts",
    "partitions": "storage_partitions",
}

_lock = threading.RLock()


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _next_id(table: str) -> int:
    rows = store.rows(table)
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1


def _text(value: Any) -> str:
    return str(value if value is not None else "").strip()


def normalize_write_mode(value: Any) -> str | None:
    """把配置里的覆写模式归一成「追加 / 替换 / 拒绝」；认不出来的返回 None。"""
    raw = _text(value)
    if not raw:
        return "替换"
    if raw in WRITE_MODES:
        return raw
    return MODE_ALIASES.get(raw.lower(), None)


def _digest(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


class StorageService:
    # ------------------------------------------------------------------ 配置
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
            rows = [row for row in rows if keyword in str(row.get("配置编号", ""))]
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
        # 可选项一并登记：提交篮冻结的配置快照会用到文件格式、分区策略、覆写模式
        for field in OPTIONAL_FIELDS:
            if values.get(field) is not None:
                entry[field] = values.get(field)
        entry.setdefault("文件格式", "parquet")
        entry.setdefault("分区策略", "按日期")
        entry.setdefault("覆写模式", "替换")
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def update_entry(self, entry_id: int, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """编辑存储配置。只改配置本身，不改已经复制进提交篮的配置快照。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"存储配置 {entry_id} 不存在或已归档"
        if entry.get("status") == STATUS_ORDER[-1]:
            return None, "已停用的存储配置请先启用再编辑，避免在半生效状态下改输出路径"
        changed: list[str] = []
        for field, value in values.items():
            if field in {"id", "status", "pending", "abnormal"}:
                continue
            if value is None:
                continue
            if str(entry.get(field)) != str(value):
                entry[field] = value
                changed.append(field)
        if not changed:
            return entry, "存储配置没有变化"
        return entry, f"存储配置已更新：{'、'.join(changed)}"

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"存储配置 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于数据落盘可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"存储配置已{action}"

    # ------------------------------------------------------------- 提交篮
    def open_basket(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """创建提交篮：冻结当时的存储配置，并登记批次标识与预期分片数。

        参数：
            批次标识  batch_id（必填，全系统唯一）
            配置编号  config_code（必填）
            预期分片数 shard_total（必填，正整数）
            候选水位  candidate_watermark（可选，但必须来自已交付封箱单才能推进游标）
            存储配置  config（缺省时从数据落盘配置里按编号取）
        """
        batch_id = _text(values.get("批次标识") or values.get("batch_id"))
        config_code = _text(values.get("配置编号") or values.get("config_code"))
        shard_total_raw = values.get("预期分片数", values.get("shard_total"))
        candidate = values.get("候选水位", values.get("candidate_watermark"))
        batch_digest = _text(values.get("批次摘要") or values.get("batch_digest"))
        if not batch_id:
            return None, "缺少必填字段：批次标识"
        if not config_code:
            return None, "缺少必填字段：配置编号"
        try:
            shard_total = int(shard_total_raw)
        except (TypeError, ValueError):
            return None, "预期分片数必须是正整数"
        if shard_total <= 0 or shard_total > MAX_SHARD_INDEX:
            return None, f"预期分片数必须在 1..{MAX_SHARD_INDEX} 之间"

        with _lock:
            baskets = store.rows(TABLES["baskets"])
            existing = next((row for row in baskets if row.get("批次标识") == batch_id), None)
            if existing is not None:
                # 同批次标识带不同批次摘要：两份内容都可能对，登记冲突并要求人工裁决，
                # 不能用后到内容悄悄替换先到的提交篮。
                previous = _text(existing.get("批次摘要"))
                if batch_digest and previous and batch_digest != previous:
                    existing["批次摘要冲突"] = batch_digest
                    return (
                        None,
                        f"批次标识 {batch_id} 已存在且摘要不同，已登记为待人工裁决；"
                        "请先裁决批次内容，不能直接覆盖",
                    )
                return None, f"批次标识 {batch_id} 已经建过提交篮，请使用续提入口继续上报"

            config = self._config_snapshot(config_code, values)
            if isinstance(config, str):
                return None, config
            write_mode = normalize_write_mode(config.get("覆写模式"))
            if write_mode is None:
                return None, f"覆写模式「{config.get('覆写模式')}」无法识别，只支持追加、替换、拒绝"

            basket = {
                "id": _next_id(TABLES["baskets"]),
                "批次标识": batch_id,
                "配置编号": config_code,
                "state": BASKET_OPEN,
                "预期分片数": shard_total,
                "收到分片数": 0,
                "候选水位": candidate,
                "覆写模式": write_mode,
                "配置快照": config,
                "批次摘要": batch_digest or None,
                "批次摘要冲突": None,
                "创建时间": _now(),
                "封箱时间": None,
                "封箱单号": None,
            }
            baskets.append(basket)
            return self._basket_view(basket), f"提交篮已创建，等待 {shard_total} 个分片"

    def resolve_batch_conflict(self, basket_id: int, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """人工裁决「同批次标识、不同批次摘要」：明确选择保留现有批次还是改用新批次。

        选择「沿用原批次」时直接丢弃新摘要，原提交篮原样保留；
        选择「改用新批次」时必须给出与冲突摘要一致的分片摘要或批次摘要，避免误操作。
        """
        decision = _text(values.get("裁决") or values.get("decision")).lower()
        operator = _text(values.get("裁决人") or values.get("operator")) or "值班管理员"
        with _lock:
            basket = store.find(TABLES["baskets"], basket_id)
            if basket is None:
                return None, f"提交篮 {basket_id} 不存在或已归档"
            if not basket.get("批次摘要冲突"):
                return None, "这个提交篮没有待裁决的批次摘要冲突"
            if decision in {"沿用原批次", "原批次", "keep", "先到"}:
                basket["批次摘要冲突"] = None
                basket["批次裁决"] = "沿用原批次"
                basket["批次裁决人"] = operator
                basket["批次裁决时间"] = _now()
                return self._basket_view(basket), "已裁决沿用原批次，后到摘要被丢弃"
            if decision in {"改用新批次", "新批次", "replace", "后到"}:
                replacement = _text(values.get("新批次摘要") or values.get("内容"))
                if not replacement:
                    return None, "改用新批次时必须提供新的批次摘要，裁决才生效"
                if replacement != basket["批次摘要冲突"]:
                    return None, "新批次摘要与登记的冲突摘要不一致，裁决未生效"
                basket["批次摘要"] = replacement
                basket["批次摘要冲突"] = None
                basket.setdefault("批次裁决历史", []).append(
                    {"时间": _now(), "裁决人": operator, "处理": "改用新批次"}
                )
                basket["批次裁决"] = "改用新批次"
                basket["批次裁决人"] = operator
                basket["批次裁决时间"] = _now()
                return self._basket_view(basket), "已裁决改用新批次摘要，请继续核对分片"
            return None, "裁决必须明确选择「沿用原批次」或「改用新批次」"

    def _config_snapshot(self, config_code: str, values: dict[str, Any]) -> dict[str, Any] | str:
        """取提交篮要冻结的存储配置。优先用请求里显式给出的完整快照。"""
        provided = values.get("存储配置") or values.get("config")
        config: dict[str, Any] = {}
        if isinstance(provided, dict):
            config = dict(provided)
        else:
            for field in ["存储类型", "输出路径", "文件格式", "分区策略", "压缩算法", "保留天数", "覆写模式"]:
                if values.get(field) is not None:
                    config[field] = values.get(field)
        config.setdefault("配置编号", config_code)
        if not _text(config.get("输出路径")):
            # 没带快照时回落到数据落盘里当前那条配置
            row = next(
                (item for item in store.rows(MODULE) if item.get("配置编号") == config_code),
                None,
            )
            if row is None:
                return f"配置编号 {config_code} 不在数据落盘登记表里，请先登记存储配置"
            for field in ["存储类型", "输出路径", "文件格式", "分区策略", "压缩算法", "保留天数", "覆写模式"]:
                config.setdefault(field, row.get(field))
        if not _text(config.get("输出路径")):
            return "存储配置缺少输出路径，无法确定落地位置"
        config["输出路径"] = _text(config["输出路径"])
        config["文件格式"] = _text(config.get("文件格式")) or "parquet"
        config["分区策略"] = _text(config.get("分区策略")) or "按日期"
        config["覆写模式"] = normalize_write_mode(config.get("覆写模式")) or "替换"
        return config

    def _basket_view(self, basket: dict[str, Any]) -> dict[str, Any]:
        shards = sorted(
            (row for row in store.rows(TABLES["shards"]) if row.get("篮号") == basket["id"]),
            key=lambda row: int(row.get("分片序号", 0)),
        )
        received = [int(row["分片序号"]) for row in shards if row.get("状态") == SHARD_RECEIVED]
        conflicts = [int(row["分片序号"]) for row in shards if row.get("状态") == SHARD_CONFLICT]
        total = int(basket["预期分片数"])
        missing = [index for index in range(1, total + 1) if index not in received]
        view = dict(basket)
        view["分片明细"] = [dict(row) for row in shards]
        view["已收到序号"] = received
        view["冲突序号"] = conflicts
        view["缺片序号"] = missing
        view["可封箱"] = (
            basket["state"] == BASKET_OPEN and not missing and not conflicts
        )
        return view

    def get_basket(self, basket_id: int) -> dict[str, Any] | None:
        basket = store.find(TABLES["baskets"], basket_id)
        return self._basket_view(basket) if basket else None

    def find_basket_by_batch(self, batch_id: str) -> dict[str, Any] | None:
        for basket in store.rows(TABLES["baskets"]):
            if basket.get("批次标识") == batch_id:
                return self._basket_view(basket)
        return None

    def list_baskets(self, state: str | None = None) -> list[dict[str, Any]]:
        rows = store.rows(TABLES["baskets"])
        if state:
            rows = [row for row in rows if row.get("state") == state]
        return [self._basket_view(row) for row in sorted(rows, key=lambda row: int(row["id"]), reverse=True)]

    def receive_shard(
        self, basket_id: int, values: dict[str, Any]
    ) -> tuple[dict[str, Any] | None, str, bool]:
        """把一片结果放进提交篮。

        幂等：同一分片序号 + 同一摘要重复上报，返回原记录（created=False），不追加第二份。
        冲突：同一分片序号 + 不同摘要，登记为待裁决，不悄悄替换先到的内容。
        中断续提：只要分片序号对得上，重复上报就自然识别为「已收到」。
        """
        index_raw = values.get("分片序号", values.get("shard_index"))
        content = values.get("内容", values.get("content"))
        claimed = _text(values.get("分片摘要") or values.get("shard_digest"))
        if isinstance(content, (dict, list)):
            import json

            content = json.dumps(content, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        content = "" if content is None else str(content)
        try:
            index = int(index_raw)
        except (TypeError, ValueError):
            return None, "分片序号必须是正整数", False

        with _lock:
            basket = store.find(TABLES["baskets"], basket_id)
            if basket is None:
                return None, f"提交篮 {basket_id} 不存在或已归档", False
            if basket["state"] != BASKET_OPEN:
                return None, f"提交篮当前是「{basket['state']}」，不能再收分片", False
            if index < 1 or index > int(basket["预期分片数"]):
                return None, f"分片序号 {index} 超出本批次 1..{basket['预期分片数']} 的范围", False

            digest = _digest(content)
            if not content:
                return None, "分片内容为空，无内容可交付；请先把结果切成非空分片再上报", False
            if claimed and claimed != digest:
                return None, "上报的分片摘要与内容实际摘要不一致，拒绝收片", False

            existing = next(
                (
                    row
                    for row in store.rows(TABLES["shards"])
                    if row.get("篮号") == basket["id"] and int(row.get("分片序号", 0)) == index
                ),
                None,
            )
            if existing is not None:
                if existing["状态"] == SHARD_RECEIVED and existing["分片摘要"] == digest:
                    return self._basket_view(basket), f"分片 {index} 已收到，按原记录处理", False
                if existing["状态"] == SHARD_CONFLICT:
                    return self._basket_view(basket), f"分片 {index} 仍在等待人工裁决，请先裁决再继续", False
                # 同号不同摘要：登记冲突，保留先到内容原样
                existing["状态"] = SHARD_CONFLICT
                existing["冲突摘要"] = digest
                existing["冲突时间"] = _now()
                return self._basket_view(basket), f"分片 {index} 摘要与已收到内容不一致，已转人工裁决", False

            shard = {
                "id": _next_id(TABLES["shards"]),
                "篮号": basket["id"],
                "分片序号": index,
                "分片摘要": digest,
                "内容": content,
                "字节数": len(content.encode("utf-8")),
                "状态": SHARD_RECEIVED,
                "收到时间": _now(),
                "冲突摘要": None,
                "冲突时间": None,
            }
            store.rows(TABLES["shards"]).append(shard)
            basket["收到分片数"] = sum(
                1
                for row in store.rows(TABLES["shards"])
                if row.get("篮号") == basket["id"] and row.get("状态") == SHARD_RECEIVED
            )
            return self._basket_view(basket), f"分片 {index} 已入篮", True

    def resolve_conflict(
        self, basket_id: int, values: dict[str, Any]
    ) -> tuple[dict[str, Any] | None, str]:
        """人工裁决同号不同摘要的分片：明确选择保留哪一份。"""
        index_raw = values.get("分片序号", values.get("shard_index"))
        keep = _text(values.get("裁决") or values.get("decision")).lower()
        operator = _text(values.get("裁决人") or values.get("operator")) or "值班管理员"
        try:
            index = int(index_raw)
        except (TypeError, ValueError):
            return None, "分片序号必须是正整数"

        if keep not in {"先到", "first", "保留先到", "后到", "second", "保留后到"}:
            return None, "裁决必须明确选择「先到」或「后到」，系统不替人猜"

        with _lock:
            basket = store.find(TABLES["baskets"], basket_id)
            if basket is None:
                return None, f"提交篮 {basket_id} 不存在或已归档"
            if basket["state"] != BASKET_OPEN:
                return None, "提交篮已结束，冲突无法再裁决"
            shard = next(
                (
                    row
                    for row in store.rows(TABLES["shards"])
                    if row.get("篮号") == basket["id"] and int(row.get("分片序号", 0)) == index
                ),
                None,
            )
            if shard is None:
                return None, f"分片 {index} 还没有任何内容，无需裁决"
            if shard["状态"] != SHARD_CONFLICT:
                return None, f"分片 {index} 没有待裁决的冲突"

            keep_later = keep in {"后到", "second", "保留后到"}
            if keep_later:
                late_content = _text(values.get("后到内容") or values.get("content"))
                if not late_content:
                    return None, "选择保留后到内容时必须提供后到分片的完整内容"
                if _digest(late_content) != shard["冲突摘要"]:
                    return None, "后到内容与登记的冲突摘要不一致，裁决未生效"
                shard["内容"] = late_content
                shard["分片摘要"] = shard["冲突摘要"]
                shard["字节数"] = len(late_content.encode("utf-8"))
            shard["状态"] = SHARD_RECEIVED
            shard["裁决"] = "保留后到" if keep_later else "保留先到"
            shard["裁决人"] = operator
            shard["裁决时间"] = _now()
            shard["冲突摘要"] = None
            return self._basket_view(basket), f"分片 {index} 已裁决为保留{'后到' if keep_later else '先到'}内容"

    # ------------------------------------------------------------- 封箱交付
    def seal_basket(self, basket_id: int) -> tuple[dict[str, Any] | None, str]:
        """封箱：分片齐全且无冲突才生成封箱单，并给出候选水位的去向。

        幂等：已经封过的篮子再点一次封箱，返回原封箱单而不是再生成一份。
        """
        with _lock:
            basket = store.find(TABLES["baskets"], basket_id)
            if basket is None:
                return None, f"提交篮 {basket_id} 不存在或已归档"
            if basket["state"] == BASKET_SEALED:
                existing = next(
                    (
                        row
                        for row in store.rows(TABLES["receipts"])
                        if row.get("篮号") == basket["id"]
                    ),
                    None,
                )
                if existing is not None:
                    return dict(existing), f"提交篮已经封箱，封箱单 {existing['封箱单号']} 未重复生成"
                return None, "提交篮标记为已封箱，但没有找到对应封箱单，请联系管理员核对"
            if basket["state"] != BASKET_OPEN:
                return None, f"提交篮当前是「{basket['state']}」，不能封箱"
            view = self._basket_view(basket)
            if view.get("批次摘要冲突"):
                return None, f"提交篮存在待裁决的批次摘要冲突（{view['批次摘要冲突'][:8]}…），裁决后才能封箱"
            if view["冲突序号"]:
                shown = "、".join(str(index) for index in view["冲突序号"])
                return None, f"分片 {shown} 存在待裁决冲突，裁决后才能封箱"
            if view["缺片序号"]:
                shown = "、".join(str(index) for index in view["缺片序号"][:10])
                more = " 等" if len(view["缺片序号"]) > 10 else ""
                return None, f"缺少分片 {shown}{more}，缺片不能封箱"

            receipt = {
                "id": _next_id(TABLES["receipts"]),
                "封箱单号": None,
                "篮号": basket["id"],
                "批次标识": basket["批次标识"],
                "配置编号": basket["配置编号"],
                "配置快照": dict(basket["配置快照"]),
                "覆写模式": basket["覆写模式"],
                "候选水位": basket["候选水位"],
                "封箱摘要": self._box_digest(basket),
                "分片数": int(basket["预期分片数"]),
                "状态": RECEIPT_PENDING,
                "封箱时间": _now(),
                "交付时间": None,
                "回执": None,
                "分区号": None,
            }
            receipts = store.rows(TABLES["receipts"])
            receipt["封箱单号"] = f"BOX-{receipt['id']:06d}"
            receipts.append(receipt)
            basket["state"] = BASKET_SEALED
            basket["封箱时间"] = receipt["封箱时间"]
            basket["封箱单号"] = receipt["封箱单号"]
            return dict(receipt), f"已封箱并生成封箱单 {receipt['封箱单号']}，等待交付"

    def _box_digest(self, basket: dict[str, Any]) -> str:
        shards = sorted(
            (
                row
                for row in store.rows(TABLES["shards"])
                if row.get("篮号") == basket["id"] and row.get("状态") == SHARD_RECEIVED
            ),
            key=lambda row: int(row["分片序号"]),
        )
        material = "|".join(
            f"{row['分片序号']}:{row['分片摘要']}" for row in shards
        )
        return _digest(f"{basket['批次标识']}#{material}")

    def list_receipts(self) -> list[dict[str, Any]]:
        return [dict(row) for row in store.rows(TABLES["receipts"])]

    def get_receipt(self, receipt_id: int) -> dict[str, Any] | None:
        receipt = store.find(TABLES["receipts"], receipt_id)
        return dict(receipt) if receipt else None

    def deliver_receipt(
        self, receipt_id: int, values: dict[str, Any] | None = None
    ) -> tuple[dict[str, Any] | None, str]:
        """交付封箱单为可读结果。

        幂等：已交付的封箱单重送，只返回原回执，不再写一次分区。
        """
        values = values or {}
        with _lock:
            receipt = store.find(TABLES["receipts"], receipt_id)
            if receipt is None:
                return None, f"封箱单 {receipt_id} 不存在或已归档"
            if receipt["状态"] == RECEIPT_DELIVERED:
                return dict(receipt), self._already_delivered_message(receipt)
            if receipt["状态"] == RECEIPT_SUPERSEDED:
                return None, "该封箱单已被同分区的替换模式覆盖，不能再交付"

            config = receipt["配置快照"]
            partition = _text(values.get("目标分区") or values.get("partition")) or self._partition_of(config)

            ok, message, partition_row = self._write_partition(receipt, partition, config)
            if not ok:
                return None, message

            receipt["状态"] = RECEIPT_DELIVERED
            receipt["交付时间"] = _now()
            receipt["分区号"] = partition
            payload = self._readable_payload(receipt)
            receipt["回执"] = {
                "封箱单号": receipt["封箱单号"],
                "批次标识": receipt["批次标识"],
                "配置编号": receipt["配置编号"],
                "目标分区": partition,
                "覆写模式": receipt["覆写模式"],
                "分片数": receipt["分片数"],
                "封箱摘要": receipt["封箱摘要"],
                "候选水位": receipt["候选水位"],
                "结果摘要": payload["结果摘要"],
                "版本": partition_row["当前版本"],
                "交付时间": receipt["交付时间"],
            }
            return dict(receipt), f"封箱单 {receipt['封箱单号']} 已交付到分区 {partition}"

    def _already_delivered_message(self, receipt: dict[str, Any]) -> str:
        stamp = (receipt.get("回执") or {}).get("交付时间") or receipt.get("交付时间") or ""
        return f"封箱单 {receipt['封箱单号']} 已于 {stamp} 交付，重送只返回原回执"

    def _partition_of(self, config: dict[str, Any]) -> str:
        base = _text(config.get("输出路径")).rstrip("/")
        strategy = _text(config.get("分区策略"))
        fmt = _text(config.get("文件格式")) or "parquet"
        if strategy == "按日期":
            suffix = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        elif strategy in {"按站点", "按批次"}:
            suffix = _text(config.get("配置编号")) or "default"
        else:
            suffix = "default"
        return f"{base}/dt={suffix}/data.{fmt}"

    def _write_partition(
        self, receipt: dict[str, Any], partition: str, config: dict[str, Any]
    ) -> tuple[bool, str, dict[str, Any]]:
        """按冻结的覆写模式把整份封箱内容写进分区。

        读者看到的永远是一整版：先准备完整内容，再一次性切换当前版本。
        """
        partitions = store.rows(TABLES["partitions"])
        current = next((row for row in partitions if row.get("分区号") == partition), None)
        mode = normalize_write_mode(receipt["覆写模式"]) or "替换"
        shards = [
            row
            for row in store.rows(TABLES["shards"])
            if row.get("篮号") == receipt["篮号"] and row.get("状态") == SHARD_RECEIVED
        ]
        content = self._merge_content(shards)

        if current is not None and mode == "拒绝":
            return False, f"分区 {partition} 已有交付版本，覆写模式为拒绝，本次交付被拦下", current

        if current is None:
            current = {
                "id": _next_id(TABLES["partitions"]),
                "分区号": partition,
                "配置编号": receipt["配置编号"],
                "覆写模式": receipt["覆写模式"],
                "当前版本": 0,
                "版本历史": [],
                "内容": "",
                "结果摘要": None,
                "封箱单号": None,
                "更新时间": _now(),
            }
            partitions.append(current)

        if mode == "追加" and current["内容"]:
            merged = current["内容"] + "\n" + content
            operation = "追加"
        else:
            merged = content
            operation = "替换"

        version = current["当前版本"] + 1
        current["版本历史"].append(
            {
                "版本": current["当前版本"],
                "内容": current["内容"],
                "结果摘要": current["结果摘要"],
                "封箱单号": current["封箱单号"],
            }
        )
        current["当前版本"] = version
        current["内容"] = merged
        current["结果摘要"] = _digest(merged)
        current["封箱单号"] = receipt["封箱单号"]
        current["覆写模式"] = receipt["覆写模式"]
        current["更新时间"] = _now()
        receipt["写入方式"] = operation

        if mode == "替换":
            self._supersede_previous(receipt, partition)
        return True, operation, current

    def _supersede_previous(self, receipt: dict[str, Any], partition: str) -> None:
        for row in store.rows(TABLES["receipts"]):
            if (
                row["id"] != receipt["id"]
                and row.get("分区号") == partition
                and row.get("状态") == RECEIPT_DELIVERED
            ):
                row["状态"] = RECEIPT_SUPERSEDED

    def _merge_content(self, shards: Iterable[dict[str, Any]]) -> str:
        ordered = sorted(shards, key=lambda row: int(row["分片序号"]))
        return "\n".join(str(row["内容"]) for row in ordered)

    def _readable_payload(self, receipt: dict[str, Any]) -> dict[str, Any]:
        shards = sorted(
            (
                row
                for row in store.rows(TABLES["shards"])
                if row.get("篮号") == receipt["篮号"]
            ),
            key=lambda row: int(row["分片序号"]),
        )
        content = self._merge_content(shards)
        return {
            "封箱单号": receipt["封箱单号"],
            "批次标识": receipt["批次标识"],
            "配置编号": receipt["配置编号"],
            "配置快照": receipt["配置快照"],
            "分片": [
                {"分片序号": row["分片序号"], "分片摘要": row["分片摘要"], "字节数": row["字节数"]}
                for row in shards
            ],
            "结果内容": content,
            "结果摘要": _digest(content),
        }

    def read_receipt(self, receipt_id: int) -> tuple[dict[str, Any] | None, str]:
        """把已交付的封箱单读成可核对的结果；未交付的不给内容。"""
        payload = self.readable_result(receipt_id)
        if isinstance(payload, str):
            return None, payload
        return payload, ""

    def readable_result(self, receipt_id: int) -> dict[str, Any] | str:
        receipt = store.find(TABLES["receipts"], receipt_id)
        if receipt is None:
            return f"封箱单 {receipt_id} 不存在或已归档"
        # 已交付过的封箱单保留完整内容供审计；但只有最新一版参与实际落地，
        # 已被同分区替换的封箱单不能再次交付。
        if receipt["状态"] not in {RECEIPT_DELIVERED, RECEIPT_SUPERSEDED}:
            return "封箱单尚未交付，不能作为可读结果使用"
        return self._readable_payload(receipt)

    def list_partitions(self) -> list[dict[str, Any]]:
        return [
            {
                "id": row["id"],
                "分区号": row["分区号"],
                "配置编号": row["配置编号"],
                "覆写模式": row["覆写模式"],
                "当前版本": row["当前版本"],
                "结果摘要": row["结果摘要"],
                "封箱单号": row["封箱单号"],
                "更新时间": row["更新时间"],
            }
            for row in store.rows(TABLES["partitions"])
        ]

    def get_partition(self, partition_id: int) -> dict[str, Any] | None:
        """读取分区当前一整版：读者只会看到这一份内容，不会读到半写状态。"""
        row = store.find(TABLES["partitions"], partition_id)
        if row is None:
            return None
        return {
            "id": row["id"],
            "分区号": row["分区号"],
            "配置编号": row["配置编号"],
            "覆写模式": row["覆写模式"],
            "当前版本": row["当前版本"],
            "结果摘要": row["结果摘要"],
            "封箱单号": row["封箱单号"],
            "内容": row["内容"],
            "版本历史": row["版本历史"],
            "更新时间": row["更新时间"],
        }

    # ------------------------------------------------------------- 增量游标
    def resolve_delivered_receipt(self, ref: Any) -> tuple[dict[str, Any] | None, str]:
        """按封箱单号或 id 取一份已交付封箱单；未交付、不存在都给出可读原因。"""
        if ref in (None, ""):
            return None, "缺少封箱单号，不能从上传进度推断采集完成"
        text = _text(ref)
        receipt = None
        if text.isdigit():
            receipt = store.find(TABLES["receipts"], int(text))
        if receipt is None:
            receipt = next(
                (row for row in store.rows(TABLES["receipts"]) if row.get("封箱单号") == text),
                None,
            )
        if receipt is None:
            return None, f"封箱单 {text} 不存在或已归档"
        if receipt["状态"] != RECEIPT_DELIVERED:
            return None, f"封箱单 {receipt['封箱单号']} 尚未交付，不能用于推进游标"
        return dict(receipt), ""

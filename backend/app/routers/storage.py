"""数据落盘接口：存储配置、采集批次提交篮、封箱单交付与分区落地。

路由层只做参数转发；状态判断、缺片拦截、冲突裁决、覆写模式都在
``app.services.storage`` 里。

注意路由顺序：静态路径（/baskets、/receipts、/export）必须排在 ``/{entry_id}``
前面，否则 "baskets" 会被当成 entry_id 去解析成整数。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.storage import StorageService

router = APIRouter(prefix="/api/storage", tags=["数据落盘"])

service = StorageService()

LIST_FIELDS = ["配置编号", "存储类型", "输出路径", "文件格式", "分区策略", "压缩算法", "保留天数", "覆写模式"]
STATUSES = ["草稿", "待校验", "已启用", "已停用"]
BASKET_STATES = ["收分片中", "已封箱"]


# ---------------------------------------------------------------- 存储配置
@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按配置编号检索"),
    status: str | None = Query(default=None, description="草稿、待校验、已启用、已停用"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按配置编号与状态过滤数据落盘列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条存储配置，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="存储配置已登记", entry=entry)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出数据落盘清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "storage", "total": total, "items": items}


# ---------------------------------------------------------------- 提交篮
@router.get("/baskets", response_model=dict)
def list_baskets(state: str | None = Query(default=None, description="收分片中、已封箱")) -> dict[str, Any]:
    """列出采集批次提交篮；按创建时间倒序，带上缺片序号与可封箱判定。"""
    items = service.list_baskets(state=state)
    return {"items": items, "total": len(items)}


@router.post("/baskets", response_model=ActionResult)
def open_basket(payload: EntryPayload) -> ActionResult:
    """创建提交篮：冻结当前存储配置，登记批次标识、配置编号与预期分片数。"""
    basket, message = service.open_basket(payload.values)
    if basket is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=basket)


@router.get("/baskets/by-batch/{batch_id}", response_model=dict)
def get_basket_by_batch(batch_id: str) -> dict[str, Any]:
    """按批次标识找提交篮，供续提入口使用；找不到时给出可读说明。"""
    basket = service.find_basket_by_batch(batch_id)
    if basket is None:
        raise HTTPException(status_code=404, detail=f"批次 {batch_id} 还没有提交篮，请先创建")
    return basket


@router.get("/baskets/{basket_id}", response_model=dict)
def get_basket(basket_id: int) -> dict[str, Any]:
    """读取提交篮明细：已收到序号、冲突序号、缺片序号、能否封箱。"""
    basket = service.get_basket(basket_id)
    if basket is None:
        raise HTTPException(status_code=404, detail=f"提交篮 {basket_id} 不存在或已归档")
    return basket


@router.post("/baskets/{basket_id}/shards", response_model=ActionResult)
def receive_shard(basket_id: int, payload: EntryPayload) -> ActionResult:
    """把一片结果放进提交篮；同序号同摘要幂等，同序号不同摘要转人工裁决。"""
    basket, message, created = service.receive_shard(basket_id, payload.values)
    if basket is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message or ("分片已入篮" if created else "分片已存在"), entry=basket)


@router.post("/baskets/{basket_id}/conflicts", response_model=ActionResult)
def resolve_conflict(basket_id: int, payload: EntryPayload) -> ActionResult:
    """人工裁决同号不同摘要的分片；必须明确选择保留先到还是后到内容。"""
    basket, message = service.resolve_conflict(basket_id, payload.values)
    if basket is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=basket)


@router.post("/baskets/{basket_id}/batch-conflicts", response_model=ActionResult)
def resolve_batch_conflict(basket_id: int, payload: EntryPayload) -> ActionResult:
    """人工裁决同批次标识、不同批次摘要：沿用原批次或明确改用新批次。"""
    basket, message = service.resolve_batch_conflict(basket_id, payload.values)
    if basket is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=basket)


@router.post("/baskets/{basket_id}/seal", response_model=ActionResult)
def seal_basket(basket_id: int) -> ActionResult:
    """封箱：分片齐全且无待裁决冲突才生成封箱单；缺片一律拒绝。"""
    receipt, message = service.seal_basket(basket_id)
    if receipt is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=receipt)


# ---------------------------------------------------------------- 封箱单
@router.get("/receipts", response_model=dict)
def list_receipts() -> dict[str, Any]:
    """列出封箱单及其交付状态。"""
    items = service.list_receipts()
    return {"items": items, "total": len(items)}


@router.get("/receipts/{receipt_id}", response_model=dict)
def get_receipt(receipt_id: int) -> dict[str, Any]:
    """读取封箱单及其回执。"""
    receipt = service.get_receipt(receipt_id)
    if receipt is None:
        raise HTTPException(status_code=404, detail=f"封箱单 {receipt_id} 不存在或已归档")
    return receipt


@router.post("/receipts/{receipt_id}/deliver", response_model=ActionResult)
def deliver_receipt(receipt_id: int, payload: EntryPayload | None = None) -> ActionResult:
    """交付封箱单为可读结果；同分区的写入按冻结的覆写模式处理，重送只返回原回执。"""
    receipt, message = service.deliver_receipt(receipt_id, payload.values if payload else {})
    if receipt is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=receipt)


@router.get("/receipts/{receipt_id}/result", response_model=dict)
def read_receipt_result(receipt_id: int) -> dict[str, Any]:
    """把已交付封箱单读成可核对的结果；未交付的不返回内容。"""
    payload, message = service.read_receipt(receipt_id)
    if payload is None:
        raise HTTPException(status_code=409, detail=message)
    return payload


@router.get("/partitions", response_model=dict)
def list_partitions() -> dict[str, Any]:
    """列出落地分区与当前版本；读者总能看到一整版。"""
    items = service.list_partitions()
    return {"items": items, "total": len(items)}


@router.get("/partitions/{partition_id}", response_model=dict)
def get_partition(partition_id: int) -> dict[str, Any]:
    """读取分区当前一整版内容与版本历史。"""
    partition = service.get_partition(partition_id)
    if partition is None:
        raise HTTPException(status_code=404, detail=f"分区 {partition_id} 不存在或尚未落地")
    return partition


# ---------------------------------------------------------------- 单条配置
@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条存储配置明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"存储配置 {entry_id} 不存在或已归档")
    return entry


@router.patch("/{entry_id}", response_model=ActionResult)
def update_entry(entry_id: int, payload: EntryPayload) -> ActionResult:
    """编辑存储配置。只改这条配置；已经复制进提交篮的配置快照不受影响。"""
    entry, message = service.update_entry(entry_id, payload.values)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条存储配置执行提交校验、启用配置、停用配置；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)

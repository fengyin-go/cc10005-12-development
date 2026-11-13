"""采集批次提交篮规则的端到端自检。

直接用 FastAPI TestClient 打真实路由，覆盖：
  * 缺片不能封箱
  * 同摘要重复上报走幂等、不拼出两份
  * 同批次同序号不同摘要 → 人工裁决，裁决前不能封箱
  * 封箱单交付幂等，重送返回原回执
  * 两个封箱单命中同一分区：追加 / 替换 / 拒绝
  * 提交篮冻结配置，配置编辑不影响在途封箱单
  * 增量游标只接受已交付封箱单的候选水位

运行：cd backend && .venv/bin/python tests/test_storage_basket.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402


def check(label: str, condition: bool, detail: str = "") -> None:
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {label}" + (f" — {detail}" if detail and not condition else ""))
    if not condition:
        raise SystemExit(1)


def post(client: TestClient, path: str, values: dict) -> dict:
    response = client.post(path, json={"values": values})
    assert response.status_code == 200, (path, response.status_code, response.text)
    return response.json()


def get(client: TestClient, path: str) -> dict:
    response = client.get(path)
    assert response.status_code == 200, (path, response.status_code, response.text)
    return response.json()


def open_basket(client: TestClient, batch: str, total: int, config: dict, watermark: str | None = None) -> dict:
    result = post(
        client,
        "/api/storage/baskets",
        {
            "批次标识": batch,
            "配置编号": config["配置编号"],
            "预期分片数": total,
            "存储配置": config,
            "候选水位": watermark,
        },
    )
    assert result["ok"], result
    return result["entry"]


def add_shard(client: TestClient, basket_id: int, index: int, content: str) -> dict:
    return post(
        client,
        f"/api/storage/baskets/{basket_id}/shards",
        {"分片序号": index, "内容": content},
    )


def main() -> None:
    client = TestClient(app)
    config = {
        "配置编号": "STOR-CFG-1",
        "输出路径": "s3://landing/orders",
        "文件格式": "parquet",
        "分区策略": "按日期",
        "覆写模式": "替换",
    }

    # ---- 1. 缺片不能封箱 -------------------------------------------------
    basket = open_basket(client, "BATCH-001", 3, config, watermark="2026-11-13T00:00:00Z")
    add_shard(client, basket["id"], 1, "row-1")
    add_shard(client, basket["id"], 2, "row-2")
    sealed = post(client, f"/api/storage/baskets/{basket['id']}/seal", {})
    check("缺第 3 片时封箱被拒绝", sealed["ok"] is False, sealed["message"])
    check("拒绝信息点名缺片序号", "3" in sealed["message"], sealed["message"])
    view = get(client, f"/api/storage/baskets/{basket['id']}")
    check("缺片序号被如实列出", view["缺片序号"] == [3], str(view["缺片序号"]))
    check("缺片时不可封箱", view["可封箱"] is False)

    # ---- 2. 中断续提：同片同摘要幂等 ------------------------------------
    again = add_shard(client, basket["id"], 1, "row-1")
    check("重复上报同一分片仍成功", again["ok"] is True)
    check("重复上报只保留一份分片", len(again["entry"]["分片明细"]) == 2, str(len(again["entry"]["分片明细"])))
    check("重复上报识别为已收到", would_be_idempotent(again["message"]), again["message"])

    # ---- 3. 同号不同摘要：转人工裁决，不悄悄替换 ------------------------
    conflict = add_shard(client, basket["id"], 2, "row-2-tampered")
    check("同号不同摘要被拦下", conflict["ok"] is True)
    view = get(client, f"/api/storage/baskets/{basket['id']}")
    check("冲突序号进入待裁决", view["冲突序号"] == [2], str(view["冲突序号"]))
    original = next(row for row in view["分片明细"] if row["分片序号"] == 2)
    check("先到内容没有被悄悄替换", original["内容"] == "row-2", original["内容"])
    blocked = post(client, f"/api/storage/baskets/{basket['id']}/seal", {})
    check("存在待裁决冲突时不能封箱", blocked["ok"] is False and "裁决" in blocked["message"], blocked["message"])

    # 裁决为保留先到；未到齐前仍不能封箱
    verdict = post(
        client,
        f"/api/storage/baskets/{basket['id']}/conflicts",
        {"分片序号": 2, "裁决": "先到", "裁决人": "值班员甲"},
    )
    check("人工裁决保留先到内容", verdict["ok"] is True, verdict["message"])
    check("裁决后冲突清空", verdict["entry"]["冲突序号"] == [])
    keep_first = next(row for row in verdict["entry"]["分片明细"] if row["分片序号"] == 2)
    check("保留先到后内容仍是先到那份", keep_first["内容"] == "row-2", keep_first["内容"])

    # 另一个分片演示「保留后到」：必须给出与冲突摘要一致的后到内容
    add_shard(client, basket["id"], 3, "row-3-old")
    second_view = add_shard(client, basket["id"], 3, "row-3-new")["entry"]
    late_digest = next(row for row in second_view["分片明细"] if row["分片序号"] == 3)["冲突摘要"]
    wrong_late = post(
        client,
        f"/api/storage/baskets/{basket['id']}/conflicts",
        {"分片序号": 3, "裁决": "后到", "后到内容": "not-the-same"},
    )
    check("后到内容与冲突摘要不一致时裁决不生效", wrong_late["ok"] is False, wrong_late["message"])
    check("需要后到内容摘要可核对", bool(late_digest))
    late = post(
        client,
        f"/api/storage/baskets/{basket['id']}/conflicts",
        {"分片序号": 3, "裁决": "后到", "后到内容": "row-3-new"},
    )
    check("人工裁决保留后到内容", late["ok"] is True, late["message"])
    kept_late = next(row for row in late["entry"]["分片明细"] if row["分片序号"] == 3)
    check("保留后到后内容是后到那份", kept_late["内容"] == "row-3-new", kept_late["内容"])

    # ---- 4. 齐片后封箱，生成封箱单 --------------------------------------
    # 1、2、3 号分片都已入篮且裁决完毕，直接封箱
    sealed = post(client, f"/api/storage/baskets/{basket['id']}/seal", {})
    check("分片齐全后可以封箱", sealed["ok"] is True, sealed["message"])
    receipt = sealed["entry"]
    check("封箱单状态为待交付", receipt["状态"] == "待交付", receipt["状态"])
    resealed = post(client, f"/api/storage/baskets/{basket['id']}/seal", {})
    check("重复点封箱不会生成第二份封箱单", resealed["entry"]["封箱单号"] == receipt["封箱单号"], resealed["message"])
    check("封箱后不再收片", add_shard(client, basket["id"], 1, "late")["ok"] is False)

    # 未交付的封箱单不能当可读结果 / 不能推进游标
    undelivered = client.get(f"/api/storage/receipts/{receipt['id']}/result")
    check("未交付封箱单不返回可读结果", undelivered.status_code == 409)
    early = post(client, "/api/delta/1/watermark", {"封箱单号": receipt["封箱单号"]})
    check("未交付封箱单不能推进游标", early["ok"] is False, early["message"])

    # ---- 5. 交付与重送幂等 ----------------------------------------------
    delivered = post(client, f"/api/storage/receipts/{receipt['id']}/deliver", {})
    check("封箱单可交付", delivered["ok"] is True, delivered["message"])
    first_receipt = delivered["entry"]["回执"]
    check("回执包含目标分区", bool(first_receipt["目标分区"]), str(first_receipt))
    check("回执包含结果摘要", bool(first_receipt["结果摘要"]))

    resent = post(client, f"/api/storage/receipts/{receipt['id']}/deliver", {})
    check("重送已交付封箱单返回原回执", resent["entry"]["回执"] == first_receipt, "回执被改写")
    check("重送提示为返回原回执", "原回执" in resent["message"], resent["message"])

    readable = get(client, f"/api/storage/receipts/{receipt['id']}/result")
    check("交付后可读结果包含完整内容", readable["结果内容"] == "row-1\nrow-2\nrow-3-new", readable["结果内容"])
    check("结果摘要与回执一致", readable["结果摘要"] == first_receipt["结果摘要"])

    # ---- 6. 配置冻结：编辑配置不影响在途封箱单 --------------------------
    inflated = post(
        client,
        "/api/storage/baskets",
        {
            "批次标识": "BATCH-002",
            "配置编号": "STOR-CFG-2",
            "预期分片数": 1,
            "存储配置": {
                "配置编号": "STOR-CFG-2",
                "输出路径": "s3://landing/orders",
                "文件格式": "jsonl",
                "分区策略": "按日期",
                "覆写模式": "追加",
            },
            "候选水位": "2026-11-13T01:00:00Z",
        },
    )
    pending = inflated["entry"]
    check("第二个提交篮创建成功", inflated["ok"] is True, inflated["message"])
    check("在途封箱单冻结了创建时的文件格式", pending["配置快照"]["文件格式"] == "jsonl", str(pending["配置快照"]))
    edited = client.patch(
        "/api/storage/1",
        json={"values": {"文件格式": "csv", "覆写模式": "拒绝"}},
    ).json()
    check("配置编辑本身成功", edited["ok"] is True, edited["message"])
    still = get(client, f"/api/storage/baskets/{pending['id']}")
    check("配置编辑不改变在途封箱单", still["配置快照"]["文件格式"] == "jsonl" and still["覆写模式"] == "追加")

    # ---- 7. 同分区追加 ----------------------------------------------------
    add_shard(client, pending["id"], 1, "row-4")
    sealed2 = post(client, f"/api/storage/baskets/{pending['id']}/seal", {})
    check("第二个提交篮可以封箱", sealed2["ok"] is True, sealed2["message"])
    delivered2 = post(client, f"/api/storage/receipts/{sealed2['entry']['id']}/deliver", {})
    check("追加模式交付成功", delivered2["ok"] is True, delivered2["message"])
    receipt2 = delivered2["entry"]["回执"]
    # 分区由创建时冻结的输出路径 + 分区策略 + 文件格式共同决定
    check("追加模式写进冻结快照的分区", receipt2["目标分区"].endswith("data.jsonl"), str(receipt2))
    partition = next(
        row
        for row in get(client, "/api/storage/partitions")["items"]
        if row["分区号"] == receipt2["目标分区"]
    )
    check("追加后版本仍是一整版", partition["当前版本"] == 1, str(partition["当前版本"]))
    appended = get(client, "/api/storage/receipts/{}/result".format(sealed2["entry"]["id"]))
    check("追加内容可与上一版衔接读取", appended["结果内容"] == "row-4", appended["结果内容"])

    # 另一个追加封箱单命中同一分区，才真正验证追加语义
    append_again = open_basket(
        client,
        "BATCH-002B",
        1,
        {
            "配置编号": "STOR-CFG-2",
            "输出路径": "s3://landing/orders",
            "文件格式": "jsonl",
            "分区策略": "按日期",
            "覆写模式": "追加",
        },
    )
    add_shard(client, append_again["id"], 1, "row-5")
    sealed2b = post(client, f"/api/storage/baskets/{append_again['id']}/seal", {})
    delivered2b = post(client, f"/api/storage/receipts/{sealed2b['entry']['id']}/deliver", {})
    check("同分区第二个追加封箱单交付成功", delivered2b["ok"] is True, delivered2b["message"])
    partition2 = next(
        row
        for row in get(client, "/api/storage/partitions")["items"]
        if row["分区号"] == receipt2["目标分区"]
    )
    check("追加后同一分区版本递增", partition2["当前版本"] == 2, str(partition2["当前版本"]))
    current = get(client, f"/api/storage/partitions/{partition2['id']}")
    check("追加后分区里是两版合并的完整内容", current["内容"] == "row-4\nrow-5", current["内容"])

    # ---- 8. 同分区替换：读者只看到最新完整一版 --------------------------
    replace_basket = open_basket(
        client,
        "BATCH-003",
        1,
        {
            "配置编号": "STOR-CFG-3",
            "输出路径": "s3://landing/orders",
            "文件格式": "jsonl",
            "分区策略": "按日期",
            "覆写模式": "替换",
        },
    )
    add_shard(client, replace_basket["id"], 1, "row-9")
    sealed3 = post(client, f"/api/storage/baskets/{replace_basket['id']}/seal", {})
    delivered3 = post(client, f"/api/storage/receipts/{sealed3['entry']['id']}/deliver", {})
    check("替换模式交付成功", delivered3["ok"] is True, delivered3["message"])
    replaced = next(
        row for row in get(client, "/api/storage/partitions")["items"] if row["分区号"] == receipt2["目标分区"]
    )
    check("替换后封箱单号指向新版本", replaced["封箱单号"] == sealed3["entry"]["封箱单号"], str(replaced))
    superseded_view = get(client, f"/api/storage/receipts/{sealed2b['entry']['id']}")
    check("被同分区替换的封箱单标记为已被替换", superseded_view["状态"] == "已被替换", superseded_view["状态"])
    old_read = get(client, f"/api/storage/receipts/{receipt['id']}/result")
    check("被替换前的旧回执内容仍可按封箱单读到", old_read["结果内容"] == "row-1\nrow-2\nrow-3-new")
    redeliver_old = post(client, f"/api/storage/receipts/{sealed2b['entry']['id']}/deliver", {})
    check("已被替换的封箱单不能再交付", redeliver_old["ok"] is False, redeliver_old["message"])
    current_after_replace = get(client, f"/api/storage/partitions/{replaced['id']}")
    check("替换后读者只看到最新完整一版", current_after_replace["内容"] == "row-9", current_after_replace["内容"])

    # ---- 9. 同分区拒绝 ----------------------------------------------------
    reject_basket = open_basket(
        client,
        "BATCH-004",
        1,
        {
            "配置编号": "STOR-CFG-4",
            "输出路径": "s3://landing/orders",
            "文件格式": "parquet",
            "分区策略": "按日期",
            "覆写模式": "拒绝",
        },
    )
    add_shard(client, reject_basket["id"], 1, "row-10")
    sealed4 = post(client, f"/api/storage/baskets/{reject_basket['id']}/seal", {})
    rejected = post(client, f"/api/storage/receipts/{sealed4['entry']['id']}/deliver", {})
    check("拒绝模式下同分区交付被拦下", rejected["ok"] is False, rejected["message"])
    rejected_view = get(client, f"/api/storage/receipts/{sealed4['entry']['id']}")
    check("被拒绝的封箱单不落成已交付", rejected_view["状态"] == "待交付", rejected_view["状态"])

    # ---- 10. 游标只接受已交付封箱单的候选水位 ---------------------------
    numeric = client.post(
        "/api/delta",
        json={"values": {"游标编号": "DELT-T1", "目标站点": "shop.example", "增量字段": "updated_at", "当前水位": "100"}},
    ).json()
    check("可登记数值水位游标", numeric["ok"] is True, numeric["message"])
    cursor_id = numeric["entry"]["id"]
    promoted = post(client, f"/api/delta/{cursor_id}/watermark", {"封箱单号": receipt["封箱单号"]})
    check("已交付封箱单可推进游标", promoted["ok"] is True, promoted["message"])
    check("游标水位来自候选水位", promoted["entry"]["当前水位"] == "2026-11-13T00:00:00Z", str(promoted["entry"]))
    check("游标记录水位来源封箱单", promoted["entry"].get("水位来源封箱单") == receipt["封箱单号"])
    no_receipt = post(client, f"/api/delta/{cursor_id}/watermark", {})
    check("不提供封箱单不能推断水位", no_receipt["ok"] is False and "上传进度" in no_receipt["message"], no_receipt["message"])

    numeric_cursor = client.post(
        "/api/delta",
        json={"values": {"游标编号": "DELT-T2", "目标站点": "shop.example", "增量字段": "seq", "当前水位": "250"}},
    ).json()["entry"]
    monotonic = post(
        client,
        f"/api/delta/{numeric_cursor['id']}/watermark",
        {"封箱单号": sealed2b["entry"]["封箱单号"]},
    )
    check("候选水位低于当前数值水位时游标不倒退", monotonic["ok"] is False, monotonic["message"])

    stale = open_basket(
        client,
        "BATCH-005",
        1,
        {
            "配置编号": "STOR-CFG-5",
            "输出路径": "s3://landing/orders",
            "文件格式": "parquet",
            "分区策略": "按日期",
            "覆写模式": "替换",
        },
        watermark="2020-01-01T00:00:00Z",
    )
    add_shard(client, stale["id"], 1, "old")
    sealed5 = post(client, f"/api/storage/baskets/{stale['id']}/seal", {})
    post(client, f"/api/storage/receipts/{sealed5['entry']['id']}/deliver", {})
    backwards = post(client, f"/api/delta/{cursor_id}/watermark", {"封箱单号": sealed5["entry"]["封箱单号"]})
    check("候选水位早于当前水位时游标不倒退", backwards["ok"] is False, backwards["message"])

    conflict_type = open_basket(
        client,
        "BATCH-006",
        1,
        {
            "配置编号": "STOR-CFG-6",
            "输出路径": "s3://landing/orders",
            "文件格式": "parquet",
            "分区策略": "按日期",
            "覆写模式": "替换",
        },
        watermark="批次完成",
    )
    add_shard(client, conflict_type["id"], 1, "x")
    sealed6 = post(client, f"/api/storage/baskets/{conflict_type['id']}/seal", {})
    post(client, f"/api/storage/receipts/{sealed6['entry']['id']}/deliver", {})
    unclear = post(client, f"/api/delta/{cursor_id}/watermark", {"封箱单号": sealed6["entry"]["封箱单号"]})
    check("水位格式对不上时不擅自比较", unclear["ok"] is False and "人工确认" in unclear["message"], unclear["message"])
    confirmed = post(
        client,
        f"/api/delta/{cursor_id}/watermark",
        {"封箱单号": sealed6["entry"]["封箱单号"], "确认跨类型推进": True},
    )
    check("人工确认后可跨格式推进", confirmed["ok"] is True, confirmed["message"])

    # ---- 11. 批次标识唯一 + 续提入口 ------------------------------------
    duplicate = post(
        client,
        "/api/storage/baskets",
        {"批次标识": "BATCH-001", "配置编号": "STOR-CFG-1", "预期分片数": 3, "存储配置": config},
    )
    check("批次标识重复创建被拒绝", duplicate["ok"] is False, duplicate["message"])
    resumed = get(client, "/api/storage/baskets/by-batch/BATCH-001")
    check("续提入口能按批次标识找到原篮子", resumed["id"] == basket["id"], str(resumed["id"]))
    check("续提入口看到已收到的分片而不是两份", len(resumed["分片明细"]) == 3, str(len(resumed["分片明细"])))

    # ---- 12. 同批次标识 + 不同批次摘要：人工裁决 ------------------------
    digest_a = open_basket(
        client,
        "BATCH-DIGEST",
        1,
        {
            "配置编号": "STOR-CFG-7",
            "输出路径": "s3://landing/digest",
            "文件格式": "parquet",
            "分区策略": "按批次",
            "覆写模式": "替换",
        },
    )
    with_digest = client.post(
        "/api/storage/baskets",
        json={
            "values": {
                "批次标识": "BATCH-DIGEST-B",
                "配置编号": "STOR-CFG-7",
                "预期分片数": 1,
                "批次摘要": "sha256-first",
                "存储配置": {
                    "配置编号": "STOR-CFG-7",
                    "输出路径": "s3://landing/digest",
                    "文件格式": "parquet",
                    "分区策略": "按批次",
                    "覆写模式": "替换",
                },
            }
        },
    ).json()
    check("带批次摘要可以建篮", with_digest["ok"] is True, with_digest["message"])
    digest_basket = with_digest["entry"]
    same_digest = client.post(
        "/api/storage/baskets",
        json={"values": {"批次标识": "BATCH-DIGEST-B", "配置编号": "STOR-CFG-7", "预期分片数": 1, "批次摘要": "sha256-first"}},
    ).json()
    check("同标识同摘要走续提提示", same_digest["ok"] is False and "续提" in same_digest["message"], same_digest["message"])
    differing = client.post(
        "/api/storage/baskets",
        json={"values": {"批次标识": "BATCH-DIGEST-B", "配置编号": "STOR-CFG-7", "预期分片数": 1, "批次摘要": "sha256-second"}},
    ).json()
    check("同标识不同摘要要求人工裁决", differing["ok"] is False and "人工裁决" in differing["message"], differing["message"])
    conflicted = get(client, f"/api/storage/baskets/{digest_basket['id']}")
    check("冲突摘要被登记但先到摘要未被替换", conflicted["批次摘要"] == "sha256-first" and conflicted["批次摘要冲突"] == "sha256-second", str(conflicted))
    check("批次摘要冲突未裁决时不能封箱", post(client, f"/api/storage/baskets/{digest_basket['id']}/seal", {})["ok"] is False)
    kept = post(
        client,
        f"/api/storage/baskets/{digest_basket['id']}/batch-conflicts",
        {"裁决": "沿用原批次", "裁决人": "值班员乙"},
    )
    check("可裁决沿用原批次", kept["ok"] is True, kept["message"])
    check("沿用原批次后摘要不变", kept["entry"]["批次摘要"] == "sha256-first" and kept["entry"]["批次摘要冲突"] is None, str(kept["entry"]))

    # 改用新批次路径
    second = client.post(
        "/api/storage/baskets",
        json={
            "values": {
                "批次标识": "BATCH-DIGEST-C",
                "配置编号": "STOR-CFG-7",
                "预期分片数": 1,
                "批次摘要": "sha256-x",
                "存储配置": {
                    "配置编号": "STOR-CFG-7",
                    "输出路径": "s3://landing/digest",
                    "文件格式": "parquet",
                    "分区策略": "按批次",
                    "覆写模式": "替换",
                },
            }
        },
    ).json()["entry"]
    client.post(
        "/api/storage/baskets",
        json={"values": {"批次标识": "BATCH-DIGEST-C", "配置编号": "STOR-CFG-7", "预期分片数": 1, "批次摘要": "sha256-y"}},
    )
    switched = post(
        client,
        f"/api/storage/baskets/{second['id']}/batch-conflicts",
        {"裁决": "改用新批次", "新批次摘要": "sha256-y", "裁决人": "值班员丙"},
    )
    check("可裁决改用新批次", switched["ok"] is True, switched["message"])
    check("改用新批次后摘要更新", switched["entry"]["批次摘要"] == "sha256-y", str(switched["entry"]))

    print("\n全部规则自检通过")


def would_be_idempotent(message: str) -> bool:
    return "已收到" in message


if __name__ == "__main__":
    main()

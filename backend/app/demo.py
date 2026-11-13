"""演示数据：用真实的业务规则造两个提交篮，起服务后数据落盘页就有内容可看。

故意留下两种典型现场：
  * 一个还在收片的篮子，缺第 2 片 —— 用来演示「缺片不能封箱」；
  * 一个已封箱并交付的篮子 —— 用来演示封箱单、回执与落地分区。

不用手写分片摘要，全部走 StorageService，保证摘要和校验规则一致。
"""
from __future__ import annotations

from app.services.storage import StorageService


def bootstrap_demo() -> None:
    service = StorageService()
    config = {
        "配置编号": "STOR-0003",
        "存储类型": "对象存储",
        "输出路径": "s3://crawler-landing/orders",
        "文件格式": "parquet",
        "分区策略": "按站点",
        "压缩算法": "snappy",
        "保留天数": 30,
        "覆写模式": "替换",
    }

    open_basket, _ = service.open_basket(
        {
            "批次标识": "BATCH-DEMO-OPEN",
            "配置编号": config["配置编号"],
            "预期分片数": 3,
            "存储配置": config,
            "候选水位": "2026-11-13T06:00:00Z",
        }
    )
    if open_basket is not None:
        # 第 2 片没有上报，页面上会明确显示为缺片
        service.receive_shard(open_basket["id"], {"分片序号": 1, "内容": "order-1001\norder-1002"})
        service.receive_shard(open_basket["id"], {"分片序号": 3, "内容": "order-1005"})

    sealed, _ = service.open_basket(
        {
            "批次标识": "BATCH-DEMO-SEALED",
            "配置编号": config["配置编号"],
            "预期分片数": 2,
            "存储配置": config,
            "候选水位": "2026-11-13T07:00:00Z",
        }
    )
    if sealed is not None:
        service.receive_shard(sealed["id"], {"分片序号": 1, "内容": "order-2001"})
        service.receive_shard(sealed["id"], {"分片序号": 2, "内容": "order-2002"})
        receipt, _ = service.seal_basket(sealed["id"])
        if receipt is not None:
            service.deliver_receipt(receipt["id"])

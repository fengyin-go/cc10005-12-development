# 分布式网页采集调度系统

面向多站点网页抽取、代理池调度、反爬对抗、增量采集、失败重试与采集结果校验的分布式采集调度系统。

这是一个前后端分离的管理平台：前端 React + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 React + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm install
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 站点管理 | `site` | 采集站点 | 站点编号、域名地址、站点类型 |
| 抽取模板 | `template` | 网页模板 | 模板编号、入口地址、列表选择器 |
| 代理池管理 | `proxy` | 代理节点 | 节点编号、代理地址、代理协议 |
| 采集任务 | `spider` | 采集作业 | 作业编号、目标站点、抽取模板 |
| 调度策略 | `schedule` | 调度规则 | 规则编号、关联站点、调度表达式 |
| 反爬对抗 | `antibot` | 对抗策略 | 策略编号、验证码类型、识别服务 |
| 限速控制 | `ratelimit` | 限速配置 | 配置编号、目标站点、每秒请求上限 |
| 失败重试 | `retry` | 重试策略 | 策略编号、关联任务、最大重试次数 |
| 字段映射 | `extractor` | 映射规则 | 规则编号、源字段名称、目标字段名称 |
| 采集流水线 | `pipeline` | 流水线步骤 | 步骤编号、步骤名称、输入数据源 |
| 结果校验 | `validator` | 校验规则 | 规则编号、校验对象、校验类型 |
| 数据落盘 | `storage` | 存储配置 / 采集批次提交篮 | 配置编号、存储类型、输出路径、批次标识、封箱单号 |
| 采集监控 | `monitor` | 监控指标 | 指标编号、关联任务、指标名称 |
| 数据清洗 | `cleanse` | 清洗规则 | 规则编号、目标字段、空值替换 |
| 增量采集 | `delta` | 增量游标 | 游标编号、目标站点、增量字段、当前水位、水位来源封箱单 |
| 数据分发 | `export` | 分发通道 | 通道编号、输出数据集、目标系统 |
| RPA流程 | `rpaflow` | 自动化流程 | 流程编号、目标页面、操作序列 |
| 登录凭证 | `auth` | 凭证配置 | 凭证编号、目标站点、认证方式 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数据采集、爬虫与RPA

## 采集批次提交篮（数据落盘）

数据落盘页除了存储配置，还有一条「批次 → 分片 → 封箱 → 交付 → 分区」的链路，
规则集中在 `backend/app/services/storage.py`：

- **建篮冻结配置**：创建提交篮时复制当时的输出路径、文件格式、分区策略、覆写模式。
  之后编辑存储配置（`PATCH /api/storage/{id}`）不会改变在途封箱单。
- **分片入篮**：`POST /api/storage/baskets/{id}/shards`。同序号同摘要重复上报返回原记录，
  不会拼出两份；同序号不同摘要登记为冲突，要求 `POST /baskets/{id}/conflicts` 人工裁决，
  不能用后到内容悄悄替换。
- **同批次标识不同摘要**：建篮时若批次标识已存在且批次摘要不同，只登记冲突并要求
  `POST /baskets/{id}/batch-conflicts` 人工裁决（沿用原批次 / 改用新批次），
  不能用后到内容悄悄替换先到批次；未裁决前不能封箱。
- **缺片不能封箱**：`POST /baskets/{id}/seal` 只在 1..N 分片全部到齐且无待裁决冲突时成功，
  否则点名缺片序号。重复点封箱返回原封箱单，不会生成第二份。续提时按批次标识找回原篮子
  （`GET /baskets/by-batch/{批次标识}`），服务端按分片序号识别已收到的分片，
  中断重来不会重复拼接。
- **交付幂等**：`POST /receipts/{id}/deliver` 把封箱单写成可读结果并返回回执；
  重送已交付封箱单只返回原回执，不会再写一次分区。
- **同分区覆写**：两个封箱单命中同一分区时按冻结的覆写模式处理 —— `追加` 保留上一版，
  `替换` 以新版本整体覆盖并把旧封箱单标记为已被替换，`拒绝` 直接拦下本次交付。
  分区内容按版本整体切换，读者只会看到完整一版（`GET /api/storage/partitions/{id}`）。
- **游标只认已交付封箱单**：`POST /api/delta/{id}/watermark` 必须引用一份已交付封箱单，
  取它的候选水位；未交付、低于当前水位、或格式无法比较时都会拒绝（跨格式需人工确认）。
  上传进度、已收分片数不能用来推断采集完成。

自检脚本：`cd backend && .venv/bin/python tests/test_storage_basket.py`。

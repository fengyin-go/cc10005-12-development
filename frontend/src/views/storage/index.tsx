import { useCallback, useEffect, useMemo, useState } from 'react'

import {
  type BasketView,
  type PartitionView,
  type ReceiptView,
  type ReadableResult,
  deliverReceipt,
  fetchBaskets,
  fetchPartitions,
  fetchReadableResult,
  fetchReceipts,
  openBasket,
  resolveBatchConflict,
  resolveShardConflict,
  sealBasket,
  submitShard,
} from '@/api/baskets'
import { request } from '@/api/client'

const ENDPOINT = '/api/storage'
const columns = [
  "配置编号",
  "存储类型",
  "输出路径",
  "文件格式",
  "分区策略",
  "压缩算法",
  "保留天数",
  "覆写模式"
]
const actions = [
  "提交校验",
  "启用配置",
  "停用配置"
]
const statuses = [
  "草稿",
  "待校验",
  "已启用",
  "已停用"
]

type Row = Record<string, string | number | null>

/** 与后端 _partition_of 保持同一口径：输出路径 + 分区策略 + 文件格式，用于交付前预览。 */
function previewPartition(snapshot: Record<string, unknown>): string {
  const base = String(snapshot['输出路径'] ?? '').replace(/\/+$/, '')
  if (!base) return '—'
  const strategy = String(snapshot['分区策略'] ?? '')
  const format = String(snapshot['文件格式'] ?? 'parquet')
  const suffix = strategy === '按日期'
    ? new Date().toISOString().slice(0, 10)
    : String(snapshot['配置编号'] ?? 'default')
  return `${base}/dt=${suffix}/data.${format}`
}

type BasketForm = {
  batchId: string
  configCode: string
  输出路径: string
  文件格式: string
  分区策略: string
  覆写模式: string
  预期分片数: string
  候选水位: string
  批次摘要: string
}

const emptyBasketForm: BasketForm = {
  batchId: '',
  configCode: 'STOR-0003',
  输出路径: 's3://crawler-landing/orders',
  文件格式: 'parquet',
  分区策略: '按站点',
  覆写模式: '替换',
  预期分片数: '3',
  候选水位: '',
  批次摘要: '',
}

export default function StorageView() {
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [errorMessage, setErrorMessage] = useState('')
  const [notice, setNotice] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})

  const [baskets, setBaskets] = useState<BasketView[]>([])
  const [receipts, setReceipts] = useState<ReceiptView[]>([])
  const [partitions, setPartitions] = useState<PartitionView[]>([])
  const [activeBasket, setActiveBasket] = useState<BasketView | null>(null)
  const [readable, setReadable] = useState<ReadableResult | null>(null)
  const [basketForm, setBasketForm] = useState<BasketForm>(emptyBasketForm)
  const [shardIndex, setShardIndex] = useState('1')
  const [shardContent, setShardContent] = useState('')
  const [conflictIndex, setConflictIndex] = useState('1')
  const [lateContent, setLateContent] = useState('')

  const filterFields = useMemo(() => columns.slice(0, 3), [])

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams(filters).toString()
    try {
      const response = await request(`${ENDPOINT}?${query}`)
      if (!response.ok) {
        throw new Error('存储配置列表读取失败')
      }
      const payload = await response.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '数据落盘列表读取失败')
    }
  }, [filters])

  const reloadBasketArea = useCallback(async () => {
    try {
      const [nextBaskets, nextReceipts, nextPartitions] = await Promise.all([
        fetchBaskets(),
        fetchReceipts(),
        fetchPartitions(),
      ])
      setBaskets(nextBaskets)
      setReceipts(nextReceipts)
      setPartitions(nextPartitions)
      setActiveBasket((current) => {
        if (!current) return current
        return nextBaskets.find((item) => item.id === current.id) ?? null
      })
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '提交篮读取失败')
    }
  }, [])

  useEffect(() => {
    void reload()
  }, [reload])

  useEffect(() => {
    void reloadBasketArea()
  }, [reloadBasketArea])

  async function runAction(action: string, row: Row) {
    setErrorMessage('')
    setNotice('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}/actions`, {
        method: 'POST',
        body: JSON.stringify({ action }),
      })
      if (!response.ok) {
        throw new Error('数据落盘动作未生效，请稍后重试')
      }
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '数据落盘操作失败')
    }
  }

  async function handleOpenBasket() {
    setErrorMessage('')
    setNotice('')
    const result = await openBasket({
      批次标识: basketForm.batchId.trim(),
      配置编号: basketForm.configCode.trim(),
      预期分片数: basketForm.预期分片数,
      候选水位: basketForm.候选水位.trim() || null,
      批次摘要: basketForm.批次摘要.trim() || null,
      存储配置: {
        配置编号: basketForm.configCode.trim(),
        输出路径: basketForm.输出路径.trim(),
        文件格式: basketForm.文件格式.trim(),
        分区策略: basketForm.分区策略.trim(),
        覆写模式: basketForm.覆写模式,
      },
    })
    if (!result.ok || !result.data) {
      setErrorMessage(result.message || '提交篮创建失败')
      return
    }
    setNotice(result.message)
    setBasketForm((prev) => ({ ...prev, batchId: '', 批次摘要: '' }))
    setActiveBasket(result.data)
    await reloadBasketArea()
  }

  async function handleResume() {
    setErrorMessage('')
    setNotice('')
    const batchId = basketForm.batchId.trim()
    if (!batchId) {
      setErrorMessage('请先填写批次标识，续提入口按批次标识找回原提交篮')
      return
    }
    try {
      const response = await request(`${ENDPOINT}/baskets/by-batch/${encodeURIComponent(batchId)}`)
      const payload = await response.json()
      if (!response.ok) {
        throw new Error(payload.detail ?? '没有找到这个批次的提交篮')
      }
      setActiveBasket(payload as BasketView)
      setNotice(`已找回批次 ${batchId}：服务端按分片序号识别已收到的分片，不会重复拼接`)
    } catch (error) {
      setActiveBasket(null)
      setErrorMessage(error instanceof Error ? error.message : '续提失败')
    }
  }

  async function handleSubmitShard() {
    if (!activeBasket) return
    setErrorMessage('')
    setNotice('')
    const result = await submitShard(activeBasket.id, {
      分片序号: shardIndex,
      内容: shardContent,
    })
    if (!result.ok) {
      setErrorMessage(result.message || '分片入篮失败')
      await reloadBasketArea()
      return
    }
    setNotice(result.message)
    if (result.data) {
      setActiveBasket(result.data)
      const conflicts = result.data.冲突序号 ?? []
      if (conflicts.length > 0) {
        // 刚提交的这个分片转成了待裁决冲突：把序号带过去，内容留在输入框里，
        // 方便直接点「保留后到内容」。
        setConflictIndex(String(conflicts[conflicts.length - 1]))
        setLateContent(shardContent)
      } else {
        setShardContent('')
        setShardIndex(String(Math.min(activeBasket.预期分片数 + 1, (Number(shardIndex) || 1) + 1)))
      }
    }
    await reloadBasketArea()
  }

  async function handleResolve(decision: '先到' | '后到') {
    if (!activeBasket) return
    setErrorMessage('')
    setNotice('')
    const result = await resolveShardConflict(activeBasket.id, {
      分片序号: conflictIndex,
      裁决: decision,
      裁决人: '值班管理员',
      后到内容: decision === '后到' ? lateContent : undefined,
    })
    if (!result.ok) {
      setErrorMessage(result.message || '冲突裁决未生效')
      return
    }
    setNotice(result.message)
    setConflictIndex('')
    setLateContent('')
    if (result.data) setActiveBasket(result.data)
    await reloadBasketArea()
  }

  async function handleBatchDecide(decision: '沿用原批次' | '改用新批次') {
    if (!activeBasket) return
    setErrorMessage('')
    setNotice('')
    const result = await resolveBatchConflict(activeBasket.id, {
      裁决: decision,
      裁决人: '值班管理员',
      新批次摘要: decision === '改用新批次' ? activeBasket.批次摘要冲突 : undefined,
    })
    if (!result.ok) {
      setErrorMessage(result.message || '批次摘要裁决未生效')
      return
    }
    setNotice(result.message)
    if (result.data) setActiveBasket(result.data)
    await reloadBasketArea()
  }

  async function handleSeal() {
    if (!activeBasket) return
    setErrorMessage('')
    setNotice('')
    const result = await sealBasket(activeBasket.id)
    if (!result.ok) {
      setErrorMessage(result.message || '封箱失败')
      return
    }
    setNotice(result.message)
    await reloadBasketArea()
    if (result.data) await showResult(result.data.id, false)
  }

  async function handleDeliver(receiptId: number) {
    setErrorMessage('')
    setNotice('')
    const result = await deliverReceipt(receiptId)
    if (!result.ok) {
      setErrorMessage(result.message || '封箱单交付失败')
      await reloadBasketArea()
      return
    }
    setNotice(result.message)
    await reloadBasketArea()
    await showResult(receiptId, false)
  }

  async function showResult(receiptId: number, strict = true) {
    try {
      const payload = await fetchReadableResult(receiptId)
      setReadable(payload)
    } catch (error) {
      setReadable(null)
      if (strict) {
        setErrorMessage(error instanceof Error ? error.message : '封箱单读不出结果')
      }
    }
  }

  return (
    <section className="page" data-module="storage">
      <header className="page-head">
        <div>
          <h2>数据落盘管理</h2>
          <p className="page-desc">
            把该批次的结果分片放进提交篮，核对摘要与目标分区后封箱；缺片不能封箱。
            封箱单交付前，增量游标不会拿它推进水位。
          </p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => setErrorMessage('存储配置登记入口尚未接入审批流')}>
            登记存储配置
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出数据落盘清单
          </button>
        </div>
      </header>

      <div className="stat-row">
        <article className="stat-card">
          <span className="stat-label">收分片中的提交篮</span>
          <strong className="stat-value">{baskets.filter((item) => item.state === '收分片中').length}</strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">待交付封箱单</span>
          <strong className="stat-value">{receipts.filter((item) => item.状态 === '待交付').length}</strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">已交付封箱单</span>
          <strong className="stat-value">{receipts.filter((item) => item.状态 === '已交付').length}</strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">落地分区</span>
          <strong className="stat-value">{partitions.length}</strong>
        </article>
      </div>

      <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); void reload() }}>
        {filterFields.map((field) => (
          <label key={field} className="filter-item">
            <span>{field}</span>
            <input
              value={filters[field] ?? ''}
              placeholder={`按${field}检索`}
              onChange={(event) => setFilters((prev) => ({ ...prev, [field]: event.target.value }))}
            />
          </label>
        ))}
        <button className="btn" type="submit">查询</button>
        <button className="btn ghost" type="button" onClick={() => setFilters({})}>重置条件</button>
      </form>

      <table className="data-table">
        <thead>
          <tr>
            {columns.map((column) => <th key={column}>{column}</th>)}
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={String(row.id)}>
              {columns.map((column) => <td key={column}>{row[column] ?? '—'}</td>)}
              <td className="row-actions">
                {actions.map((action) => (
                  <button key={action} className="link" type="button" onClick={() => void runAction(action, row)}>
                    {action}
                  </button>
                ))}
              </td>
            </tr>
          ))}
          {rows.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={columns.length + 1}>暂无数据落盘数据，可先登记存储配置</td>
            </tr>
          )}
        </tbody>
      </table>

      <footer className="page-foot">
        <span>共 {total} 条数据落盘记录</span>
        {errorMessage && <span className="error-text">{errorMessage}</span>}
        {!errorMessage && notice && <span className="notice-text">{notice}</span>}
      </footer>

      <section className="workbench" aria-label="采集批次提交篮">
        <header className="workbench-head">
          <h3>采集批次提交篮</h3>
          <p className="page-desc">
            提交篮创建时复制当时的存储配置；之后编辑配置不会改动在途封箱单。
          </p>
        </header>

        <div className="panel-grid">
          <article className="panel">
            <h4>① 建篮 / 续提</h4>
            <div className="field-grid">
              <label className="field">
                <span>批次标识</span>
                <input
                  value={basketForm.batchId}
                  placeholder="如 BATCH-20261113-01"
                  onChange={(event) => setBasketForm((prev) => ({ ...prev, batchId: event.target.value }))}
                />
              </label>
              <label className="field">
                <span>配置编号</span>
                <input
                  value={basketForm.configCode}
                  onChange={(event) => setBasketForm((prev) => ({ ...prev, configCode: event.target.value }))}
                />
              </label>
              <label className="field">
                <span>预期分片数</span>
                <input
                  value={basketForm.预期分片数}
                  onChange={(event) => setBasketForm((prev) => ({ ...prev, 预期分片数: event.target.value }))}
                />
              </label>
              <label className="field">
                <span>候选水位（可选）</span>
                <input
                  value={basketForm.候选水位}
                  placeholder="来自本批次的采集水位"
                  onChange={(event) => setBasketForm((prev) => ({ ...prev, 候选水位: event.target.value }))}
                />
              </label>
              <label className="field">
                <span>批次摘要（可选）</span>
                <input
                  value={basketForm.批次摘要}
                  placeholder="同一批次标识再次提交时会比对"
                  onChange={(event) => setBasketForm((prev) => ({ ...prev, 批次摘要: event.target.value }))}
                />
              </label>
              <label className="field">
                <span>输出路径（冻结）</span>
                <input
                  value={basketForm.输出路径}
                  onChange={(event) => setBasketForm((prev) => ({ ...prev, 输出路径: event.target.value }))}
                />
              </label>
              <label className="field">
                <span>文件格式（冻结）</span>
                <input
                  value={basketForm.文件格式}
                  onChange={(event) => setBasketForm((prev) => ({ ...prev, 文件格式: event.target.value }))}
                />
              </label>
              <label className="field">
                <span>分区策略（冻结）</span>
                <select
                  value={basketForm.分区策略}
                  onChange={(event) => setBasketForm((prev) => ({ ...prev, 分区策略: event.target.value }))}
                >
                  <option value="按日期">按日期</option>
                  <option value="按站点">按站点</option>
                  <option value="按批次">按批次</option>
                </select>
              </label>
              <label className="field">
                <span>覆写模式（冻结）</span>
                <select
                  value={basketForm.覆写模式}
                  onChange={(event) => setBasketForm((prev) => ({ ...prev, 覆写模式: event.target.value }))}
                >
                  <option value="追加">追加</option>
                  <option value="替换">替换</option>
                  <option value="拒绝">拒绝</option>
                </select>
              </label>
            </div>
            <div className="row-actions">
              <button className="btn primary" type="button" onClick={() => void handleOpenBasket()}>创建提交篮</button>
              <button className="btn" type="button" onClick={() => void handleResume()}>续提（按批次标识找回）</button>
            </div>
          </article>

          <article className="panel">
            <h4>② 分片核对</h4>
            {!activeBasket && <p className="panel-hint">先从左侧创建提交篮，或在批次标识栏点续提找回一个在途篮子。</p>}
            {activeBasket && (
              <>
                <div className="basket-summary">
                  <span>批次 <strong>{activeBasket.批次标识}</strong></span>
                  <span className={`tag tag-${activeBasket.state === '已封箱' ? 'done' : 'open'}`}>{activeBasket.state}</span>
                  <span>已收 {activeBasket.已收到序号.length}/{activeBasket.预期分片数}</span>
                  <span>覆写模式 <strong>{activeBasket.覆写模式}</strong></span>
                </div>
                <div className="freeze-note">
                  冻结配置：{activeBasket.配置快照['输出路径'] as string} · {activeBasket.配置快照['文件格式'] as string} · {
                    activeBasket.配置快照['分区策略'] as string
                  }
                </div>
                {activeBasket.缺片序号.length > 0 && (
                  <div className="warn-banner">缺片序号：{activeBasket.缺片序号.join('、')} —— 缺片不能封箱</div>
                )}
                {activeBasket.冲突序号.length > 0 && (
                  <div className="warn-banner">待裁决冲突：{activeBasket.冲突序号.join('、')} —— 裁决前不能封箱</div>
                )}
                {activeBasket.批次摘要冲突 && (
                  <div className="conflict-box">
                    <h5>批次摘要冲突</h5>
                    <p className="panel-hint">
                      相同批次标识带着不同摘要再次提交。先到的批次内容仍在，后到摘要
                      <span className="mono"> {activeBasket.批次摘要冲突.slice(0, 16)}… </span>
                      只是登记，未替换任何内容。请明确选择保留哪一份。
                    </p>
                    <div className="row-actions">
                      <button className="btn" type="button" onClick={() => void handleBatchDecide('沿用原批次')}>沿用原批次</button>
                      <button className="btn" type="button" onClick={() => void handleBatchDecide('改用新批次')}>改用新批次</button>
                    </div>
                  </div>
                )}

                <table className="data-table compact">
                  <thead>
                    <tr><th>序号</th><th>摘要</th><th>字节</th><th>状态</th></tr>
                  </thead>
                  <tbody>
                    {activeBasket.分片明细.map((shard) => (
                      <tr key={shard.id}>
                        <td>{shard.分片序号}</td>
                        <td className="mono">{shard.分片摘要.slice(0, 16)}…</td>
                        <td>{shard.字节数}</td>
                        <td>
                          {shard.状态}
                          {shard.裁决 && <span className="muted">（{shard.裁决}）</span>}
                        </td>
                      </tr>
                    ))}
                    {activeBasket.分片明细.length === 0 && (
                      <tr><td className="empty-state" colSpan={4}>还没有分片入篮</td></tr>
                    )}
                  </tbody>
                </table>

                {activeBasket.state === '收分片中' && (
                  <div className="field-grid">
                    <label className="field">
                      <span>分片序号</span>
                      <input value={shardIndex} onChange={(event) => setShardIndex(event.target.value)} />
                    </label>
                    <label className="field field-wide">
                      <span>分片内容</span>
                      <textarea
                        rows={2}
                        value={shardContent}
                        placeholder="把这一片的结果贴进来；重复上报按序号识别，不会拼出两份"
                        onChange={(event) => setShardContent(event.target.value)}
                      />
                    </label>
                  </div>
                )}
                <div className="row-actions">
                  {activeBasket.state === '收分片中' && (
                    <>
                      <button className="btn primary" type="button" onClick={() => void handleSubmitShard()}>放入篮中</button>
                      <button
                        className="btn"
                        type="button"
                        disabled={!activeBasket.可封箱}
                        onClick={() => void handleSeal()}
                      >
                        封箱（{activeBasket.可封箱 ? '可执行' : '分片不全'}）
                      </button>
                    </>
                  )}
                  {activeBasket.封箱单号 && <span className="muted">封箱单 {activeBasket.封箱单号}</span>}
                </div>

                {activeBasket.冲突序号.length > 0 && (
                  <div className="conflict-box">
                    <h5>人工裁决</h5>
                    <p className="panel-hint">同号不同摘要必须由人选定保留哪一份，系统不会用后到内容悄悄替换。</p>
                    <div className="field-grid">
                      <label className="field">
                        <span>冲突分片序号</span>
                        <input value={conflictIndex} onChange={(event) => setConflictIndex(event.target.value)} />
                      </label>
                      <label className="field field-wide">
                        <span>后到内容（选择保留后到时填写）</span>
                        <textarea rows={2} value={lateContent} onChange={(event) => setLateContent(event.target.value)} />
                      </label>
                    </div>
                    <div className="row-actions">
                      <button className="btn" type="button" onClick={() => void handleResolve('先到')}>保留先到内容</button>
                      <button className="btn" type="button" onClick={() => void handleResolve('后到')}>保留后到内容</button>
                    </div>
                  </div>
                )}
              </>
            )}
          </article>
        </div>

        <div className="panel-grid">
          <article className="panel">
            <h4>③ 封箱单与交付</h4>
            <table className="data-table compact">
              <thead>
                <tr><th>封箱单号</th><th>批次</th><th>分片</th><th>目标分区</th><th>状态</th><th>操作</th></tr>
              </thead>
              <tbody>
                {receipts.map((receipt) => (
                  <tr key={receipt.id}>
                    <td className="mono">{receipt.封箱单号}</td>
                    <td>{receipt.批次标识}</td>
                    <td>{receipt.分片数}</td>
                    <td className="mono">
                      {receipt.分区号 ?? previewPartition(receipt.配置快照)}
                      <span className="muted">（{receipt.覆写模式}）</span>
                    </td>
                    <td>{receipt.状态}</td>
                    <td className="row-actions">
                      <button className="link" type="button" onClick={() => void handleDeliver(receipt.id)}>
                        {receipt.状态 === '已交付' ? '重送（取原回执）' : '交付'}
                      </button>
                      {receipt.状态 !== '待交付' && (
                        <button className="link" type="button" onClick={() => void showResult(receipt.id)}>读结果</button>
                      )}
                    </td>
                  </tr>
                ))}
                {receipts.length === 0 && (
                  <tr><td className="empty-state" colSpan={6}>还没有封箱单，先收齐分片再封箱</td></tr>
                )}
              </tbody>
            </table>
            {readable && (
              <div className="readable-result">
                <div className="readable-head">
                  <strong>可读结果</strong>
                  <span className="muted">{readable.封箱单号} · 摘要 {readable.结果摘要.slice(0, 16)}…</span>
                </div>
                <pre>{readable.结果内容}</pre>
                <span className="muted">
                  分片 {readable.分片.map((shard) => `${shard.分片序号}#${shard.分片摘要.slice(0, 8)}`).join('  ')}
                </span>
              </div>
            )}
          </article>

          <article className="panel">
            <h4>④ 落地分区</h4>
            <table className="data-table compact">
              <thead>
                <tr><th>分区号</th><th>覆写模式</th><th>版本</th><th>当前封箱单</th></tr>
              </thead>
              <tbody>
                {partitions.map((partition) => (
                  <tr key={partition.id}>
                    <td className="mono">{partition.分区号}</td>
                    <td>{partition.覆写模式}</td>
                    <td>v{partition.当前版本}</td>
                    <td className="mono">{partition.封箱单号}</td>
                  </tr>
                ))}
                {partitions.length === 0 && (
                  <tr><td className="empty-state" colSpan={4}>还没有分区落地</td></tr>
                )}
              </tbody>
            </table>
            <p className="panel-hint">两个封箱单命中同一分区时按冻结的覆写模式决定追加、替换或拒绝；读者只会看到一整版。</p>
          </article>
        </div>
      </section>
    </section>
  )
}

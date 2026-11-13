import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/delta'
const columns = [
  "游标编号",
  "目标站点",
  "增量字段",
  "当前水位",
  "回退步数",
  "批次大小",
  "去重窗口",
  "重跑开关"
]
const actions = [
  "提交校验",
  "启动增量",
  "暂停增量"
]
const statuses = [
  "草稿",
  "待校验",
  "运行中",
  "已暂停"
]

type Row = Record<string, string | number | null>
type ActionResult = {
  ok: boolean
  message: string
  entry?: { id: number; 当前水位?: string; 水位来源封箱单?: string }
}

export default function DeltaView() {
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [errorMessage, setErrorMessage] = useState('')
  const [notice, setNotice] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const [cursorId, setCursorId] = useState('')
  const [receiptNo, setReceiptNo] = useState('')
  const [confirmCrossType, setConfirmCrossType] = useState(false)
  const filterFields = useMemo(() => columns.slice(0, 3), [])

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams(filters).toString()
    try {
      const response = await request(`${ENDPOINT}?${query}`)
      if (!response.ok) {
        throw new Error('增量游标列表读取失败')
      }
      const payload = await response.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '增量采集列表读取失败')
    }
  }, [filters])

  useEffect(() => {
    void reload()
  }, [reload])

  async function runAction(action: string, row: Row) {
    setErrorMessage('')
    setNotice('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}/actions`, {
        method: 'POST',
        body: JSON.stringify({ action }),
      })
      if (!response.ok) {
        throw new Error('增量采集动作未生效，请稍后重试')
      }
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '增量采集操作失败')
    }
  }

  async function promoteWatermark() {
    setErrorMessage('')
    setNotice('')
    const id = cursorId.trim() || String(rows[0]?.id ?? '')
    if (!id) {
      setErrorMessage('请先指定要推进的游标')
      return
    }
    const response = await request(`${ENDPOINT}/${id}/watermark`, {
      method: 'POST',
      body: JSON.stringify({
        values: {
          封箱单号: receiptNo.trim(),
          确认跨类型推进: confirmCrossType,
        },
      }),
    })
    const payload = (await response.json().catch(() => ({}))) as ActionResult & { detail?: string }
    if (!response.ok || !payload.ok) {
      setErrorMessage(payload.message ?? payload.detail ?? '水位推进失败')
      return
    }
    setNotice(payload.message)
    await reload()
  }

  return (
    <section className="page" data-module="delta">
      <header className="page-head">
        <div>
          <h2>增量采集管理</h2>
          <p className="page-desc">
            游标水位只接受「已交付封箱单」里的候选水位；上传进度和已收分片数都不能当作采集完成。
          </p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => setErrorMessage('增量游标登记入口尚未接入审批流')}>
            登记增量游标
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出增量采集清单
          </button>
        </div>
      </header>

      <div className="stat-row">
        <article className="stat-card">
          <span className="stat-label">增量游标总数</span>
          <strong className="stat-value">{total}</strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">运行中游标</span>
          <strong className="stat-value">{rows.filter((row) => row.status === '运行中').length}</strong>
        </article>
        <article className="stat-card">
          <span className="stat-label">待校验游标</span>
          <strong className="stat-value">{rows.filter((row) => row.status === '待校验').length}</strong>
        </article>
      </div>

      <article className="panel">
        <h4>用封箱单推进水位</h4>
        <p className="panel-hint">
          必须引用一份已交付的封箱单。封箱单未交付、候选水位低于当前水位、或两边格式对不上时都会被拦下。
        </p>
        <div className="field-grid">
          <label className="field">
            <span>游标 id</span>
            <input value={cursorId} placeholder={rows[0] ? String(rows[0].id) : '如 1'} onChange={(event) => setCursorId(event.target.value)} />
          </label>
          <label className="field">
            <span>封箱单号</span>
            <input value={receiptNo} placeholder="如 BOX-000001" onChange={(event) => setReceiptNo(event.target.value)} />
          </label>
          <label className="field">
            <span>跨格式推进</span>
            <input
              type="checkbox"
              checked={confirmCrossType}
              onChange={(event) => setConfirmCrossType(event.target.checked)}
            />
          </label>
        </div>
        <div className="row-actions">
          <button className="btn primary" type="button" onClick={() => void promoteWatermark()}>推进游标水位</button>
        </div>
        {notice && <p className="notice-text">{notice}</p>}
        {errorMessage && <p className="error-text">{errorMessage}</p>}
      </article>

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
            <th>水位来源封箱单</th>
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={String(row.id)}>
              {columns.map((column) => <td key={column}>{row[column] ?? '—'}</td>)}
              <td className="mono">{row.水位来源封箱单 ?? '—'}</td>
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
              <td className="empty-state" colSpan={columns.length + 2}>暂无增量采集数据，可先登记增量游标</td>
            </tr>
          )}
        </tbody>
      </table>

      <footer className="page-foot">
        <span>共 {total} 条增量采集记录</span>
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}

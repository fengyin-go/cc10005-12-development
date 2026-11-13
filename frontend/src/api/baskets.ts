/**
 * 采集批次提交篮的接口封装：分片、冲突裁决、封箱、交付、可读结果、分区。
 * 后端返回 { ok, message, entry }，这里统一转成带中文提示的结果对象。
 */
const STORAGE_ENDPOINT = '/api/storage'

export type ShardView = {
  id: number
  分片序号: number
  分片摘要: string
  字节数: number
  状态: string
  冲突摘要?: string | null
  裁决?: string
  裁决人?: string
}

export type BasketView = {
  id: number
  批次标识: string
  配置编号: string
  state: string
  预期分片数: number
  收到分片数: number
  候选水位: string | null
  覆写模式: string
  配置快照: Record<string, unknown>
  分片明细: ShardView[]
  批次摘要?: string | null
  批次摘要冲突?: string | null
  批次裁决?: string
  已收到序号: number[]
  冲突序号: number[]
  缺片序号: number[]
  可封箱: boolean
  创建时间: string
  封箱时间: string | null
  封箱单号: string | null
}

export type ReceiptView = {
  id: number
  封箱单号: string
  篮号: number
  批次标识: string
  配置编号: string
  配置快照: Record<string, unknown>
  覆写模式: string
  候选水位: string | null
  封箱摘要: string
  分片数: number
  状态: string
  封箱时间: string
  交付时间: string | null
  分区号: string | null
  回执: Record<string, unknown> | null
}

export type PartitionView = {
  id: number
  分区号: string
  配置编号: string
  覆写模式: string
  当前版本: number
  结果摘要: string | null
  封箱单号: string | null
  更新时间: string
}

export type ReadableResult = {
  封箱单号: string
  批次标识: string
  配置编号: string
  配置快照: Record<string, unknown>
  分片: { 分片序号: number; 分片摘要: string; 字节数: number }[]
  结果内容: string
  结果摘要: string
}

export type ActionOutcome<T> = { ok: boolean; message: string; data?: T }

async function readJson(response: Response): Promise<Record<string, unknown>> {
  try {
    return (await response.json()) as Record<string, unknown>
  } catch {
    return {}
  }
}

async function callAction<T>(path: string, method: string, values?: Record<string, unknown>): Promise<ActionOutcome<T>> {
  const response = await fetch(path, {
    method,
    headers: { 'Content-Type': 'application/json' },
    body: values ? JSON.stringify({ values }) : undefined,
  })
  const payload = await readJson(response)
  if (!response.ok) {
    const detail = typeof payload.detail === 'string' ? payload.detail : `接口返回 ${response.status}`
    return { ok: false, message: detail }
  }
  return {
    ok: Boolean(payload.ok),
    message: String(payload.message ?? ''),
    data: (payload.entry ?? payload) as T,
  }
}

async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(path)
  const payload = await readJson(response)
  if (!response.ok) {
    const detail = typeof payload.detail === 'string' ? payload.detail : `接口返回 ${response.status}`
    throw new Error(detail)
  }
  return payload as T
}

export async function fetchBaskets(): Promise<BasketView[]> {
  const payload = await getJson<{ items: BasketView[] }>(`${STORAGE_ENDPOINT}/baskets`)
  return payload.items ?? []
}

export async function fetchBasketByBatch(batchId: string): Promise<BasketView> {
  return getJson<BasketView>(`${STORAGE_ENDPOINT}/baskets/by-batch/${encodeURIComponent(batchId)}`)
}

export async function openBasket(values: Record<string, unknown>) {
  return callAction<BasketView>(`${STORAGE_ENDPOINT}/baskets`, 'POST', values)
}

export async function submitShard(basketId: number, values: Record<string, unknown>) {
  return callAction<BasketView>(`${STORAGE_ENDPOINT}/baskets/${basketId}/shards`, 'POST', values)
}

export async function resolveShardConflict(basketId: number, values: Record<string, unknown>) {
  return callAction<BasketView>(`${STORAGE_ENDPOINT}/baskets/${basketId}/conflicts`, 'POST', values)
}

export async function resolveBatchConflict(basketId: number, values: Record<string, unknown>) {
  return callAction<BasketView>(`${STORAGE_ENDPOINT}/baskets/${basketId}/batch-conflicts`, 'POST', values)
}

export async function sealBasket(basketId: number) {
  return callAction<ReceiptView>(`${STORAGE_ENDPOINT}/baskets/${basketId}/seal`, 'POST')
}

export async function fetchReceipts(): Promise<ReceiptView[]> {
  const payload = await getJson<{ items: ReceiptView[] }>(`${STORAGE_ENDPOINT}/receipts`)
  return payload.items ?? []
}

export async function deliverReceipt(receiptId: number, targetPartition?: string) {
  return callAction<ReceiptView>(
    `${STORAGE_ENDPOINT}/receipts/${receiptId}/deliver`,
    'POST',
    targetPartition ? { 目标分区: targetPartition } : {},
  )
}

export async function fetchReadableResult(receiptId: number): Promise<ReadableResult> {
  return getJson<ReadableResult>(`${STORAGE_ENDPOINT}/receipts/${receiptId}/result`)
}

export async function fetchPartitions(): Promise<PartitionView[]> {
  const payload = await getJson<{ items: PartitionView[] }>(`${STORAGE_ENDPOINT}/partitions`)
  return payload.items ?? []
}

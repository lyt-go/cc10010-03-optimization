/** 统一请求封装：拼后端地址、抛网络错误、给页脚留一句可读的说明。 */
const API_BASE = import.meta.env.VITE_API_BASE ?? ''

export function request(path: string, init?: RequestInit): Promise<Response> {
  const url = path.startsWith('http') ? path : `${API_BASE}${path}`
  return fetch(url, {
    headers: { 'Content-Type': 'application/json' },
    ...init,
  }).catch((error: unknown) => {
    const detail = error instanceof Error ? error.message : '请求未送达'
    throw new Error(`接口请求失败：${detail}`)
  })
}

export async function fetchJson<T>(path: string): Promise<T> {
  const response = await request(path)
  if (!response.ok) {
    throw new Error(`接口返回 ${response.status}，数据未更新`)
  }
  return (await response.json()) as T
}

/** 带超时与重试的下载：超时或失败自动重试，最终成功才落盘，失败时抛出可读错误。 */
export async function downloadWithRetry(
  path: string,
  filename: string,
  options: { timeoutMs?: number; retries?: number } = {},
): Promise<void> {
  const { timeoutMs = 10000, retries = 2 } = options
  let lastError: unknown = null
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(), timeoutMs)
    try {
      const response = await request(path, { signal: controller.signal })
      if (!response.ok) {
        throw new Error(`下载返回 ${response.status}`)
      }
      const blob = await response.blob()
      const url = URL.createObjectURL(blob)
      const anchor = document.createElement('a')
      anchor.href = url
      anchor.download = filename
      anchor.click()
      URL.revokeObjectURL(url)
      return
    } catch (error) {
      lastError = error
    } finally {
      clearTimeout(timer)
    }
  }
  const detail = lastError instanceof Error ? lastError.message : '下载失败'
  throw new Error(`下载超时或失败，已重试 ${retries} 次：${detail}`)
}

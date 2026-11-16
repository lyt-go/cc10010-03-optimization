<template>
  <section class="page" data-module="etljob">
    <header class="page-head">
      <div>
        <h2>ETL作业管理</h2>
        <p class="page-desc">管理ETL作业的定义，包括源端目标端映射、执行逻辑和调度配置。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记作业定义</button>
        <button class="btn" type="button" @click="exportRows">导出ETL作业清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无ETL作业数据，可先登记作业定义</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条ETL作业记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>

    <section class="panel">
      <h3>变更批次提交</h3>
      <p class="panel-desc">
        从ETL作业变更批次按实际关联字段（源端数据表、目标数据表）重算影响范围，只把真实受影响的依赖标为待复核。
        相同批次反复提交幂等跳过；填写「中断位置」可模拟批次写入中断，再次提交同一批次号即可断点补写。
      </p>
      <form class="filter-bar" @submit.prevent="submitBatch">
        <label class="filter-item">
          <span>批次编号</span>
          <input v-model="batchForm.批次编号" placeholder="如 CHG-20261116-01" />
        </label>
        <label class="filter-item">
          <span>轮次序号</span>
          <input v-model="batchForm.轮次序号" type="number" min="1" placeholder="变更轮次" />
        </label>
        <label class="filter-item">
          <span>中断位置（可选）</span>
          <input v-model="batchForm.中断位置" type="number" min="0" placeholder="模拟写入中断" />
        </label>
        <button class="btn primary" type="submit">提交变更批次</button>
        <button class="btn ghost" type="button" @click="fillSample">填入示例</button>
      </form>
      <textarea
        v-model="batchItemsText"
        class="json-editor"
        rows="6"
        placeholder='变更项JSON数组，如 [{"作业编号":"ETLJ-0001","变更后源端数据表":"ods_orders","变更后目标数据表":"dwd_order_detail_v2","变更后作业版本":"v1.1"}]'
      ></textarea>
      <p v-if="batchMessage" :class="batchOk ? 'ok-text' : 'error-text'">{{ batchMessage }}</p>
    </section>

    <section class="panel">
      <h3>变更批次与影响重算</h3>
      <table class="data-table">
        <thead>
          <tr>
            <th v-for="column in batchColumns" :key="column">{{ column }}</th>
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="batch in batches" :key="String(batch.批次编号)">
            <td v-for="column in batchColumns" :key="column">
              <span v-if="column === '状态'" class="tag" :class="batch.状态 === '部分写入' ? 'warn' : 'ok'">
                {{ batch[column] }}
              </span>
              <template v-else>{{ batch[column] ?? '—' }}</template>
            </td>
            <td class="row-actions">
              <button class="link" type="button" @click="recalcBatch(batch)">重算影响</button>
              <button class="link" type="button" @click="generateExport(batch)">生成导出文件</button>
              <button class="link" type="button" @click="downloadExport(batch, 'csv')">下载CSV</button>
              <button class="link" type="button" @click="downloadExport(batch, 'zip')">打包下载</button>
              <button class="link" type="button" @click="showImpact(batch)">查看影响</button>
            </td>
          </tr>
          <tr v-if="!batches.length">
            <td :colspan="batchColumns.length + 1" class="empty-state">暂无变更批次，可先提交变更批次</td>
          </tr>
        </tbody>
      </table>
      <footer class="page-foot">
        <span>共 {{ batchTotal }} 个变更批次</span>
        <span v-if="batchActionMessage" class="ok-text">{{ batchActionMessage }}</span>
      </footer>
    </section>

    <section v-if="impactBatchNo" class="panel">
      <h3>受影响依赖清单（批次 {{ impactBatchNo }}）</h3>
      <p class="panel-desc">与依赖解析模块按同一批次号取同一份受影响依赖数据快照，作业版本逐条对应。</p>
      <table class="data-table">
        <thead>
          <tr>
            <th v-for="column in impactColumns" :key="column">{{ column }}</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in impactRows" :key="String(row.id)">
            <td v-for="column in impactColumns" :key="column">
              <span v-if="column === '复核状态'" class="tag" :class="row.复核状态 === '待复核' ? 'warn' : 'ok'">
                {{ row[column] }}
              </span>
              <template v-else>{{ row[column] ?? '—' }}</template>
            </td>
          </tr>
          <tr v-if="!impactRows.length">
            <td :colspan="impactColumns.length" class="empty-state">该批次没有受影响依赖</td>
          </tr>
        </tbody>
      </table>
    </section>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { downloadWithRetry, request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/etljob'
const columns = ["作业编号", "作业名称", "源端数据表", "目标数据表", "执行SQL", "调度表达式", "超时分钟", "失败重试次数"]
const actions = ["提交作业评审", "发布作业", "下线作业"]
const statuses = ["草稿", "作业评审中", "已发布", "已下线"]
const stats = [{"label": "作业定义总数", "value": 0}, {"label": "已发布作业", "value": 0}, {"label": "评审中作业", "value": 0}]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)

const batchColumns = ["批次编号", "轮次序号", "状态", "变更项数", "受影响依赖数", "待复核数", "提交时间"]
const impactColumns = ["关系编号", "上游作业", "下游作业", "命中方向", "变更前源端数据表", "变更后源端数据表", "变更前目标数据表", "变更后目标数据表", "作业版本", "复核状态", "来源"]

const batches = ref<Row[]>([])
const batchTotal = ref(0)
const batchForm = ref<Record<string, string>>({ 批次编号: '', 轮次序号: '', 中断位置: '' })
const batchItemsText = ref('')
const batchMessage = ref('')
const batchOk = ref(false)
const batchActionMessage = ref('')
const impactBatchNo = ref('')
const impactRows = ref<Row[]>([])

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '作业定义登记入口尚未接入审批流'
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('ETL作业动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : 'ETL作业操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('作业定义列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : 'ETL作业列表读取失败'
  }
}

function fillSample() {
  batchForm.value = { 批次编号: 'CHG-20261116-01', 轮次序号: '2', 中断位置: '' }
  batchItemsText.value = JSON.stringify(
    [
      {
        作业编号: 'ETLJ-0001',
        作业名称: '订单明细同步作业',
        变更前源端数据表: 'ods_orders',
        变更后源端数据表: 'ods_orders',
        变更前目标数据表: 'dwd_order_detail',
        变更后目标数据表: 'dwd_order_detail_v2',
        变更前作业版本: 'v1.0',
        变更后作业版本: 'v1.1',
      },
    ],
    null,
    2,
  )
}

async function submitBatch() {
  batchMessage.value = ''
  let items: unknown
  try {
    items = JSON.parse(batchItemsText.value || '[]')
  } catch {
    batchOk.value = false
    batchMessage.value = '变更项不是合法的JSON数组，请检查后再提交'
    return
  }
  const values: Record<string, unknown> = {
    批次编号: batchForm.value.批次编号,
    轮次序号: batchForm.value.轮次序号,
    items,
  }
  if (batchForm.value.中断位置 !== '') {
    values.中断位置 = batchForm.value.中断位置
  }
  try {
    const response = await request(`${ENDPOINT}/change-batches`, {
      method: 'POST',
      body: JSON.stringify({ values }),
    })
    const payload = await response.json()
    batchOk.value = Boolean(payload.ok)
    batchMessage.value = payload.message ?? (payload.ok ? '批次已提交' : '批次提交失败')
    if (payload.ok) {
      await reloadBatches()
      await reload()
    }
  } catch (error) {
    batchOk.value = false
    batchMessage.value = error instanceof Error ? error.message : '变更批次提交失败'
  }
}

async function reloadBatches() {
  try {
    const response = await request(`${ENDPOINT}/change-batches?size=100`)
    if (!response.ok) {
      throw new Error('变更批次列表读取失败')
    }
    const payload = await response.json()
    batches.value = payload.items ?? []
    batchTotal.value = payload.total ?? batches.value.length
  } catch (error) {
    batchActionMessage.value = error instanceof Error ? error.message : '变更批次列表读取失败'
  }
}

async function recalcBatch(batch: Row) {
  batchActionMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/change-batches/${encodeURIComponent(String(batch.批次编号))}/recalc`, {
      method: 'POST',
      body: JSON.stringify({ values: {} }),
    })
    const payload = await response.json()
    batchActionMessage.value = payload.message ?? ''
    await reloadBatches()
    if (impactBatchNo.value === batch.批次编号) {
      await showImpact(batch)
    }
  } catch (error) {
    batchActionMessage.value = error instanceof Error ? error.message : '影响重算失败'
  }
}

async function generateExport(batch: Row) {
  batchActionMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/change-batches/${encodeURIComponent(String(batch.批次编号))}/export`, {
      method: 'POST',
      body: JSON.stringify({ values: {} }),
    })
    const payload = await response.json()
    batchActionMessage.value = payload.message ?? ''
  } catch (error) {
    batchActionMessage.value = error instanceof Error ? error.message : '导出文件生成失败'
  }
}

async function downloadExport(batch: Row, kind: 'csv' | 'zip') {
  batchActionMessage.value = ''
  const batchNo = String(batch.批次编号)
  const suffix = kind === 'csv' ? 'download' : 'package'
  const filename = `change-impact-${batchNo}.${kind}`
  try {
    await downloadWithRetry(
      `${ENDPOINT}/change-batches/${encodeURIComponent(batchNo)}/export/${suffix}`,
      filename,
      { timeoutMs: 10000, retries: 2 },
    )
    batchActionMessage.value = `批次 ${batchNo} 的${kind === 'csv' ? '变更导出文件' : '打包文件'}已下载`
  } catch (error) {
    batchActionMessage.value = error instanceof Error ? error.message : '下载失败'
  }
}

async function showImpact(batch: Row) {
  const batchNo = String(batch.批次编号)
  impactBatchNo.value = batchNo
  impactRows.value = []
  try {
    const response = await request(`${ENDPOINT}/change-batches/${encodeURIComponent(batchNo)}/impact?size=200`)
    if (!response.ok) {
      throw new Error('受影响依赖清单读取失败')
    }
    const payload = await response.json()
    impactRows.value = payload.items ?? []
  } catch (error) {
    batchActionMessage.value = error instanceof Error ? error.message : '受影响依赖清单读取失败'
  }
}

onMounted(() => {
  void reload()
  void reloadBatches()
})
</script>

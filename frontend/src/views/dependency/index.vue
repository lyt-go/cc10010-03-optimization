<template>
  <section class="page" data-module="dependency">
    <header class="page-head">
      <div>
        <h2>依赖解析管理</h2>
        <p class="page-desc">管理ETL作业间的依赖关系，定义时间偏移、触发条件和等待策略。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记依赖关系</button>
        <button class="btn" type="button" @click="exportRows">导出依赖解析清单</button>
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
          <td :colspan="columns.length + 1" class="empty-state">暂无依赖解析数据，可先登记依赖关系</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条依赖解析记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>

    <section class="panel">
      <h3>受影响依赖清单</h3>
      <p class="panel-desc">
        与ETL作业变更导出按同一批次号取同一份受影响依赖数据快照；确认后的受影响依赖不会被后续批次重算回退。
      </p>
      <form class="filter-bar" @submit.prevent="reloadImpact">
        <label class="filter-item">
          <span>批次编号</span>
          <input v-model="impactFilters.batch_no" placeholder="按批次编号过滤" />
        </label>
        <label class="filter-item">
          <span>复核状态</span>
          <input v-model="impactFilters.review" placeholder="待复核、已确认" />
        </label>
        <button class="btn" type="submit">查询</button>
        <button class="btn ghost" type="button" @click="resetImpactFilters">重置条件</button>
      </form>
      <table class="data-table">
        <thead>
          <tr>
            <th v-for="column in impactColumns" :key="column">{{ column }}</th>
            <th>可执行动作</th>
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
            <td class="row-actions">
              <button
                v-if="row.复核状态 === '待复核'"
                class="link"
                type="button"
                @click="confirmImpact(row)"
              >
                确认复核
              </button>
              <span v-else class="tag muted">已确认</span>
            </td>
          </tr>
          <tr v-if="!impactRows.length">
            <td :colspan="impactColumns.length + 1" class="empty-state">暂无受影响依赖记录</td>
          </tr>
        </tbody>
      </table>
      <footer class="page-foot">
        <span>共 {{ impactTotal }} 条受影响依赖记录</span>
        <span v-if="impactMessage" class="ok-text">{{ impactMessage }}</span>
      </footer>
    </section>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/dependency'
const columns = ["关系编号", "上游作业", "下游作业", "依赖类型", "偏移量分钟", "触发条件", "等待超时", "跳过标志"]
const actions = ["提交关系评审", "生效关系", "解除关系"]
const statuses = ["草稿", "关系评审中", "已生效", "已解除"]
const stats = [{"label": "依赖关系总数", "value": 0}, {"label": "已生效关系", "value": 0}, {"label": "评审中关系", "value": 0}]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)

const impactColumns = ["批次编号", "关系编号", "上游作业", "下游作业", "命中方向", "变更前源端数据表", "变更后源端数据表", "变更前目标数据表", "变更后目标数据表", "作业版本", "复核状态", "来源"]
const impactRows = ref<Row[]>([])
const impactTotal = ref(0)
const impactMessage = ref('')
const impactFilters = ref<Record<string, string>>({ batch_no: '', review: '' })

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '依赖关系登记入口尚未接入审批流'
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('依赖解析动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '依赖解析操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('依赖关系列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '依赖解析列表读取失败'
  }
}

function resetImpactFilters() {
  impactFilters.value = { batch_no: '', review: '' }
  void reloadImpact()
}

async function reloadImpact() {
  impactMessage.value = ''
  const params: Record<string, string> = { size: '100' }
  if (impactFilters.value.batch_no) {
    params.batch_no = impactFilters.value.batch_no
  }
  if (impactFilters.value.review) {
    params.review = impactFilters.value.review
  }
  const query = new URLSearchParams(params).toString()
  try {
    const response = await request(`${ENDPOINT}/impact-list?${query}`)
    if (!response.ok) {
      throw new Error('受影响依赖清单读取失败')
    }
    const payload = await response.json()
    impactRows.value = payload.items ?? []
    impactTotal.value = payload.total ?? impactRows.value.length
  } catch (error) {
    impactMessage.value = error instanceof Error ? error.message : '受影响依赖清单读取失败'
  }
}

async function confirmImpact(row: Row) {
  impactMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/impact-list/${row.id}/confirm`, {
      method: 'POST',
      body: JSON.stringify({ values: {} }),
    })
    const payload = await response.json()
    impactMessage.value = payload.message ?? ''
    await reloadImpact()
  } catch (error) {
    impactMessage.value = error instanceof Error ? error.message : '确认复核失败'
  }
}

onMounted(() => {
  void reload()
  void reloadImpact()
})
</script>

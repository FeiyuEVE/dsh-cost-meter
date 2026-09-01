/**
 * dsh-cost-meter 调用明细 SQLite 存储(issue #78)。
 *
 * 每次 llm/stream 计费入账(ledger.account)的同时写入一行调用明细,字段:
 * 工作区(会话 cwd)/会话 id/会话标题/时间/token 各桶/成本(统一人民币口径)。
 * 页面整体统计(today/month/total/history、当日会话、跨日会话排行)以本库为
 * 数据源;账本 ledger.json 继续承担会话投影、Plan 统计与余额对账。
 *
 * 兼容性:
 *  - 升级前历史:启动与手动导入后,把 ledger.days 的会话聚合以
 *    aggregate=1 的行导入(INSERT OR IGNORE + (day_key, session_id) 部分唯一
 *    索引,幂等),meta JSON 保留 byProviderModel(成本折算为人民币);
 *  - 行级成本一律存人民币(cost_rmb / api_cost_rmb),下发 wire 时按当前展示
 *    汇率折算回美元,客户端展示管道(× exchangeRate)往返回到人民币。
 *
 * 打开校验:application_id 防错库;user_version 不匹配时整库重置(派生索引,
 * 可从账本重建,无需保全)。
 */

import { mkdirSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { resolveDshHome } from '@deepseek-ai/dsh-home-paths'
import { localDayKey } from './store.js'
import { providerPriceEntryFor, costOf, resolveCurrency, rmbFromCost, isLocalOriginProviderOrModel } from './pricing.js'
import { billingClassOf, enabledPlanSetOf } from './plan-billing.js'

// 宿主进程统一用 node:sqlite 内置驱动(与 dsh-session-query-sqlite 同源);
// 不可用(旧 Node)时模块加载即响亮失败——调用明细是页面统计的唯一数据源。
let DatabaseSync
try {
  ;({ DatabaseSync } = await import('node:sqlite'))
} catch (error) {
  throw new Error(`node:sqlite 不可用(Node >= 22.5 才支持调用明细库): ${String(error?.message ?? error)}`)
}

/** 本库的 application_id('DCM1'),防止误开其它应用的数据库。 */
const CALLS_DB_APPLICATION_ID = 0x44434D31

/** 当前 schema 版本;不兼容升级时整库重置(可从账本重新导入)。 */
const CALLS_DB_SCHEMA_VERSION = 1

/** 调用明细库路径:$DSH_HOME/storages/cost-meter/calls.sqlite。 */
export function callsDbPath() {
  return join(resolveDshHome(), 'storages', 'cost-meter', 'calls.sqlite')
}

/** 把 byProviderModel 的成本字段(USD)折算为人民币(迁移时一次性转换)。 */
function rmbMeta(by, rate) {
  const out = {}
  if (by === null || typeof by !== 'object') return out
  for (const [key, b] of Object.entries(by)) {
    if (b === null || typeof b !== 'object') continue
    out[key] = {
      input: Number(b.input) || 0,
      output: Number(b.output) || 0,
      cacheRead: Number(b.cacheRead) || 0,
      cacheWrite: Number(b.cacheWrite) || 0,
      reasoning: Number(b.reasoning) || 0,
      calls: Number(b.calls) || 0,
      cost: (Number(b.cost) || 0) * rate,
      apiCost: (Number(b.apiCost) ?? Number(b.cost) ?? 0) * rate,
    }
  }
  return out
}

/** 有效汇率;非法(<=0 / 非有限数)按 1 兜底,与计费侧同规则。 */
function safeRate(rate) {
  const r = Number(rate)
  return Number.isFinite(r) && r > 0 ? r : 1
}

/**
 * SQLite 调用明细库。
 * 所有统计在 JS 内聚合(行级数据有界:保留窗口内天数 × 每日调用数),
 * SQL 只承担按日/会话过滤与排序。
 */
export class CallsDb {
  /**
   * @param db - node:sqlite DatabaseSync 句柄。
   * @param path - 数据库文件路径(仅日志用)。
   */
  constructor(db, path) {
    this.db = db
    this.path = path
    this.closed = false
    this.insertLive = db.prepare(`
      INSERT INTO calls (
        workspace, session_id, session_title, provider, model, at_ms,
        day_key, month_key, input_tokens, output_tokens, cache_read_tokens,
        cache_write_tokens, reasoning_tokens, cost_rmb, api_cost_rmb,
        aggregate, meta
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, '{}')
    `)
    this.insertAggregate = db.prepare(`
      INSERT OR IGNORE INTO calls (
        workspace, session_id, session_title, provider, model, at_ms,
        day_key, month_key, input_tokens, output_tokens, cache_read_tokens,
        cache_write_tokens, reasoning_tokens, cost_rmb, api_cost_rmb,
        aggregate, meta
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
    `)
    this.selectBetween = db.prepare(`
      SELECT * FROM calls WHERE day_key >= ? AND day_key <= ?
      ORDER BY day_key, at_ms, id
    `)
    this.selectAll = db.prepare('SELECT * FROM calls ORDER BY day_key, at_ms, id')
    this.deleteAll = db.prepare('DELETE FROM calls')
    // 去重感知导入(importLedgerDayMissing):按 (day_key, session_id) 判重。
    this.selectPair = db.prepare('SELECT 1 AS hit FROM calls WHERE day_key = ? AND session_id = ? LIMIT 1')
    this.updateTitle = db.prepare("UPDATE calls SET session_title = ? WHERE day_key = ? AND session_id = ? AND session_title = ''")
  }

  /**
   * 打开(必要时创建)调用明细库,校验/初始化 schema。
   * @param path - 数据库路径;缺省 $DSH_HOME/storages/cost-meter/calls.sqlite。
   * @returns 就绪的 CallsDb。
   */
  static open(path = callsDbPath()) {
    const actual = path
    if (actual !== ':memory:') {
      mkdirSync(dirname(actual), { recursive: true, mode: 0o700 })
    }
    const db = new DatabaseSync(actual)
    try {
      const { application_id: appId } = db.prepare('PRAGMA application_id').get()
      const { user_version: version } = db.prepare('PRAGMA user_version').get()
      const tables = db.prepare(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT GLOB 'sqlite_*' ORDER BY name",
      ).all().map(row => row.name)
      if (appId !== 0 && appId !== CALLS_DB_APPLICATION_ID) {
        throw new Error(`calls database at "${actual}" belongs to another application`)
      }
      if (appId === CALLS_DB_APPLICATION_ID && version !== CALLS_DB_SCHEMA_VERSION) {
        for (const name of tables) db.exec(`DROP TABLE IF EXISTS "${name.replaceAll('"', '""')}"`)
        db.exec('PRAGMA user_version = 0')
      }
      db.exec('PRAGMA journal_mode = WAL')
      // 多 profile 共用同一库(web/acp/headless 进程可并发运行,DSH home 是
      // 单根):WAL 下同一时刻只有一个写者,busy_timeout 让写等待而非抛
      // SQLITE_BUSY,避免并发写互相丢记录。
      db.exec('PRAGMA busy_timeout = 5000')
      db.exec(`PRAGMA application_id = ${CALLS_DB_APPLICATION_ID}`)
      db.exec(`
        CREATE TABLE IF NOT EXISTS calls (
          id                INTEGER PRIMARY KEY AUTOINCREMENT,
          workspace         TEXT NOT NULL DEFAULT '',
          session_id        TEXT NOT NULL DEFAULT '',
          session_title     TEXT NOT NULL DEFAULT '',
          provider          TEXT NOT NULL DEFAULT '',
          model             TEXT NOT NULL DEFAULT '',
          at_ms             INTEGER NOT NULL,
          day_key           TEXT NOT NULL,
          month_key         TEXT NOT NULL,
          input_tokens      INTEGER NOT NULL DEFAULT 0,
          output_tokens     INTEGER NOT NULL DEFAULT 0,
          cache_read_tokens INTEGER NOT NULL DEFAULT 0,
          cache_write_tokens INTEGER NOT NULL DEFAULT 0,
          reasoning_tokens  INTEGER NOT NULL DEFAULT 0,
          cost_rmb          REAL NOT NULL DEFAULT 0,
          api_cost_rmb      REAL NOT NULL DEFAULT 0,
          aggregate         INTEGER NOT NULL DEFAULT 0,
          meta              TEXT NOT NULL DEFAULT '{}'
        ) STRICT
      `)
      db.exec('CREATE INDEX IF NOT EXISTS idx_calls_day ON calls(day_key, at_ms)')
      db.exec('CREATE INDEX IF NOT EXISTS idx_calls_session ON calls(session_id, at_ms)')
      // 迁移行按 (day_key, session_id) 幂等;实时行(aggregate=0)不受限。
      db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_calls_agg_session
        ON calls(day_key, session_id) WHERE aggregate = 1`)
      db.exec(`PRAGMA user_version = ${CALLS_DB_SCHEMA_VERSION}`)
      return new CallsDb(db, actual)
    } catch (error) {
      db.close()
      throw error
    }
  }

  /** 记入一次实时调用(aggregate=0)。 */
  record({ workspace = '', sessionId = '', title = '', provider = '', model = '', atMs, buckets, costRmb, apiCostRmb }) {
    if (this.closed) return
    const at = Number(atMs)
    const atSafe = Number.isFinite(at) && at > 0 ? at : Date.now()
    const dayKey = localDayKey(atSafe)
    const num = value => {
      const n = Number(value)
      return Number.isFinite(n) && n > 0 ? Math.floor(n) : 0
    }
    this.insertLive.run(
      String(workspace ?? ''),
      String(sessionId ?? ''),
      String(title ?? ''),
      String(provider ?? ''),
      String(model ?? ''),
      atSafe,
      dayKey,
      dayKey.slice(0, 7),
      num(buckets?.input),
      num(buckets?.output),
      num(buckets?.cacheRead),
      num(buckets?.cacheWrite),
      num(buckets?.reasoning),
      Math.max(0, Number(costRmb) || 0),
      Math.max(0, Number(apiCostRmb) || 0),
    )
  }

  /**
   * 导入账本某一天的会话聚合为迁移行(aggregate=1,幂等)。
   * 成本按当前展示汇率折算为人民币;byProviderModel 存 meta JSON(已折算)。
   * 无会话的残余调用(day.calls 超出会话之和的部分)记一条 session_id='' 行,
   * 保证日合计与账本一致。
   * @param day - 账本单日记录。
   * @param rate - 展示汇率(美元 → 人民币)。
   */
  importLedgerDay(day, rate) {
    if (this.closed || day === null || typeof day !== 'object') return
    const date = String(day.date ?? '')
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return
    const r = safeRate(rate)
    const month = date.slice(0, 7)
    const num = value => Math.floor(Math.max(0, Number(value) || 0))
    const sessions = Array.isArray(day.sessions) ? day.sessions : []
    let sessionCalls = 0
    let sessionCost = 0
    let sessionApiCost = 0
    const sums = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, reasoning: 0 }
    for (const s of sessions) {
      if (s === null || typeof s !== 'object' || typeof s.id !== 'string' || s.id.length === 0) continue
      const calls = num(s.calls)
      const cost = Number(s.cost) || 0
      sessionCalls += calls
      sessionCost += cost
      sessionApiCost += Number(s.apiCost ?? s.cost) || 0
      sums.input += num(s.input); sums.output += num(s.output)
      sums.cacheRead += num(s.cacheRead); sums.cacheWrite += num(s.cacheWrite)
      sums.reasoning += num(s.reasoning)
      this.insertAggregateRow(date, month, s, calls, cost, r, '')
    }
    const dayCalls = num(day.calls)
    const residual = dayCalls - sessionCalls
    if (residual > 0) {
      const meta = JSON.stringify({ calls: residual, byProviderModel: {} })
      this.insertAggregate.run(
        '', '', '', '', '',
        0, date, month,
        Math.max(0, num(day.input) - sums.input),
        Math.max(0, num(day.output) - sums.output),
        Math.max(0, num(day.cacheRead) - sums.cacheRead),
        Math.max(0, num(day.cacheWrite) - sums.cacheWrite),
        Math.max(0, num(day.reasoning) - sums.reasoning),
        Math.max(0, ((Number(day.cost) || 0) - sessionCost) * r),
        Math.max(0, ((Number(day.apiCost ?? day.cost) || 0) - sessionApiCost) * r),
        meta,
      )
    }
  }

  /** 会话聚合行的公共插入(byProviderModel 折算为人民币存 meta)。 */
  insertAggregateRow(date, month, s, calls, cost, r, workspace) {
    const meta = JSON.stringify({ calls, byProviderModel: rmbMeta(s.byProviderModel, r) })
    this.insertAggregate.run(
      workspace, s.id, typeof s.title === 'string' ? s.title : '', '', '',
      // 无有效时刻的旧会话保持 0:排序语义与账本一致(无时间戳排末尾),
      // 而非伪造为当日 0 点(会把「无时间戳」误排到时间降序最前)。
      Number(s.at) > 0 ? Number(s.at) : 0,
      date, month,
      Math.floor(Math.max(0, Number(s.input) || 0)),
      Math.floor(Math.max(0, Number(s.output) || 0)),
      Math.floor(Math.max(0, Number(s.cacheRead) || 0)),
      Math.floor(Math.max(0, Number(s.cacheWrite) || 0)),
      Math.floor(Math.max(0, Number(s.reasoning) || 0)),
      cost * r, (Number(s.apiCost ?? s.cost) || 0) * r,
      meta,
    )
  }

  /** (day_key, session_id) 是否已有任意行(实时行或迁移行)。 */
  hasPair(dayKey, sessionId) {
    if (this.closed) return false
    const row = this.selectPair.get(String(dayKey), String(sessionId))
    return row !== undefined
  }

  /** 给 (day_key, session_id) 的空标题行补标题(只填空,不覆盖已有值)。 */
  fillSessionTitle(dayKey, sessionId, title) {
    if (this.closed || typeof title !== 'string' || title.length === 0) return 0
    const info = this.updateTitle.run(String(title), String(dayKey), String(sessionId))
    return Number(info?.changes ?? 0)
  }

  /**
   * 官方渠道判定(镜像 store.js officialCostOfDay):llm- 前缀剥离后
   * provider 为 deepseek / deepseek-official。
   * @param provider - 原始 provider id(可为空/非字符串)。
   */
  isOfficialProvider(provider) {
    let p = String(provider ?? '').trim().toLowerCase()
    if (p.startsWith('llm-')) p = p.slice(4)
    return p === 'deepseek' || p === 'deepseek-official'
  }

  /**
   * 某日官方渠道(DeepSeek 官方)成本,人民币口径(issue #78 统一 sqlite 取数)。
   * 实时行按 provider 判定;迁移行按 meta.byProviderModel 的 provider 键判定
   * (meta 成本已在迁移时折算为人民币);无模型明细的残余行(session_id='' 且
   * meta 无 byProviderModel)无法拆分渠道,不计入。对账用:与官方余额当日
   * 变动同基准比较前按汇率折回美元。
   * @param dayKey - 本地日键 YYYY-MM-DD。
   * @returns 该日官方渠道成本(人民币,非负)。
   */
  officialChannelCostRmb(dayKey) {
    if (this.closed) return 0
    let sum = 0
    for (const row of this.selectBetween.all(String(dayKey), String(dayKey))) {
      if (row.aggregate === 0) {
        if (this.isOfficialProvider(row.provider)) sum += Number(row.cost_rmb) || 0
        continue
      }
      let meta = {}
      try { meta = JSON.parse(row.meta) ?? {} } catch { /* 损坏 meta:按空处理 */ }
      const by = meta.byProviderModel
      if (by === null || typeof by !== 'object') continue
      for (const [key, b] of Object.entries(by)) {
        const sep = key.indexOf(':')
        const provider = sep >= 0 ? key.slice(0, sep) : key
        if (this.isOfficialProvider(provider)) sum += Number(b?.cost) || 0
      }
    }
    return sum
  }

  /**
   * 去重感知的账本日导入(issue #78 后续:安装前历史回填 / 账本重算重建):
   * 与 importLedgerDay 同构,但按 (day_key, session_id) 对调用明细**现有全部
   * 行**(实时行 aggregate=0 与迁移行 aggregate=1 都算)判重——已存在的会话
   * 只补空标题、金额绝不动,避免「实时行 + 迁移聚合行」双计;残余行
   * (session_id='')同样仅在无任何行时写入。
   * @param day - 账本单日记录。
   * @param rate - 展示汇率(美元 → 人民币)。
   * @returns { imported, skipped, titled } 新增会话日数 / 已存在跳过数 / 补标题行数。
   */
  importLedgerDayMissing(day, rate) {
    const stats = { imported: 0, skipped: 0, titled: 0 }
    if (this.closed || day === null || typeof day !== 'object') return stats
    const date = String(day.date ?? '')
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return stats
    const r = safeRate(rate)
    const month = date.slice(0, 7)
    const num = value => Math.floor(Math.max(0, Number(value) || 0))
    const sessions = Array.isArray(day.sessions) ? day.sessions : []
    let sessionCalls = 0
    let sessionCost = 0
    let sessionApiCost = 0
    const sums = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, reasoning: 0 }
    for (const s of sessions) {
      if (s === null || typeof s !== 'object' || typeof s.id !== 'string' || s.id.length === 0) continue
      const calls = num(s.calls)
      const cost = Number(s.cost) || 0
      sessionCalls += calls
      sessionCost += cost
      sessionApiCost += Number(s.apiCost ?? s.cost) || 0
      sums.input += num(s.input); sums.output += num(s.output)
      sums.cacheRead += num(s.cacheRead); sums.cacheWrite += num(s.cacheWrite)
      sums.reasoning += num(s.reasoning)
      if (this.hasPair(date, s.id)) {
        stats.skipped += 1
        // 已有会话(安装后实时记录的调用行,或此前迁移行):只补空标题。
        if (typeof s.title === 'string' && s.title.length > 0) {
          stats.titled += this.fillSessionTitle(date, s.id, s.title)
        }
        continue
      }
      this.insertAggregateRow(date, month, s, calls, cost, r, '')
      stats.imported += 1
    }
    const residual = num(day.calls) - sessionCalls
    if (residual > 0 && !this.hasPair(date, '')) {
      const meta = JSON.stringify({ calls: residual, byProviderModel: {} })
      this.insertAggregate.run(
        '', '', '', '', '',
        0, date, month,
        Math.max(0, num(day.input) - sums.input),
        Math.max(0, num(day.output) - sums.output),
        Math.max(0, num(day.cacheRead) - sums.cacheRead),
        Math.max(0, num(day.cacheWrite) - sums.cacheWrite),
        Math.max(0, num(day.reasoning) - sums.reasoning),
        Math.max(0, ((Number(day.cost) || 0) - sessionCost) * r),
        Math.max(0, ((Number(day.apiCost ?? day.cost) || 0) - sessionApiCost) * r),
        meta,
      )
      stats.imported += 1
    }
    return stats
  }

  /** 拉取 [fromKey, toKey] 日区间内的全部行(含两端,YYYY-MM-DD 字典序)。 */
  rowsBetween(fromKey, toKey) {
    if (this.closed) return []
    return this.selectBetween.all(String(fromKey), String(toKey))
  }

  /** 拉取全部行(跨日会话排行用;上限受 sqlite 全量保留约束)。 */
  allRows() {
    if (this.closed) return []
    return this.selectAll.all()
  }

  /** 清空全部调用明细(与 resetHistory 联动)。 */
  clear() {
    if (this.closed) return
    this.deleteAll.run()
  }

  /**
   * 本地来源键判定(镜像 store.js unpriceLocalOriginModels):显式
   * `__local__` 哨兵覆盖,或 provider/model 属于本地推理来源。
   */
  isLocalKey(key, overrides) {
    if (overrides !== null && typeof overrides === 'object' && overrides[key] === '__local__') return true
    const sep = key.indexOf(':')
    const rawProvider = (sep >= 0 ? key.slice(0, sep) : 'deepseek').toLowerCase()
    const provider = rawProvider.startsWith('llm-') ? rawProvider.slice(4) : rawProvider
    const model = sep >= 0 ? key.slice(sep + 1) : key
    return isLocalOriginProviderOrModel(provider, model)
  }

  /**
   * 本地来源行成本归零(镜像 unpriceLocalOriginModels,issue #76 后续):
   * priceOverrides 变更(含 __local__ 哨兵)后账本把本地来源历史桶归零,
   * sqlite 实时行同步归零(token 保留);迁移行不在此处理(rebuildAggregates
   * 以重算后的账本重建)。
   * @param config - 当前配置(读 priceOverrides 与 priceMatch)。
   */
  zeroLocalCosts(config) {
    if (this.closed) return
    const overrides = config?.priceOverrides !== null && typeof config?.priceOverrides === 'object' ? config.priceOverrides : {}
    const stmt = this.db.prepare('UPDATE calls SET cost_rmb = 0, api_cost_rmb = 0 WHERE id = ?')
    for (const row of this.selectAll.all()) {
      if (row.aggregate === 1) continue
      const key = `${row.provider || 'deepseek'}:${row.model || 'default'}`
      if (this.isLocalKey(key, overrides)) stmt.run(row.id)
    }
  }

  /**
   * 实时行按当前价目全量重算成本(镜像 recomputeLedgerPricingBasis 对实时
   * 调用的数学:同一解析链 + 按行 at_ms 的峰谷档位 + 生效币种折算人民币;
   * Plan/API 分类重算 api_cost_rmb)。价格币种切换后账本逐事件重定价,
   * sqlite 以同参数重算保持一致。迁移行不在此处理。
   * @param config - 当前配置(prices/priceMatch/priceOverrides/峰谷/汇率/planBilling)。
   */
  repriceAll(config) {
    if (this.closed) return
    const mode = config?.priceMatch === 'exact' ? 'exact' : 'auto'
    const overrides = config?.priceOverrides !== null && typeof config?.priceOverrides === 'object' ? config.priceOverrides : {}
    const stmt = this.db.prepare('UPDATE calls SET cost_rmb = ?, api_cost_rmb = ? WHERE id = ?')
    const enabledPlans = enabledPlanSetOf(config)
    for (const row of this.selectAll.all()) {
      if (row.aggregate === 1) continue
      const key = `${row.provider || 'deepseek'}:${row.model || 'default'}`
      // 本地来源键与账本同语义:直接归零,不做解析(显式云端覆盖也不计费)。
      if (this.isLocalKey(key, overrides)) {
        stmt.run(0, 0, row.id)
        continue
      }
      const resolved = providerPriceEntryFor(row.provider, row.model, config?.prices, { mode, overrides })
      const entry = resolved.entry ?? { cacheHit: 0, cacheMiss: 0, output: 0 }
      const peak = {
        enabled: resolved.billingMode === 'deepseek-peak' && config?.peakEnabled === true,
        effectiveAtMs: Date.parse(config?.peakEffectiveAt),
        windows: config?.peakWindows,
      }
      const priced = resolved.priced ? costOf({
        input: row.input_tokens, output: row.output_tokens,
        cacheRead: row.cache_read_tokens, cacheWrite: row.cache_write_tokens,
        reasoning: row.reasoning_tokens,
      }, entry, row.at_ms, peak) : 0
      const currency = resolveCurrency(resolved.entry, resolved.billingMode, config?.prices, row.provider)
      const costRmb = rmbFromCost(priced, currency, config?.exchangeRate)
      const cls = billingClassOf(row.provider, row.model, config?.planBilling, enabledPlans, config?.prices)
      stmt.run(costRmb, cls === 'api' ? costRmb : 0, row.id)
    }
  }

  /**
   * 迁移行整体重建(价格币种切换 / Plan 分类变更后账本已重算):删除全部
   * aggregate 行,按当前账本天数重新导入。导入用去重感知路径:与实时行
   * 重叠的会话(账本里的会话含安装后实时计费部分)不再补聚合行,避免
   * 「实时行 + 迁移聚合行」双计;仅迁移行覆盖的会话按重算后的账本重建。
   * 在事务内执行:并发 profile(web/acp)同时触发重算时,读侧要么看到旧
   * 迁移行、要么看到新迁移行,不会看到删了一半的中间态。
   * @param days - 账本 days 对象。
   * @param rate - 当前展示汇率。
   */
  rebuildAggregates(days, rate) {
    if (this.closed) return
    this.db.exec('BEGIN IMMEDIATE')
    try {
      this.db.exec('DELETE FROM calls WHERE aggregate = 1')
      for (const day of Object.values(days)) this.importLedgerDayMissing(day, rate)
      this.db.exec('COMMIT')
    } catch (error) {
      try { this.db.exec('ROLLBACK') } catch { /* 事务已回滚/未开启:忽略 */ }
      throw error
    }
  }

  close() {
    if (this.closed) return
    this.closed = true
    try { this.db.close() } catch { /* 已关闭/异常:忽略 */ }
  }
}

/** 合并一个 byProviderModel 条目到目标聚合(成本按人民币累计)。 */
function mergeBy(target, key, b) {
  const cur = target[key] ?? { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, reasoning: 0, calls: 0, cost: 0, apiCost: 0 }
  target[key] = {
    input: cur.input + (Number(b.input) || 0),
    output: cur.output + (Number(b.output) || 0),
    cacheRead: cur.cacheRead + (Number(b.cacheRead) || 0),
    cacheWrite: cur.cacheWrite + (Number(b.cacheWrite) || 0),
    reasoning: cur.reasoning + (Number(b.reasoning) || 0),
    calls: cur.calls + (Number(b.calls) || 0),
    cost: cur.cost + (Number(b.cost) || 0),
    apiCost: cur.apiCost + (Number(b.apiCost) || 0),
  }
}

/**
 * 行集合 → 单日 wire 记录(成本由人民币按 rate 折算回美元,与旧展示管道
 * 兼容;rate 非法按 1)。byProviderModel/sessions 由实时行(aggregate=0)按
 * 键分组、迁移行(aggregate=1)按 meta JSON 合并构成,两类可共存于同一会话。
 * @param rows - CallsDb 行。
 * @param rate - 当前展示汇率(美元 → 人民币)。
 * @param date - 日键。
 * @param withSessions - 是否组装 sessions 明细(history 列表不需要)。
 * @returns daySchema 形状记录。
 */
export function dayFromRows(rows, rate, date, withSessions = true) {
  const r = safeRate(rate)
  const toUsd = v => (Number(v) || 0) / r
  const out = {
    date,
    input: 0, output: 0, cacheRead: 0, cacheWrite: 0, reasoning: 0,
    calls: 0, cost: 0, apiCost: 0,
    byProviderModel: {},
    sessions: withSessions ? {} : [],
  }
  const mergeRow = (target, row) => {
    target.input += row.input_tokens
    target.output += row.output_tokens
    target.cacheRead += row.cache_read_tokens
    target.cacheWrite += row.cache_write_tokens
    target.reasoning += row.reasoning_tokens
    target.calls += row.aggregate === 1 ? (Number(JSON.parse(row.meta).calls) || 0) : 1
    target.cost += toUsd(row.cost_rmb)
    target.apiCost += toUsd(row.api_cost_rmb)
  }
  const mergeByOf = (target, row) => {
    if (row.aggregate === 1) {
      let meta = {}
      try { meta = JSON.parse(row.meta) ?? {} } catch { /* 损坏 meta:按空处理 */ }
      for (const [key, b] of Object.entries(meta.byProviderModel ?? {})) mergeBy(target, key, b)
    } else if (row.provider.length > 0 || row.model.length > 0) {
      const key = `${row.provider || 'deepseek'}:${row.model || 'default'}`
      mergeBy(target, key, {
        input: row.input_tokens, output: row.output_tokens,
        cacheRead: row.cache_read_tokens, cacheWrite: row.cache_write_tokens,
        reasoning: row.reasoning_tokens, calls: 1,
        cost: toUsd(row.cost_rmb), apiCost: toUsd(row.api_cost_rmb),
      })
    }
  }
  for (const row of rows) {
    mergeRow(out, row)
    mergeByOf(out.byProviderModel, row)
    if (!withSessions) continue
    if (typeof row.session_id !== 'string' || row.session_id.length === 0) continue
    let session = out.sessions[row.session_id]
    if (session === undefined) {
      session = out.sessions[row.session_id] = {
        id: row.session_id,
        title: '',
        at: 0,
        input: 0, output: 0, cacheRead: 0, cacheWrite: 0, reasoning: 0,
        calls: 0, cost: 0, apiCost: 0,
        byProviderModel: {},
      }
    }
    if (typeof row.session_title === 'string' && row.session_title.length > 0 && session.title.length === 0) {
      session.title = row.session_title
    }
    if (session.at === 0 || row.at_ms < session.at) session.at = row.at_ms
    mergeRow(session, row)
    mergeByOf(session.byProviderModel, row)
  }
  if (withSessions) {
    out.sessions = Object.values(out.sessions).sort((a, b) => b.cost - a.cost)
    // 与旧 wire 契约一致:标题/时刻缺失时不携带空值键。
    for (const s of out.sessions) {
      if (s.title.length === 0) delete s.title
      if (s.at === 0) delete s.at
    }
  }
  return out
}

/** 单日记录:由该日行集合组装(含会话明细)。 */
export function dayRecord(rows, rate, date) {
  return dayFromRows(rows, rate, date, true)
}

/**
 * 跨日会话排行行:按 (day_key, session_id) 分组(与账本按日拆会话同语义)。
 * firstId 为该组最早行的自增 id,供 recent(实时顺序)排序复刻账本的
 * 构造序(日倒序 + 每日首记倒序);不参与 wire(调用方使用后可删除)。
 * @param rows - 全部行。
 * @param rate - 当前展示汇率。
 * @returns [{ date, id, title, at, input, ..., calls, cost, apiCost, byProviderModel, firstId }]。
 */
export function sessionRows(rows, rate) {
  const r = safeRate(rate)
  const groups = new Map()
  for (const row of rows) {
    if (typeof row.session_id !== 'string' || row.session_id.length === 0) continue
    const key = `${row.day_key}\u0000${row.session_id}`
    let g = groups.get(key)
    if (g === undefined) {
      g = {
        date: row.day_key,
        id: row.session_id,
        title: '',
        at: 0,
        firstId: row.id,
        input: 0, output: 0, cacheRead: 0, cacheWrite: 0, reasoning: 0,
        calls: 0, cost: 0, apiCost: 0,
        byProviderModel: {},
      }
      groups.set(key, g)
    } else if (row.id < g.firstId) {
      g.firstId = row.id
    }
    if (typeof row.session_title === 'string' && row.session_title.length > 0 && g.title.length === 0) {
      g.title = row.session_title
    }
    if (g.at === 0 || row.at_ms < g.at) g.at = row.at_ms
    g.input += row.input_tokens
    g.output += row.output_tokens
    g.cacheRead += row.cache_read_tokens
    g.cacheWrite += row.cache_write_tokens
    g.reasoning += row.reasoning_tokens
    g.calls += row.aggregate === 1 ? (Number(JSON.parse(row.meta).calls) || 0) : 1
    g.cost += (Number(row.cost_rmb) || 0) / r
    g.apiCost += (Number(row.api_cost_rmb) || 0) / r
    if (row.aggregate === 1) {
      let meta = {}
      try { meta = JSON.parse(row.meta) ?? {} } catch { /* ignore */ }
      for (const [key, b] of Object.entries(meta.byProviderModel ?? {})) {
        mergeBy(g.byProviderModel, key, { ...b, cost: (Number(b.cost) || 0) / r, apiCost: (Number(b.apiCost) || 0) / r })
      }
    } else if (row.provider.length > 0 || row.model.length > 0) {
      const key = `${row.provider || 'deepseek'}:${row.model || 'default'}`
      mergeBy(g.byProviderModel, key, {
        input: row.input_tokens, output: row.output_tokens,
        cacheRead: row.cache_read_tokens, cacheWrite: row.cache_write_tokens,
        reasoning: row.reasoning_tokens, calls: 1,
        cost: (Number(row.cost_rmb) || 0) / r, apiCost: (Number(row.api_cost_rmb) || 0) / r,
      })
    }
  }
  const out = [...groups.values()]
  // 与旧 wire 契约一致:标题/时刻缺失时不携带空值键。
  for (const g of out) {
    if (g.title.length === 0) delete g.title
    if (g.at === 0) delete g.at
  }
  return out
}

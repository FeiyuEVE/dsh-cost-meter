#!/usr/bin/env node
/**
 * 一次性回填脚本(issue #78 后续):把插件安装前(以及任何尚未入库的)会话
 * 消耗统计从宿主会话日志写入 sqlite 调用明细库。
 *
 * 用法:node scripts/backfill-calls-sqlite.mjs [--sessions <root>]
 *  - 默认会话根目录:$DSH_HOME/sessions;
 *  - 调用明细库:$DSH_HOME/storages/cost-meter/calls.sqlite(与插件同库,
 *    页面统计直接可见);
 *  - 去重:按 (day_key, session_id) 对调用明细现有全部行(实时行与迁移行)
 *    判重——已存在的会话只补空标题(日志标题是权威来源),金额绝不动;
 *    可重复执行,增量补齐。
 *
 * 与 dsh web/acp 进程并发运行安全(WAL + busy_timeout);账本只读(取计价
 * 配置),不落盘。
 */

import { join } from 'node:path'
import { resolveDshHome } from '@deepseek-ai/dsh-home-paths'
import { Ledger } from '../lib/store.js'
import { CallsDb } from '../lib/calls-db.js'
import { importCallsFromLogs } from '../lib/backfill.js'

const argIndex = process.argv.indexOf('--sessions')
const sessionsRoot = argIndex >= 0 && process.argv[argIndex + 1] !== undefined
  ? process.argv[argIndex + 1]
  : join(resolveDshHome(), 'sessions')

const ledger = Ledger.open()
const callsDb = CallsDb.open()
try {
  const startedAt = Date.now()
  const stats = await importCallsFromLogs(callsDb, ledger, sessionsRoot)
  const seconds = ((Date.now() - startedAt) / 1000).toFixed(1)
  console.log(`[dsh-cost-meter] 调用明细回填完成:扫描 ${stats.scanned} 份会话日志,` +
    `新增 ${stats.imported} 个会话日,跳过 ${stats.skipped} 个已存在,` +
    `补标题 ${stats.titled} 行(耗时 ${seconds}s,库:${callsDb.path})`)
  if (stats.imported === 0 && stats.titled === 0) {
    console.log('[dsh-cost-meter] 无需回填:会话消耗统计均已入库(或日志无可用会话)。')
  }
} finally {
  callsDb.close()
  // 只读打开,无写入 pending,close 不会落盘,不影响运行中的 dsh 进程。
  ledger.close()
}

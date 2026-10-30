# cronv5 Review & TODO

> 审查范围：`cron.go` / `job.go` / `job_run.go` / `entry.go` / `entry_heap.go` / `parser.go` / `spec.go` / `webadmin/` / `internal/`
> 参考来源：[robfig/cron](https://github.com/robfig/cron) 的 issues / PRs（按 👍 和讨论数排序）
> 标注说明：✅ = 已写复现测试确认；🔍 = 代码阅读确认；`rc#N` = robfig/cron 对应 issue/PR 编号

---

## P0 — 已确认的 Bug（✔ 已全部修复，回归测试见 `regression_test.go`、`webadmin/main_test.go`）

- [x] **B1 `Once()` / `N()` 在执行前就把自己的 ctx 取消了** ✅
  `job.go:533` `Once` 先调用 `c.Remove(id)`，而 `removeEntry`（`cron.go:961`）会 cancel 该 entry 所有正在运行的 run —— 包括当前这次。
  复现：`Once(func(ctx){ ctx.Err() })` 得到 `context canceled`。任何使用 ctx 的 job（HTTP、DB）会立即失败。`NWrapper` 最后一次执行同理。
  修复方向：内部提供“移除 entry 但不取消当前 run”的路径（如 `removeEntry(id, cancelRunning bool)`），或在 job 结束后再移除。（rc#472、rc#317）

- [x] **B2 `Wait()` 在未启动/已停止的 cron 上永久阻塞** ✅
  `stop()`（`cron.go:593`）未运行时返回 `nil` channel，`wait()` 中 `<-nil` 永远阻塞。
  修复：未运行时返回已关闭的 channel；`Wait()` 仍应等待残留的运行中 job。

- [x] **B3 `Stop()` 之后无法再次 `Start()`** ✅
  `c.ctx` 在 `Build()` 中只创建一次，`Stop()` 调用 `c.cancel()` 后 `run()` 会立即退出，cleanup 线程也随之永久停止。robfig 是支持 Stop→Start 的。
  修复：每次 `Start` 时基于 parent ctx 重新派生 run ctx（并重启 cleanup 线程）。

- [x] **B4 同步 Hook（`HookSync`）内调用 Hook API 会死锁** ✅
  `triggerHooks`（`cron.go:1152`）持有 `hooks` 读锁时调用 hook；hook 内调用 `RemoveHook/OnEvt/DisableHook` 要写锁 → 死锁（且阻塞 job goroutine）。
  修复：读锁内复制出待执行 hook 列表，释放锁后再调用。

- [x] **B5 `hooksFor` 的 `append` 别名写入，导致 hook 串到别的 entry / 数据竞争** ✅
  `cron.go:64` `append(c.globalHooksMap[evt], entryHooks...)` 当全局切片有剩余容量时（`slices.Delete` 后很常见）会写入共享底层数组；并发 job 在 **读锁** 下同时写 → data race，entry A 可能执行 entry B 的 hook。
  修复：`slices.Concat` 或新建切片。

- [x] **B6 Job 运行日志存在数据竞争** ✅（`go test -race` 报告）
  job 通过 `JobRun.Logger()` 写 `logsBuf`（`bytes.Buffer`，无锁），同时 `export()`（`job_run.go:96`，webadmin / `RunningJobsFor` / hooks）读取 `logsBuf.String()`。
  修复：给日志 buffer 加锁的 writer（同时支持容量上限，见 P1）。

- [x] **B7 `sync.Pool` 复用 `jobRunStruct` 不安全** 🔍
  `job_run.go:56`：
  - `export()` 返回的 `Events` 与内部切片共享底层数组，run 被回收复用后 `events[:0]` + append 会改写旧快照（hooks 异步、用户保存的 `JobRun`）。
  - 旧 `JobRun.Logger()` 仍指向被复用对象的 buffer，会写到别的 run 的日志里。
  修复：移除 pool（job 运行频率下收益可忽略），`export()` 时 `slices.Clone(events)`。

- [x] **B8 `WithRetry` 吞掉错误、忽略取消** 🔍
  `job.go:697`：
  - 始终 `return nil`，最终失败不会触发 `JobErr` 事件/hook。
  - ctx 取消后仍 `continue` 继续重试。
  - 最后一次失败后还会多 sleep 一次。
  修复：返回最后一次错误；ctx 取消立即返回 `ctx.Err()`；最后一次不等待。（另见 F7 指数退避，rc#560）

- [x] **B9 `ThresholdClbAsync` 结果随机（漏报 + 误报）** 🔍
  `job.go:563`：`clock.After(threshold)` 与 `ctx2`（同一时长的 timeout）同时到期，`select` 随机选择 → 慢任务有约 50% 概率不回调；当 `keepCompletedRunsDur > 0` 时 run ctx 在 job 结束后不会被 cancel，**已经按时完成的 job** 也会约 50% 概率误报。
  修复：job 完成时关闭 done channel，`select { case <-done: case <-timer: clb() }`。

- [x] **B10 Job 结束后其 ctx 不被 cancel（资源泄漏）** 🔍
  run ctx 只在 `releaseJobRun` 时 cancel；开启 `keepCompletedRunsDur` 时会一直挂到被清理，所有派生自它的 goroutine/timer 都不会释放。
  修复：`runWithRecovery` 结束即 `cancel()`。

- [x] **B11 `Start()` 前添加的任务，在 Start 时会立即“补跑”一次** ✅
  `schedule()`（`cron.go:775`）在 Add 时计算 `Next`，`run()` 启动时不重新计算。Add 后过一段时间才 Start，所有过期 entry 会立即执行。robfig 在 Start 时会以当前时间重算 `Next`。
  修复：`run()` 开始时对所有 active entry 重算 `Next`（`RunOnStart`/`WithNext` 显式指定的保持不变）。

- [x] **B12 `Entry.Job()` 反射“解包”逻辑错误** ✅
  `entry.go:30`：任何“单字段 struct”都会被当作内部 wrapper 解包。用户 `type MyJob struct{ Name string }` 调用 `Entry.Job()` 返回的是 `"x"`（字符串）而不是 `MyJob`。
  修复：只对内部 `JobNWrapper` 类型解包（类型断言/私有接口 `unwrap() any`）。

- [x] **B13 删除正在运行的 entry 会泄漏 `runningJobsMap` 项** ✅
  `removeEntry` 先 `Delete(id)`，运行中的 job 结束时 `updateJobRuns` 又 `LoadOrStore` 出一个空项，且该 run 永不释放。
  修复：完成路径用 `Load` 而非 `LoadOrStore`；或延迟到 running 为空时再删除。另外 `removeEntry` 未清理 `entryHooksMap[id]`，hook 也会泄漏。

- [x] **B14 `addEntry` 的重复 ID 检查存在 TOCTOU 竞态** 🔍
  `cron.go:796` 先 `entryExists`（读锁）再写锁插入；并发 `AddJob(WithID(x))` 可能都成功，后者覆盖 map 但 heap `Push` 被忽略 → map 与 heap 不一致。
  修复：在同一把写锁内检查 + 插入；并校验 `job`/`Schedule` 非 nil。

- [x] **B15 webadmin 存储型 XSS** 🔍
  `webadmin/templates/run.gohtml:13`：`{{ .JobRun.Logs | Nl2br | Safe }}` 把 job 日志原样作为 HTML 输出。日志中含外部数据（URL、错误信息）即可注入脚本。
  修复：先 `html.EscapeString` 再换行，或用 CSS `white-space: pre-wrap` 去掉 `Safe`。另：POST 接口无 CSRF 防护。

- [x] **B16 `SetCleanupInterval` 可能启动多个清理线程** 🔍
  `cron.go:566`：设为 0 后旧线程要等下一轮才退出，此时再设为非 0 会起第二个线程。用单一常驻线程 + 通知 channel 替代。

## P1 — 行为/文档不一致、健壮性（✔ 已完成）

- [x] **R1 `doc.go` 过时**：示例 `c.AddFunc("...", func(){})` 无法编译（`AddFunc` 需要 `FuncJob`）；表格写 6 个字段必填，但默认 parser 是 5 字段；“Stop 不会停止正在运行的 job”与实际（Stop 会 cancel 所有 job ctx）不符。
- [x] **R2 Stop/Remove 是否取消运行中 job 改为可选项**：`Stop()` 现在一律 cancel；建议 `StopWithContext(ctx)`（优雅关闭+超时）与 `WithCancelJobsOnStop(bool)`。（rc#98）
- [x] **R3 不可满足的表达式静默接受**：`0 0 30 2 *` 返回零时间、永不执行。`AddJob` 时检测 `Next().IsZero()` 返回错误（或提供 `Validate`）。（rc#110）
- [x] **R4 `TZ= * * * * *`（空时区）被静默当成 UTC**，应返回错误。
- [x] **R5 日志 buffer 无上限**：频繁打日志的 job 会无限增长内存；加 `WithMaxLogSize`。
- [x] **R6 completed runs 仅按时间清理**：再加按条数上限（`WithMaxCompletedRunsPerEntry`）。
- [x] **R7 wrapper 在每次运行时才 `J(job)` 转换**：不支持的类型在运行时才 panic（被 recover 成 JobPanic），应在构造 wrapper 时转换并尽早报错。
- [x] **R8 `go vet` 警告**：`internal/utils/utils.go:248 NewAtomicPtr` 按值返回 `atomic.Pointer`（copylocks）。
- [x] **R9 依赖 `replace` 到 clockwork fork**：`replace` 对下游不生效，库代码实际编译的是上游 `v0.5.0`，而测试依赖 fork 独有的 `SleepNotify/AfterNotify`。建议测试改为上游 API（`BlockUntilContext`）并移除 replace，避免测试与用户实际使用的版本不一致。
- [x] **R10 CI**：`.travis.yml` 已过时，换成 GitHub Actions（`go vet`、`go test -race`、多 Go 版本）。
- [x] **R11 `NWrapper(n<=0)`** 永远不会移除，需参数校验。

## P2 — 性能优化（✔ 已完成，benchmark 见 `bench_test.go`）

- [x] **P1 调度循环每轮 `clock.After` 新建 timer**：改为复用单个 `Timer` + `Reset`，减少分配（fake clock 下还会累积 waiter）。
- [x] **P2 每次 Add/Remove/Update 都与调度 goroutine 同步握手**（`entriesUpdated` 阻塞到 `runDueEntries` 完成），批量添加 N 个任务需 N 次往返。改为非阻塞通知（带缓冲 chan, len 1），或提供批量 API。
- [x] **P3 `Entries()` 每次 Clone + 全量 Pop（O(n log n) + 两次分配）**：直接复制切片后 `slices.SortFunc` 一次即可。
- [x] **P4 `runDueEntries` 在 entries 写锁内调用用户代码**（`IDFactory`、`JobRunLoggerFactory`）：先收集到期 entry，释放锁后再创建 run。
- [x] **P5 cleanup 每次启动 `NumCPU` 个 goroutine**：数据量小，单线程循环即可。
- [x] **P6 `Parse` 每次调用 `time.LoadLocation`**（读 tzdata 文件）：加缓存。
- [x] **P7 `CompletedJobs()` 每次导出全部历史（含日志字符串复制）**：提供分页/limit 版本，webadmin 使用分页。
- [x] **P8 webadmin 每个请求重新解析模板**：启动时解析一次。
- [x] **P9 增加 benchmark**（Add 1w 任务、调度循环、Parse、Next），优化前后对比。

优化前后对比（4 核，`go test -bench . -benchmem`，取 3 次中位数）：

| Benchmark | 优化前 | 优化后 | 提升 |
|---|---|---|---|
| UpdateSchedule（运行中，1 万 entry） | 140 µs | 0.22 µs | 630× |
| RunNow（运行中） | 121 µs | 8.9 µs | 13.6× |
| CleanupOldRuns（1000 entry） | 864 µs | 64 µs | 13.4× |
| Entries（1 万 entry） | 12.7 ms | 1.3 ms | 9.7× |
| AddJob（运行中） | 106 µs | 18.8 µs | 5.7× |
| Parse（带 CRON_TZ） | 5.5 µs | 1.2 µs | 4.5× |
| CompletedJobs（1000 runs） | 2.5 ms / 2512 allocs | 2.1 ms / 1512 allocs | 1.2× |
| Dispatch（一次触发 1000 个 job） | 8.6 ms / 34 allocs/job | 8.4 ms / 30 allocs/job | ≈1× |

## P3 — 新功能（来自 robfig/cron 社区需求）

### 表达式 / 解析器
- [ ] **F1 `L`（月最后一天）、`L-n`、`LW`、`W`（最近工作日）** — rc#397、rc#279、rc#105
- [ ] **F2 `#`（第 n 个星期几，如 `MON#1`）与 `5L`（最后一个周五）** — rc#486、rc#518
- [ ] **F3 Jenkins 风格 `H` 哈希值**（按 entry ID 打散，避免同一时刻雪崩）— rc#309
- [ ] **F4 星期 `7` = 周日、环绕区间 `FRI-MON`** — 常见需求，Vixie cron 兼容
- [ ] **F5 可选 Year 字段（Quartz 兼容）** — rc#340
- [ ] **F6 `Schedule.Prev()` / 获取前 N 次、后 N 次执行时间** — rc#322、rc#361、rc#224
- [ ] **F7 时区支持 UTC 偏移（`TZ=UTC+8`）** — rc#324、rc#379
- [ ] **F8 DST 处理策略**：当前春季跳变时 `30 2 * * *` 整天被跳过（已验证 2024-03-10 纽约直接跳到 03-11），秋季回拨执行两次。提供可选策略（跳变时在下一个有效时刻执行 / 回拨只执行一次）— rc#541
- [ ] **F9 亚秒级 `@every`**（可选开启）— rc#178
- [ ] **F10 表达式转人类可读描述 `Describe(spec)`** — rc#229

### 调度 / 任务
- [ ] **F11 指定时间点的一次性任务 `At(time.Time)` / `AddOnce`** — rc#441、rc#302
- [ ] **F12 `@every` 立即执行 / 首次延迟**（已有 `RunOnStart`、`WithNext`，补充文档及 `WithFirstDelay`）— rc#406、rc#258、rc#297
- [ ] **F13 错过执行的补偿策略（Misfire policy）**：系统休眠/阻塞后选择“跳过 / 补跑一次 / 全部补跑”。
- [ ] **F14 `WithRetry` 支持指数退避 + 抖动 + 自定义重试判断** — rc#560
- [ ] **F15 全局/按标签并发限制（`WithMaxConcurrency`）**、按 entry 的 `SkipIfStillRunning` 作为 EntryOption — rc#430、rc#285
- [ ] **F16 `RunNow` 走完整 wrapper 链并返回 RunID**；`Entry.Job` 手动调用绕过 wrapper 的问题 — rc#551
- [ ] **F17 Entry 支持 tag/metadata，按 tag 查询/删除/暂停** — rc#307 扩展
- [ ] **F18 `Pause()/Resume()` 整个调度器（不取消运行中任务）**
- [ ] **F19 可插拔持久化 / 分布式锁接口（Store / Locker）**，用于多实例部署只执行一次和保存执行历史 — rc#214、rc#428
- [ ] **F20 Metrics 钩子（Prometheus 友好的计数/耗时）**，JobRun 增加 `ScheduledAt`（本次计划时间）— rc#357

---

## 建议执行顺序

1. P0 全部（每项附回归测试，`go test -race` 通过）
2. R1–R11
3. P1–P9（先加 benchmark 再优化）
4. F 系列按需挑选（推荐优先：F1/F2/F4、F11、F13、F14、F6）

# 任务调度与资源配额管理（本机可复现）

一个仅依赖 Spring Boot、无任何外部服务 / 消息中间件 / 真实凭据的任务调度原型：
用**本地执行者**（本机线程池）模拟任务的下发与执行，围绕三件事给出一致且可解释的结论：

1. **配额准入**：按提交主体统计占用中的资源，超额 / 重复提交必须被拒绝，且拒绝不改变既有占用；
2. **并发调度**：多个待调度任务按"优先级 + 主体公平性 + FIFO"挑选，同一任务不会被并发下发两次，配额不足只能等待；
3. **租约与状态流转**：租约超时 / 执行者失联可回收重排，运行中取消、超时回收、结果回传并发发生时结论唯一，终态配额精确归还且不重复归还；
4. **优先级抢占**：执行能力或主体配额被占满时，高优先级等待任务可按确定性规则抢占低优先级运行任务，
   立即占用让出的能力；被挤任务回到等待队列、不消耗失败重试预算，之后正常重新调度到终态。

---

## 1. 模块结构

| 包 / 类 | 职责 |
| --- | --- |
| `model` | `Job`(聚合根)、`JobState`、`Lease`、`JobResult`、`ResourceQuota`、`SubmitRequest`、`JobSnapshot`、`RejectReason` |
| `store.JobStore` | 任务内存存储（`ConcurrentHashMap`），`putIfAbsent` 保证重复提交只成功一次 |
| `quota.QuotaService` | 配额账本：提交准入、下发时原子"检查+发放"、终态一次性归还 |
| `scheduler.Scheduler` | PENDING 候选排序、原子占用执行者槽位、任务锁内迁移到 RUNNING 并发租约；槽位/配额占满时执行抢占 |
| `scheduler.FairnessTracker` | 主体历史下发次数记账（公平性依据） |
| `scheduler.SchedulerDaemon` | 可选后台驱动（默认关闭，测试/演示手动 tick，结论可复现） |
| `lease.LeaseManager` | 租约发放 / 校验（id、执行者归属、TTL）/ 超时与失联回收 |
| `executor.LocalTaskExecutor` | 本机线程池执行者，按 payload 模拟成功 / 失败 / 延迟 / 失联 |
| `service.JobService` | **唯一状态迁移入口**：提交、取消、结果回传、查询 |
| `web.*` | REST 入口 + 统一拒绝异常映射 |

---

## 2. 状态机与迁移约束

```
                 提交(submit)
                     │
                     ▼
                ┌─────────┐  调度选中 + 配额发放成功   ┌─────────┐
                │ PENDING │ ───────────────────────▶ │ RUNNING │
                └─────────┘                            └─────────┘
                   ▲  ▲  ▲                           ／  │  │  │  ＼
     失败可重试 │  │  │ 租约超时/失联回收(仍有额度) 成功／  失败  取消  超时/失联(额度耗尽)
               │  │  │ 被更高优先级任务抢占(退还本次尝试)／   （重试）  │       │
                   │  └──────────────────────────  ／           ▼       ▼
                   │                                     ▼       ▼       ▼
                   │                                ┌─────────┐ ┌───────────┐ ┌────────┐
                   └────────────────────────────────│  重试    │ │ CANCELLED │ │ FAILED │
                                                    └─────────┘ └───────────┘ └────────┘
                       成功回传 → SUCCEEDED；失败且 attempt==maxAttempts → FAILED(RETRY_LIMIT_EXHAUSTED)
```

- 终态：`SUCCEEDED`、`FAILED`、`CANCELLED`。**终态不接受任何迁移**，再取消 / 再回传一律 `ILLEGAL_STATE_TRANSITION`。
- 所有迁移都在 `Job` 对象内置锁内完成"校验 → 迁移 → 配额联动"，因此：
  - 取消、回传、回收并发时**只有一个生效**，败者得到可区分的拒绝原因；
  - 拒绝发生在任何写入之前，**绝不改变既有占用**。

---

## 3. 配额规则

- 资源是三元组 `(cpu, memoryMb, instances)`，按**主体（subject）**设硬上限（任一维度超限即拒绝）。
- **提交准入**：`当前占用 + 本次需求 > 上限` → `QUOTA_EXCEEDED_ON_SUBMIT`（PENDING 不占配额，硬隔离在下发时保证）。
- **下发发放**：`QuotaService.tryAcquire` 在"主体配额锁"内原子完成检查与记账，并发调度不会突破上限；不足时任务保持 `PENDING` **继续等待，绝不越权下发**。
- **归还**：任务离开 RUNNING（成功 / 失败终态 / 取消 / 租约回收）时在任务锁内归还一次；任务上的 `quotaHeld` 布尔标记保证**同一任务不会重复归还**（重复归还会触发"扣减为负"的不变量异常）。
- 锁序固定为：**任务锁 → 主体配额锁**，全工程不存在反向获取，避免死锁。

## 4. 租约协议

- 每次下发生成一条 `Lease(leaseId, jobId, executorId, issuedAt, expiresAt, attempt)`，TTL 由 `scheduler.lease-ttl-ms` 配置。
- 执行者回传结果时依次校验：任务 `RUNNING` → 租约 id 匹配 → 执行者归属匹配 → 租约未过期；任一不过即拒绝，原因可区分：
  `ILLEGAL_STATE_TRANSITION` / `LEASE_EXPIRED` / `LEASE_OWNER_MISMATCH`。
- 回收路径（`LeaseManager.reclaimExpired` 或模拟失联的 `reclaim(job)`），全部在任务锁内：
  1. 先归还配额并清 `quotaHeld`；
  2. `attempt < maxAttempts` → 回到 `PENDING` 重新排队（重新下发时重新发放配额、换新租约）；
  3. `attempt == maxAttempts` → 终结 `FAILED(RETRY_LIMIT_EXHAUSTED)`。
- 迟到回传（旧租约）不影响已回收 / 已终结的结论。

## 5. 调度选择（可解释）

`Scheduler.selectCandidates` 对 PENDING 任务依次按以下键排序，日志会打印完整顺序与依据：

1. **优先级** `priority` 降序；
2. **主体公平性**：该主体历史下发次数升序（少被服务者优先，避免单主体霸占执行者）；
3. **FIFO**：`createdAt` 升序；
4. **jobId 升序**兜底，保证排序完全确定、可复现。

执行者槽位用 CAS 原子预留：先占槽 → 进任务锁二次确认 PENDING → 原子申请配额 → 提交迁移；任一前置条件不满足则回滚槽位，任务继续等待。

## 6. 优先级抢占

**触发条件**（在现有 tick 驱动下自然生效，无需额外人工步骤）：一轮调度中，某个 PENDING 任务
因"所有执行者槽位已满"或"主体配额不足"无法下发，且存在**优先级严格更低**的 RUNNING 任务时，
进入抢占路径。等待任务优先级不高于任何运行任务时，抢占完全不触发，行为与无抢占能力时逐字节一致。

**受害者选择（确定性，可解释）**：在"RUNNING 且优先级严格更低、未达抢占免疫上限"的任务中排序取第一：

1. 优先级最低者优先；
2. 并列时租约发放时间最晚者（刚开始跑、被挤浪费的算力最少）优先；
3. 再并列按 jobId 升序兜底。

排序键在任务锁内快照，单次选择完全确定、可复现；日志打印全部候选与判定依据（`[preempt-select]`）。

**交接过程**（原子账目）：

1. 槽位：优先从受害者所在执行者 **1:1 接管**其槽位（`TaskExecutor.transferSlot`：槽位计数不变、
   归属转移给调度器，原工作线程被尽力中断且不得再归还该槽位；接管失败则退化为常规 reserve）；
2. 额度：受害者在任务锁内归还配额（`quotaHeld` 保证只还一次），抢占者随后重新入账——
   先还后入账，总量不多一份、不少一份，全程不超主体上限；
3. 抢占者复用与正常下发完全相同的提交点（PENDING 二次确认 → 原子配额准入 → 状态/租约同时生效）。

**被抢任务怎么算**：

- 状态回到 PENDING，之后被正常重新调度、跑到终态；
- **被抢占 ≠ 执行失败**：本次尝试次数退还（attempt 不增），不消耗失败重试预算
  （maxAttempts=1 的任务被抢后仍能跑完，见 `PreemptionTest`）；
- `preemptedCount` 记 1 次，作为防饿死依据。

**防饿死规则**：同一任务被抢占次数达到 `scheduler.max-preemptions-per-job`（默认 3）后获得
**抢占免疫**，不再被选为受害者；高优先级任务只能等它自然跑完。设为 0 可整体关闭抢占。

**与取消 / 超时 / 回传并发时的结论**：

- 抢占的"挤下"与取消、租约回收、结果回传一样，都在同一把任务锁内完成，**先拿锁者生效，结论唯一**；
- 取消先到 → 任务 CANCELLED，受害者选择无视终态任务，抢占不再发生；
- 回传先到 → 任务 SUCCEEDED，同样不再成为受害者；
- 抢占先到 → 任务回 PENDING：之后的取消按 PENDING 正常生效；旧租约的迟到回传因
  "状态非 RUNNING / 租约 id 不匹配"被拒，**改不回已被挤下的结论**；租约超时回收对 PENDING 任务是 no-op，
  不会二次归还配额；
- 锁序保持"抢占者锁(PENDING) → 受害者锁(RUNNING) → 主体配额锁"：PENDING 任务被持锁期间不可能
  成为别人的受害者，多调度线程并发抢占无循环等待。

**兼容范围**：无抢占需求时（等待任务优先级都不高于运行任务，或抢占未触发），调度顺序、公平性、
额度统计与租约结论与引入抢占前完全一致；既有用例全部保留并通过。

## 7. 重试规则

- `scheduler.max-attempts` 为最大尝试次数（含首次，默认 3 = 首次 + 2 次重试）。
- 失败与租约超时回收**共用同一 attempt 预算**；达到上限即终结 `FAILED(RETRY_LIMIT_EXHAUSTED)`，不再排队、不再被 tick 选中。
- 每次重排队都先归还配额，再次下发时重新发放，因此重试期间配额账实始终一致。

## 8. 拒绝原因一览（`RejectReason`）

| 原因 | 触发场景 | HTTP 状态 |
| --- | --- | --- |
| `QUOTA_EXCEEDED_ON_SUBMIT` | 提交即超出主体配额 | 422 |
| `DUPLICATE_SUBMISSION` | jobId 已存在（含并发） | 409 |
| `ILLEGAL_STATE_TRANSITION` | 终态再迁移 / 非 RUNNING 回传 | 422 |
| `RETRY_LIMIT_EXHAUSTED` | 失败或超时回收达到尝试上限（终态标记） | — |
| `LEASE_EXPIRED` | 租约 id 不匹配 / 已过期 | 422 |
| `LEASE_OWNER_MISMATCH` | 回传执行者非租约持有者 | 422 |
| `INVALID_ARGUMENT` | 请求参数非法 | 400 |
| `JOB_NOT_FOUND` | 任务不存在 | 404 |

---

## 9. 本地验证

### 9.1 构建与测试（无需任何外部服务）

```bash
mvn clean test          # 运行全部 24 个测试（含规定场景 + 抢占 + 调度顺序 + REST 集成）
mvn clean package       # 构建可执行 jar
```

测试覆盖（日志中会打印**任务标识、配额占用、判定依据**）：

| 测试类 | 覆盖场景 |
| --- | --- |
| `QuotaExhaustionTest` | 配额耗尽：提交超额拒绝且不改变占用；下发配额不足时等待，释放后继续 |
| `DuplicateSubmissionTest` | 重复提交：串行 + 16 线程并发，仅成功一次，原因 `DUPLICATE_SUBMISSION` |
| `ConcurrentDispatchTest` | 8 调度线程并发 tick 20 任务：每任务只下发一次、全成功、配额归零 |
| `LeaseReclaimTest` | 租约超时回收重排、迟到回传被拒、执行者归属校验、失联立即回收 |
| `CancelConcurrencyTest` | 取消 vs 回传：两个确定性顺序阶段 + 50 轮真并发竞速，结论唯一、败者被拒、配额只归还一次；PENDING 直取消 |
| `RetryLimitTest` | 失败重试上限终结、重试恢复成功、超时回收同样受上限约束 |
| `SchedulingOrderTest` | 优先级 → 公平性 → FIFO → jobId 的确定性选择 |
| `PreemptionTest` | 抢占触发并立即占用让出的能力、配额交接账实相符、被抢任务重跑完成且重试预算不退化、抢占 vs 取消/超时/回传并发结论唯一、连续抢占后免疫防饿死、受害者选择确定性、无抢占需求时不触发 |
| `JobApiIntegrationTest` | 经 REST 入口的配额、重复、超额、调度、取消、终态拒绝全链路 |

### 9.2 启动应用并用 HTTP 手动复现

```bash
mvn spring-boot:run        # 默认随机端口（application.properties 中 server.port=0，看启动日志端口）
# 或固定端口： mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8080
```

```bash
BASE=http://localhost:8080

# 1) 设置主体配额（cpu/mem/instances 任一维度超限即拒绝）
curl -s -X PUT  $BASE/api/jobs/quotas/alice -H 'Content-Type: application/json' \
  -d '{"cpu":4,"memoryMb":1024,"instances":8}'

# 2) 提交任务（payload: success | fail | failfirst:N | sleep:毫秒 | never）
curl -s -X POST $BASE/api/jobs -H 'Content-Type: application/json' -d '{
  "jobId":"j1","subject":"alice","demand":{"cpu":2,"memoryMb":256,"instances":1},
  "priority":5,"payload":"success"}'

# 3) 手动驱动一轮"租约回收 + 调度下发"（后台线程默认关闭，保证可复现）
curl -s -X POST $BASE/api/admin/tick

# 4) 查询 / 列表 / 取消
curl -s $BASE/api/jobs/j1
curl -s $BASE/api/jobs
curl -s -X POST $BASE/api/jobs/j1/cancel

# 5) 查看配额占用
curl -s $BASE/api/jobs/quotas/alice
```

要观察租约超时回收：提交 `payload="never"` 的任务 → tick 下发 → 等待超过 `lease-ttl-ms`（默认 3s）后
`POST /api/admin/reclaim`，任务回到 PENDING（或达到上限后 FAILED），配额同时归还。

要观察优先级抢占：把执行者并发调到 1（`--scheduler.local-executor.parallelism=1`）→
提交一个低优先级长任务（`"priority":1,"payload":"sleep:30000"`）→ tick 下发 →
再提交高优先级任务（`"priority":9,"payload":"success"`）→ tick，
日志可见 `[preempt-select]`/`[preempt-victim]`/`[preempt-dispatch]`：高优先级任务立即占用让出的槽位开跑，
低优先级任务回到 PENDING（attempt 退还、preemptedCount+1），之后再 tick 会被重新调度跑完。

开启后台自动调度（可选）：`--scheduler.tick-interval-ms=500`。

### 9.3 关键配置（`application.properties`）

| 键 | 默认 | 说明 |
| --- | --- | --- |
| `scheduler.lease-ttl-ms` | 3000 | 租约有效期 |
| `scheduler.max-attempts` | 3 | 最大尝试次数（含首次） |
| `scheduler.tick-interval-ms` | 0 | 后台调度间隔，<=0 关闭（手动 tick） |
| `scheduler.local-executor.count` | 1 | 本地执行者数量 |
| `scheduler.local-executor.parallelism` | 4 | 每个执行者并发槽位 |
| `scheduler.max-preemptions-per-job` | 3 | 同一任务最多被抢占次数，达到后免疫（防饿死）；0 关闭抢占 |

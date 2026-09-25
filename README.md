# 任务调度与资源配额管理（本地可复现）

一个仅依赖 Spring Boot、**不依赖任何外部服务 / 消息中间件 / 真实凭据** 的单机任务调度器。
用本地执行者（LocalExecutor）模拟任务的下发、执行与结果回传，围绕三件事给出**一致且可解释**的结论：

1. **配额准入**：按提交主体（submitter）统计"占用中"资源，超配额 / 与既有提交冲突一律拒绝，拒绝不改变既有占用；
2. **并发调度**：多个待调度任务按"优先级 + FIFO"挑选，同一任务在并发下不会被下发给两个执行者，配额不足只能等待；
3. **租约与终态**：租约超时 / 执行者失联可回收重排；取消、超时回收、结果回传并发发生时结论唯一；非法迁移被拒、重试上限终结；终态配额精确归还且不重复归还。

---

## 1. 模块结构

```
model/      JobState / Job / JobRequest / SubmitResult / DispatchResult / RejectReason
exception/  SchedulerException（非法迁移、过期租约、参数错误等领域错误）
quota/      QuotaManager         按 submitter 的配额账本（占用/获取/归还，含重复归还兜底）
store/      JobStore             jobId -> Job 的内存存储
core/       SchedulerProperties 可配置参数
            JobSchedulerService ★ 唯一权威状态机：准入/下发/回传/取消/回收，全部在同一把锁内
executor/   LocalExecutor        本地执行者：定时领取、线程内模拟执行、携带 leaseId 回传
            LeaseReaper          定时扫描并回收超时租约（模拟失联）
api/        JobController        HTTP 入口；ApiExceptionHandler 把领域错误翻译成可区分的 4xx
```

所有入口（HTTP、后台执行者、租约回收器、测试线程）都只调用同一个 `JobSchedulerService` 单例，
因此各入口结论天然一致。

---

## 2. 状态机与迁移约束

```
                 提交(准入通过)
   (不存在/终态id) ───────────► PENDING
                                  │  dispatchOne()：挑选 + 配额获取 + 颁发租约(leaseId)
                                  ▼
                               RUNNING ◄──────────────┐
              ┌───────────────┼───────────────┐       │ 回收/失败(未达上限)
   complete() │      cancel() │  reclaimExpired()     │ 归还配额、旧租约失效
     success  │               │ 或 complete(failure)  │
              ▼               ▼                       └────── PENDING
          SUCCEEDED       CANCELLED
                              │
              failure/超时 且 attempts >= maxAttempts
                              ▼
                          ABANDONED  （失败也可走 FAILED 语义；本实现以 ABANDONED 表达"重试耗尽终结"）
```

终态：`SUCCEEDED / FAILED / CANCELLED / ABANDONED`。

约束（违反即抛 `SchedulerException`，HTTP 为 409/400/404）：

| 约束 | 行为 |
|---|---|
| 对非 RUNNING 任务回传结果，且 leaseId 已失效 | 以 **过期租约（stale_lease）** 拒绝，状态不变 |
| 取消已终结任务 | **非法状态迁移（illegal_transition）** 拒绝 |
| 用旧 leaseId 重复回传 | 租约栅栏不匹配，拒绝 |
| 参数非法（空 id、cpuUnits<=0） | 400 |
| jobId 不存在 | 404 |

**终态一次性**：任务离开 RUNNING 的路径只有两个内部原语——`finishTerminal`（→终态）与
`releaseToPending`（→PENDING），二者都在持锁临界区内"归还配额 + 翻转状态 + 失效旧租约"原子完成，
配额有且仅有一次归还；`QuotaManager.release` 另对"归还后为负"做下限保护并告警，作为重复归还的兜底。

---

## 3. 配额规则

- 配额按 `submitter` 独立核算，单位为 `cpuUnits`（整数），默认上限 `scheduler.default-quota=10`。
- **只统计占用中（RUNNING）**：PENDING 不预留配额。
- **提交时准入**：若 `当前运行中占用 + 申请量 > 上限`，直接拒绝（`QUOTA_EXCEEDED`），不写入任务、不改变占用。
  > 这保证"提交即准入"有明确结论，避免无界排队；任务在 RUNNING 结束释放配额后可重新提交。
- **下发时再校验**：即使任务已在队列，`dispatchOne` 也会在同一临界区内再次做配额获取；
  余额不足则该任务保持 PENDING **继续等待，绝不越权下发**。
- 两种拒绝原因严格可区分：`QUOTA_EXCEEDED` vs `DUPLICATE_SUBMISSION`（相同 jobId 存在未终结任务）。
- 终态（成功/取消/重试耗尽）或回收重排都会释放对应占用。

---

## 4. 调度、优先级与公平性

挑选顺序是一个**全序**，可解释、可复现：

```
priority 数值大的优先  →  同优先级按 enqueueSeq 升序（FIFO）  →  再按 jobId 升序兜底
```

- `enqueueSeq` 在准入通过时单调递增，因此同优先级先到先得，不会饥饿。
- 每次 `dispatchOne()` 最多下发一个任务；队首 PENDING 任务若配额不足，则**阻塞其后的低优先级任务**
  （不抢占、不插队、不绕过），全部继续等待。
- "挑选 → 配额获取 → 状态翻转 RUNNING → 颁发唯一 leaseId" 在同一把全局锁内完成，
  因此多执行者并发调度时，每个任务**恰好被一个执行者领取**（有并发争抢测试证明）。

---

## 5. 租约协议

1. 下发时调度器生成全局唯一 `leaseId` 并记录绝对到期时间 `now + lease-ttl-millis`。
2. 执行者执行结束后调用 `complete(jobId, leaseId, success)`，**必须携带该 leaseId**（fencing token）。
3. `LeaseReaper` 周期扫描：`state==RUNNING && now >= leaseExpiresAt` 视为执行者失联：
   - `attempts < maxAttempts`：归还配额、旧租约失效、回到 PENDING 等待再次下发；
   - `attempts >= maxAttempts`：归还配额并终结为 `ABANDONED`。
4. **取消 / 超时回收 / 结果回传并发**时，三者竞争同一把锁：
   - 先到者完成状态迁移并把旧 leaseId 置空；
   - 后到者面对的是"leaseId 不匹配"（过期租约拒绝）或"已非 RUNNING"（非法迁移拒绝）；
   - 因而最终状态唯一、配额结论唯一（有 50 轮并发交错测试证明）。

---

## 6. 重试规则

- `attempts` 在每次**下发**时 +1；"执行失败回传"与"租约超时回收"共享同一计数。
- `attempts < scheduler.max-attempts`（默认 3）：失败/超时 → 回 PENDING 重试。
- `attempts >= maxAttempts`：失败/超时 → 终结为 `ABANDONED`，`finishReason` 写明"已达重试上限"，不再被挑选。
- 第 N 次执行若成功，正常终结为 `SUCCEEDED`，与是否曾失败无关。

---

## 7. 配置项（`src/main/resources/application.properties`）

| 键 | 默认 | 含义 |
|---|---|---|
| `scheduler.default-quota` | 10 | 每提交主体运行中资源上限 |
| `scheduler.max-attempts` | 3 | 最大下发次数，达到后失败/超时即 ABANDONED |
| `scheduler.lease-ttl-millis` | 5000 | 租约有效期 |
| `scheduler.reaper-interval-millis` | 1000 | 租约回收扫描间隔 |
| `scheduler.executor-enabled` | true | 本地执行者/回收器开关（测试环境关闭） |
| `scheduler.executor-interval-millis` | 500 | 执行者领取周期 |
| `scheduler.execute-delay-millis` | 800 | 模拟单个任务执行耗时 |

测试配置 `src/test/resources/application.properties` 关闭后台执行者与回收器，
所有调度动作由测试线程显式驱动；超时通过注入可拨快的 `Clock` 确定性制造，不依赖真实 sleep。

---

## 8. HTTP 接口（本地验证）

| 方法与路径 | 说明 |
|---|---|
| `POST /api/jobs` | body：`{jobId, submitter, cpuUnits, priority}`；成功 201，拒绝 409（body 内 `reason` 区分原因） |
| `POST /api/dispatch` | 手动触发一次挑选下发，返回 `{dispatched, jobId, leaseId, reason}` |
| `POST /api/jobs/{id}/complete?leaseId=..&success=true` | 执行者回传结果 |
| `POST /api/jobs/{id}/cancel` | 取消 PENDING/RUNNING；终态再取消返回 409 |
| `GET  /api/jobs/{id}` | 查询状态、attempts、租约、终态原因 |
| `GET  /api/quota?submitter=..` | 查询 `{used, limit}` |

---

## 9. 本地构建与验证

```bash
# 1) 编译 + 全部单元测试（无需任何外部服务）
mvn clean test

# 2) 打包并启动（随机端口由 server.port=0；也可指定 --server.port=8080）
mvn package -DskipTests
java -jar target/job-scheduler-quota-java-1.0-SNAPSHOT.jar --server.port=8080

# 3) 另开终端演示（后台执行者会自动领取并执行）
curl -s -X POST localhost:8080/api/jobs -H 'Content-Type: application/json' \
  -d '{"jobId":"alpha","submitter":"team-a","cpuUnits":4,"priority":5}'
sleep 2
curl -s localhost:8080/api/jobs/alpha
curl -s "localhost:8080/api/quota?submitter=team-a"
```

### 测试覆盖（`src/test/.../tests`，共 15 个用例）

| 测试类 | 覆盖场景 |
|---|---|
| `QuotaAdmissionTests` | 配额耗尽拒绝且占用不变；PENDING 不占配额；配额不足继续等待不越权；释放后恢复准入 |
| `DuplicateSubmitTests` | PENDING/RUNNING 重复提交以 DUPLICATE_SUBMISSION 拒绝；与 QUOTA_EXCEEDED 可区分；终态后 id 可复用 |
| `PriorityFairnessTests` | 优先级 + 同优先级 FIFO 的全序；队首配额不足时其后任务等待不绕过 |
| `ConcurrentDispatchTests` | 16 执行者抢 20 任务 / 32 线程抢 1 任务：每任务恰一次中标、leaseId 唯一、占用严格一致 |
| `LeaseAndCancelTests` | TTL 边界回收、过期租约迟到回传被拒、运行中取消释放配额、非法迁移拒绝、取消/回收/回传 50 轮并发结论唯一 |
| `RetryLimitTests` | 失败与超时共享 attempts，达上限 ABANDONED；终结后不再下发、回收不重复归还 |

所有判定日志均打印 **任务标识（jobId）、配额占用（used/limit）与判定依据**，例如：

```
准入拒绝[配额超限]: jobId=j3, submitter=team-a, 申请=2, 占用=10/10, 依据=占用+申请>配额, 拒绝后占用仍=10
下发成功: jobId=big, submitter=team-b, leaseId=7d7e..., 第1次执行, 需要=10, 下发后占用=10/10, TTL=5000ms
回传拒绝[过期租约]: jobId=race-2, 回传leaseId=2f0e..., 当前leaseId=null, 当前状态=CANCELLED, 依据=租约栅栏不匹配
```

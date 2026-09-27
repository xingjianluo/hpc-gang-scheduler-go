# HPC Gang Scheduler（进程内模拟版）

一个可以在本机直接跑起来的 HPC 作业调度组件：用**进程内节点 + 资源账本**模拟集群，
不依赖任何外部集群服务或真实凭据（仅用标准库）。它把三条核心链路做通了：

1. **整组调度（gang scheduling）**：作业 / 作业数组的所有任务必须整组同时拿到资源，
   资源不足、超配额、重复提交一律**整组拒绝**，绝不出现只启动一部分任务却占着资源的情况。
2. **优先级抢占 + 资源回填**：高优先级作业可以抢占标记为“可中断”的低优先级作业；
   被抢占任务整组回队列、账本同步归还。多提交方并发操作时同一份资源不会被重复分配，
   任何时刻 `used <= capacity`。
3. **重试 / 超时 / 节点失联 / 状态恢复**：执行事件允许重复、乱序到达，靠
   “实例代号 + 事件序号”去重；终态任务不会被改回运行中；调度器重建后从持久化
   快照继续，而不是从头重新提交。

## 目录结构

```
internal/types/      领域类型：Resources、JobSpec、状态枚举、RejectError
internal/store/      快照持久化：Store 接口 + MemoryStore / FileStore（临时文件+rename 原子写）
internal/cluster/    进程内模拟集群：Node、Cluster（启动/取消/失联）、Executor（执行模拟器）
internal/scheduler/  调度核心：Ledger 资源账本、整组调度/抢占/事件状态机、快照恢复
cmd/hpcsim/          本地可运行的端到端演示
```

## 快速验证

```bash
# 1) 端到端演示（整组拒绝 / 抢占 / 超时重试 / 节点失联重复事件 / 恢复）
go run ./cmd/hpcsim

# 2) 单元测试（含竞态检测，建议用 -count 多跑几轮）
go test ./... -race -count=20 -v

# 3) 观察跨进程恢复：第一次运行保留快照，第二次启动会打印 "从快照版本 N 恢复"
go run ./cmd/hpcsim --keep-snapshot
go run ./cmd/hpcsim --snapshot <上面打印的快照路径>
```

演示日志里会持续打印两类可对照的信息：

- **调度决策**：`作业已入队 / 拒绝提交 / 调度决策-整组启动 / 调度决策-抢占 / 失联处理 …`
- **资源账本快照**：每个节点的 `used / cap / running`，例如
  ```
  node=node-a used={cpu:4 mem:8 gpu:0} cap={cpu:4 mem:16 gpu:1} running=2
  ```
  单测日志（`t.Logf("[decision] ...")`）同样会输出这两类信息。

## 调度语义

### 作业与作业数组

`JobSpec{Procs: N}` 表示一个含 N 个任务的作业数组（`N=1` 即普通作业）。
任务 ID 为 `JobName[index]`，全局唯一。作业状态：`QUEUED / RUNNING / COMPLETED / FAILED`；
任务状态：`PENDING / RUNNING / COMPLETED / FAILED`。

### 整组调度（all-or-nothing）

调度扫描按 **优先级降序、同优先级按提交先后（FIFO）** 处理 QUEUED 作业。
一个作业的所有 PENDING 任务在“工作副本装箱”中**全部**找到落位时才一次性：

1. （必要时）抢占受害者并逐任务归还账本；
2. `Ledger.TryReserveGang` 整组落账——任何一个任务放不下，整次调用对账本零改动；
3. 整组下发到集群。若下发中途有节点启动失败，已启动部分会被杀掉、资源回滚、整组回队。

因此系统中**不会存在“同一作业启动了一部分任务”的中间状态**。
当作业只是“当前放不下、但集群总量能容纳”时，它保持 QUEUED 等待抢占或回填；
高优先级作业排前不代表低优先级作业会被饿死——扫描是保守的 FIFO 回填，
只要不影响更高优先级作业，空闲资源会被用来启动排队作业。

### 整组拒绝的三类可区分原因

提交（`Submit`）返回 `*types.RejectError`，`Reason` 取值：

| Reason | 触发条件 |
|---|---|
| `duplicate_submission` | 同名作业已存在（无论它是否已到终态） |
| `quota_exceeded` | 该租户“在队 + 在跑”资源加上本次整组需求超过租户配额 |
| `insufficient_capacity` | 单任务放不进任何节点，或整组总需求超过集群总容量（永远不可能被调度） |

被拒绝的作业**完全不进入系统**：查不到作业状态，账本也不发生任何变化。
注意判定顺序是先容量可行性、再配额——一台永远放不下的机器报的是“容量不足”，
而不是“配额超限”。仅“当前资源被占用”不属于拒绝，作业会排队等待。

### 配额规则

`Config.Quotas[tenant]` 给出该租户非终态作业（QUEUED + RUNNING，按整组需求计）
的资源上限；未配置的租户不限额。作业到 COMPLETED / FAILED 后即释放配额占用。
被抢占回队列的任务仍属于该租户的在队占用。

### 抢占规则

- 受害者必须同时满足：**严格更低优先级**且作业标记 `Interruptible: true`。
  同级优先级、不可中断作业都不会被抢占。
- 抢占粒度为整组装箱时实际占用了资源的那些任务（可能来自一个或多个受害者作业）；
  受害者选择顺序：优先级从低到高、同级按提交先后。
- 抢占动作：向集群发停止信号 → 账本逐任务归还 → 任务回 PENDING（`Preempted` 标记），
  作业重算为 QUEUED，等待后续整组调度。
- **抢占 / 节点失联导致的回队不消耗用户重试预算**；只有真实的 `failed / timeout`
  才计数（`FailCount`）。

## 重试、超时与失联

- **重试**：`Retries` 是失败重试预算。任务收到 `failed / timeout` 时若
  `FailCount <= Retries` 则回 PENDING 等待重新整组调度，否则任务 FAILED；
  作业内有任务 FAILED 且全部到终态时，作业 FAILED。
- **超时**：`TimeoutMs > 0` 时由执行器在任务 ctx 上设置 deadline，超时发 `timeout` 事件。
- **节点失联**：`node_lost` 幂等（重复事件只生效一次）。失联节点上的在跑任务被取消、
  账本清空；为维护整组语义，**同一作业在其它存活节点上的同伴任务也会一并停掉、整组回队**，
  节点恢复后重新整组分配（账本容量始终保留，恢复即可继续记账）。
- **节点恢复**：`node_up` 清除失联标记并唤醒一次调度扫描。

## 事件去重与幂等

执行事件结构为 `Event{TaskID, Kind, NodeName, Instance, Sequence}`：

- `Instance` 是调度器每次启动任务分配的**实例代号**。被抢占 / 失联 / 超时杀死的旧实例
  之后才到达的任何事件（哪怕带着“completed”）都会被直接忽略，无法影响新实例状态。
- 同一实例内再按单调 `Sequence` 过滤乱序 / 重复事件（先收到 seq=10 后，seq<=10 全部忽略）。
- 终态（COMPLETED / FAILED）任务收到的重复完成通知、迟到 started 一律忽略，
  **终态不可逆**，也不可能重复计数。
- 节点级事件（node_lost/node_up）单独做幂等处理。

## 状态持久化与恢复

每次入队、整组启动、任务终态、重试、抢占、失联/恢复后，调度器都会把状态
（版本号、全部作业与任务状态、失联节点集合、提交序号）写入 `Store`：

- `FileStore`：JSON 信封写临时文件 → `fsync` → 原子 `rename`，崩溃后只会读到完整快照；
- `MemoryStore`：测试用的并发安全内存实现。

`Recover` 语义：

- 快照里 RUNNING 的任务在新调度器中没有对应执行实体，**一律整组回 PENDING**
  （保留 FailCount 等计数），由后台扫描重新整组分配；
- PENDING / COMPLETED / FAILED 保持不变，作业从快照继续推进，不重新提交、不重头计数；
- 失联节点集合恢复，等 `node_up` 后容量重新可用。

## 测试覆盖

| 测试 | 覆盖场景 |
|---|---|
| `TestGangRejectReasons` | 整组拒绝的三种可区分原因；被拒作业不入系统 |
| `TestGangAtomicNoPartialStart` | 容量临界变化下整组排队、部分占用为零、释放后整组回填 |
| `TestGangStartsAllOrNothing` | 资源充足时 N 个任务整组同时 RUNNING |
| `TestPreemptInterruptibleGang` | 抢占整组受害者、账本归还、不消耗重试预算、之后重新整组跑完 |
| `TestNoPreemptNonInterruptible` | 不可中断 / 同级优先级不可抢占 |
| `TestConcurrentSubmitNoDoubleAllocation` | 12 个提交方并发提交，高频巡检账本不超分、CPU 与任务数一致、收敛归零 |
| `TestConcurrentSubmitDuplicateNames` | 32 路并发同名提交，恰好 1 个成功其余 duplicate |
| `TestPreemptionUnderConcurrentLoad` | 满载抢占竞态下账本始终不超分，最终高优先级全完成、底座整组恢复 |
| `TestDuplicateAndStaleEvents` | 乱序 / 重复完成 / 旧实例迟到事件不能改状态或重复计数 |
| `TestRetryAndTimeout` | 失败重试预算、执行超时、终态账本归零 |
| `TestNodeLostRepeatedAndRecovery` | 失联重复事件幂等、账本回收、整组回队、恢复后整组重跑 |
| `TestRecoverFromSnapshot` | 调度器重建后终态保持、RUNNING 回队、失联节点恢复后继续推进 |
| `TestLedger*` / `Test*Store*` | 账本整组预留原子性、跨节点重复落账防护、并发不超分、快照持久化往返 |

竞态稳定性验证：

```bash
go test ./... -race -count=30
```

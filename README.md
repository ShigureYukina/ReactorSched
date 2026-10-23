# ReactorSched

**分布式任务调度平台：调度中心与执行器分离，MySQL 作为唯一状态来源。**

[![Java](https://img.shields.io/badge/Java-21-orange?logo=openjdk)](https://openjdk.org/projects/jdk/21/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.5-6DB33F?logo=springboot)](https://spring.io/projects/spring-boot)
[![MySQL](https://img.shields.io/badge/MySQL-8-4479A1?logo=mysql&logoColor=white)](https://www.mysql.com/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

> **项目状态：S1–S5 全部完成，项目收尾。** 定义层（任务管理、两套状态机、审计、参数校验）、定时调度（到期扫描、乐观锁抢占、misfire 三策略、实例自动生成）、执行器分离（注册/心跳/分组拉取/结果回传/租约校验、执行器 SDK、强杀接替）、可靠性与过载（分片重分配、分组级共享限速与并发、入口背压、故障注入四类）与观测和交付（PRD §8.6 全量指标 + histogram + P99、Grafana 面板、单 jar 双角色 Compose 多实例、DEPLOY/CONFIG 文档、性能报告、可运行演示脚本）已落地。本文档只描述**已经能跑**的部分。

---

## 1. 项目来历

ReactorSched 是 cleanroom 新建的仓库，**代码以 `YukiReactor`（同级目录）为出发点复制并整库重命名**：

| 概念 | YukiReactor | ReactorSched |
|---|---|---|
| 包名 | `com.yuki.reactor.<module>` | `com.yuki.reactorsched.<module>` |
| 模块 | `reactor-*` / `task-*` | `reactorsched-*` |
| 配置前缀 | `reactor.*` | `reactorsched.*` |

两个仓库互不依赖：ReactorSched 不引用 YukiReactor 的任何模块，运行期也不读它的目录。逐文件的复用与改造清单见 `docs/REUSE-PLAN.md`。

---

## 2. 这个项目解决什么

多数 Java 系统把「按计划触发」和「可靠执行」塞进同一个 `@Scheduled` 方法，于是：

- 多实例部署时同一个 cron 被每个实例各跑一遍。
- 任务定义与执行记录混在一张表，改一次调度就把历史改花了。
- 执行器进程崩溃后，调度中心不知道它还在不在。
- 出问题只能说「重试了 5 次」，说不出为什么。

ReactorSched 把这几件事拆开：**调度中心只管什么时候触发、生成哪些实例；执行器只管把实例跑完并回报；两者之间的每一次交接都由数据库条件更新裁决。**

---

## 3. 当前实现（S1 + S2 + S3 + S4 + S5）

### 3.1 两级状态机

**定义 `job_definition.status`**：`ENABLED ⇄ PAUSED`，两者都可 `DELETED`（软删，终态）。
**实例 `task.status`**：`PENDING → RUNNING → SUCCEEDED / RETRY_WAIT / FAILED`，`RETRY_WAIT → RUNNING`，`FAILED → PENDING`（仅管理员重跑）。`RUNNING → CANCELLED` 不是合法自动转换。

两套状态机分别由 `JobStateMachine` 与 `TaskStateMachine` 持有，**不得互相推导**。

### 3.2 一致性手段

| 手段 | 落在哪 | 作用 |
|---|---|---|
| 条件 UPDATE | `TaskMapper`、`JobDefinitionMapper` | 状态写入带 `WHERE status / fencing_token / 乐观锁值`，受影响行数为 0 即本次竞争失败 |
| 唯一约束 | `job_definition.job_key`、`task.identity_key`、`(idempotency_scope, idempotency_key)` | 幂等由数据库裁决，不靠内存标记 |
| 幂等插入 | `TaskMapper.insertInstanceIfAbsent` | `INSERT ... ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)`，禁止先查后插 |
| 乐观锁推进 | `JobDefinitionMapper.claimFireTime` | 两个调度节点只有一个能把 `next_fire_at` 推进一步 |
| 执行权版本 | `task.fencing_token` | 每次领取 +1，迟到写入被拒并记为 `STALE_WRITER` |
| 操作审计 | `operation_audit` | 与业务同事务，写不进去则业务回滚 |

### 3.3 定义层 API

| 方法 | 路径 | 权限 |
|---|---|---|
| `POST` | `/api/v1/jobs?operator=` | ADMIN |
| `PUT` | `/api/v1/jobs/{id}?operator=` | ADMIN |
| `POST` | `/api/v1/jobs/{id}/pause`、`/enable`、`/trigger` | ADMIN |
| `DELETE` | `/api/v1/jobs/{id}` | ADMIN |
| `GET` | `/api/v1/jobs`、`/jobs/{id}`、`/jobs/{id}/instances` | 开放 |

- 调度类型：`CRON`（6 段含秒，UTC）、`FIXED_RATE`、`ONE_TIME`；错过调度策略 `SKIP` / `FIRE_ONCE` / `CATCH_UP`，**执行逻辑已随 S2 落地**（见 §3.4）。
- `POST /jobs/{id}/trigger` 手动触发，`clientKey` 保证同一键只生成一个实例。
- 参数校验用 Bean Validation，失败返回 `{"code":"INVALID_REQUEST","details":[{"field":..,"reason":..}]}`。

### 3.4 定时调度（S2）

`reactorsched-scheduler` 模块按 `scan-interval`（默认 1s）扫描到期定义，把每个触发时刻变成实例：

- **抢占**：`next_fire_at` 既是读取值也是条件 UPDATE 的比较值，多调度节点只有一个能推进；抢不到的一方计数让过，不重试不重复（`MultiSchedulerTriggerIT`：20 个定义两节点竞争，每个触发时刻全局只 fired 一次）。
- **先推进后生成**：触发时刻先推进再创建实例，两步分离事务；创建失败只记 `JOB_TRIGGER_FAILED` 决策，不回滚推进——可丢不可重。
- **misfire 三策略**：`SKIP` 折叠全部错过时刻、直接跳到第一个新鲜时刻；`FIRE_ONCE` 只为最近一个错过时刻补发（实例带原计划时刻）；`CATCH_UP` 逐个补发至 `max-catch-up`（默认 20），超出的部分跳过并记 `MISFIRE_CATCH_UP_LIMIT`。每次决策落 `scheduler_decision` 表。
- **计划完成即停机**：一次性任务与无后续出现的 cron 在最后一次触发后置 `PAUSED` + `next_fire_at = NULL`，不再被扫描到。
- **时钟漂移探测**：启动时与数据库时钟（`SELECT NOW(3)`）比对，超 `max-clock-skew`（默认 2s）打 WARN；`/actuator/health` 的 `scheduler` 项上报漂移与最近一轮扫描统计。
- 配置前缀 `reactorsched.scheduler`（`enabled` / `scan-interval` / `batch-size` / `max-catch-up` / `max-clock-skew`），只serve API 的进程设 `enabled=false`。

### 3.5 实例层（V1.x 执行内核，已可用）

直接提交实例的兼容入口 `POST /api/v1/tasks` 保留：提交、查询、执行历史、取消、重放、时间线、决策查询、指标摘要、下游故障注入。租约续约、过期回收、指数退避、AIMD 自适应并发都可用，规则见 `AGENTS.md`。

### 3.6 执行器分离（S3）

执行器进程不再依赖调度中心代码库：handler SPI 独立为 `reactorsched-executor-api`（纯 Java，无 Spring/DB 依赖），执行器侧用 `reactorsched-executor-sdk` 一行注解（`@EnableReactorSchedExecutor`）接入，按 REST 五步契约（注册 → 心跳 → 拉取 → 执行 → 回传）与调度中心交接，**不直连数据库**。

- **注册与心跳**：`(app_name, instance_id)` 唯一，同实例重启复用同一行；心跳超 `offline-threshold`（默认 30s）被清扫为 OFFLINE 并停止派活，心跳恢复可复活（`ExecutorRegistrationIT`）。
- **分组路由**：任务的 `executorGroup` 与执行器分组必须匹配，约束在 claim 的 WHERE 里，异组执行器拿不到实例（`GroupAllocationIT`）。
- **执行权校验**：领取时 `fencing_token +1`；结果回传/续约必须携带该值并写进 WHERE，过期写入被拒（409 `LEASE_VERSION_STALE`）且 attempt 关为 `ABANDONED` + `STALE_WRITER`（`ExecutorResultIT`）。
- **线程池治理**：有界队列（`queue-capacity` 256）+ 显式拒绝策略，队列满即回传 `REJECTED` → 服务端转 `RETRY_WAIT` + `EXECUTOR_REJECTED`；支持 `task-type-limits` 每类型并发上限。
- **强杀接替**：两个执行器同库运行，强杀其中一个后其在途实例被租约回收（`ABANDONED`/`LEASE_EXPIRED`），另一个执行器接替执行成功（`TwoExecutorsKillOneIT`，连跑 3 次稳定）。
- 执行日志随结果落库（`log_text`/`log_truncated`，服务端按字节截断不切 UTF-8），执行历史可读；执行器侧 REST 端点用 `X-ReactorSched-Token` 认证。

### 3.7 可靠性与过载（S4）

- **分片重分配**：分片实例走普通租约回收路径，回收写 `SHARD_REASSIGNED` 决策（`shard_index` 不变），任意同组执行器可接替（`ShardReassignIT`：crash 后每分片一条决策、索引 0/1 不变、attempt 2 完成）。
- **分组级共享限速**：重试派发的每秒上限存 `group_rate_window`（`(executor_group, window_second)` 主键，条件更新原子计数），跨执行器进程可见——MySQL 行竞争即限流，不做进程内计数（`GroupRateLimitIT`：双 worker 共享 1/s 上限）。
- **分组共享 AIMD 并发**：控制器每次调整发布到分组行（`window_second=0` 状态行），worker 每轮 poll 重读并跟随，与控制器是否同进程无关（`AdaptiveConcurrencyIT` 发布-跟随例）。
- **入口背压**：due backlog 超过 `backlog-reject-threshold` 或等待队列超过 `queue-reject-threshold` 时，提交与手动触发返回 429 + `BACKPRESSURE_REJECTED` 决策（`BackpressureIT`）。
- **故障注入四类**（`/api/v1/fault-lab/`，ADMIN + operator + 审计）：executor crash（模拟进程死亡，在途尝试留待租约回收）、duplicate-result（同执行权版本重放 → `STALE_WRITER`）、connect-hang-ms（连接挂起被协作式超时切断）、database（`READ_ONLY`/`LATENCY` 拦截写语句，审计豁免）。`FaultInjectionRecoveryIT` 四类各有恢复断言。

### 3.8 观测、界面与交付（S5）

- **全量指标**（PRD §8.6，`/actuator/prometheus` 抓取）：`reactorsched.task.completed`（result/task_type/executor_group）、`task.execution.duration` 与 `task.queue.wait`（Timer **开 histogram**，SLO 桶 10ms~30s，P99 由 `histogram_quantile` 算）、`job.trigger`、`executor.heartbeat.age`/`online`（按 executor_group）、`backlog.due`/`running`、`concurrency.limit`/`.max`/`.adjustments`、`decision.recorded`（决策原因分布面板数据源，insert 成功后计数）。`ReactorMetricsTest` 逐项钉死命名与标签；`MetricsPublishingIT` 用真实任务验证发布。
- **Grafana 面板**（`deploy/grafana/dashboards/reactor-overview.json`，11 面板）：完成速率、执行 P50/P95/P99、排队 P99（首次/重试）、积压与在飞、重试速率、执行器存活与心跳年龄、分组共享并发、调整频率、触发速率、决策原因分布、实例存活。Compose `monitoring` profile 一键起 Prometheus（DNS service discovery）+ Grafana。
- **单 jar 双角色**：同一 artifact，executor profile 只加载 SDK 与 handlers，**不连数据库**（`ExecutorProfileIT` 钉死）；Compose 默认拓扑 = scheduler ×N（可 `--scale`）+ executor-a/b。
- **管理界面**：`/panel/index.html` 只读面板（任务列表、执行历史、失败原因、时间线、决策查询、指标摘要）；管理写端点 ADMIN，审计同事务入库。
- **文档与脚本**：`docs/DEPLOY.md`（拓扑/启动顺序/运维动作）、`docs/CONFIG.md`（全配置项）、`docs/PERFORMANCE.md`（baseline/aimd 两组真实压测数据与 AIMD 取舍解读）、`scripts/demo.ps1`（PRD §16 全流程：定义 → 分片 → 在途崩溃 → 回收重跑）。

---

## 4. 状态

| 能力 | 状态 | 证据 |
|---|---|---|
| 工程骨架（13 模块，编译通过） | 完成 | `mvnw -DskipTests package` |
| 既有执行内核改名后行为不变 | 完成 | Testcontainers + MySQL 8.4 全绿 |
| 定义层：CRUD / 启停 / 软删 / 手动触发 | 完成 | `JobLifecycleIT` |
| 两套状态机 | 完成 | `JobStateMachineTest`、`TaskStateMachineTest` |
| cron 下一时刻计算 | 完成 | `ScheduleCalculatorTest`（含月末、闰年、跨年） |
| 错过时刻算术（misfire 决策依据） | 完成 | `MissedFireTimesTest`（表驱动，含闭式/有界/ONE_TIME） |
| **定时调度（扫描、抢占、misfire、自动生成实例）** | **完成（S2）** | `SchedulerScanIT`、`MultiSchedulerTriggerIT` |
| 时钟漂移探测 | 完成（S2） | `DatabaseClockProbe` + actuator `scheduler` health |
| 操作审计入库 | 完成 | `operation_audit` + `JobLifecycleIT` |
| **独立执行器（注册、心跳、分组拉取、结果回传、执行权校验）** | **完成（S3）** | `ExecutorRegistrationIT`、`GroupAllocationIT`、`ExecutorResultIT` |
| **2 执行器同库运行 + 强杀一个后被接替** | **完成（S3）** | `TwoExecutorsKillOneIT`（连跑 3 次稳定） |
| **静态分片的状态追踪** | **完成（S4：生成 + 重平衡）** | `SchedulerScanIT.shardedDefinitionCreatesOneInstancePerShard`、`ShardReassignIT` |
| **分组级共享限速与并发** | **完成（S4）** | `GroupRateLimitIT`（跨实例共享）、`AdaptiveConcurrencyIT`（发布-跟随） |
| **入口背压（429 + 决策）** | **完成（S4）** | `BackpressureIT` |
| **故障注入（崩溃/重复消息/网络超时/数据库异常）** | **完成（S4）** | `FaultInjectionRecoveryIT`（四类各有恢复断言） |
| **PRD §8.6 全量指标 + histogram（P99 可算）** | **完成（S5）** | `ReactorMetricsTest`、`MetricsPublishingIT` |
| **Grafana 面板（含决策原因分布）** | **完成（S5）** | `deploy/grafana/dashboards/reactor-overview.json`（11 面板） |
| **管理界面（列表/历史/失败原因/时间线/决策）** | **完成（S5）** | `/panel/index.html` + 演示脚本走查 |
| **单 jar 双角色（executor profile 不连库）** | **完成（S5）** | `ExecutorProfileIT` |
| **Compose 多实例（2 调度 + 2 执行器 + 监控）** | **完成（S5）** | `docker compose config` 冒烟 + `docs/DEPLOY.md` |
| **性能报告（baseline/aimd 两组实测）** | **完成（S5）** | `docs/PERFORMANCE.md`、`benchmark-results/*.json` |
| **可运行演示脚本（PRD §16 全流程）** | **完成（S5）** | `scripts/demo.ps1`，`_demo-run.log` EXIT=0 |

---

## 5. 快速开始

前置：JDK 21、Docker。

```powershell
# 1. 启动 MySQL 8（库名 reactorsched，用户 reactorsched / reactorsched123）
docker compose up -d mysql

# 2. 启动应用（Flyway 自动建表）
$env:JAVA_HOME="$env:USERPROFILE\.jdks\jdk-21.0.12.1+1"
.\mvnw.cmd -pl reactorsched-app -am spring-boot:run

# 3. 创建一个定义（管理写操作需要 ADMIN）
curl.exe -s -X POST "http://localhost:8080/api/v1/jobs?operator=alice" `
  -u admin:reactor-dev -H "Content-Type: application/json" `
  -d '{"jobKey":"nightly-report","name":"Nightly report","taskType":"echo",
       "scheduleType":"CRON","cronExpr":"0 0 2 * * *","shardTotal":3}'

# 4. 手动触发一次并查看生成的实例
curl.exe -s -X POST http://localhost:8080/api/v1/jobs/1/trigger `
  -u admin:reactor-dev -H "Content-Type: application/json" `
  -d '{"operator":"alice","reason":"demo","clientKey":"demo-1"}'
curl.exe -s http://localhost:8080/api/v1/jobs/1/instances
```

> 定义触发已自动化：启用的定义按计划生成实例，可立刻被内嵌执行器执行（S2 扫描器默认开启；只serve API 的进程设 `reactorsched.scheduler.enabled=false`）。

端到端走查（提交执行、取消审计、故障重试、分片崩溃重跑、指标面板）：

```powershell
pwsh -NoProfile -File scripts/demo.ps1
```

完整拓扑（2 调度 + 2 执行器 + Prometheus/Grafana）见 `docs/DEPLOY.md`：`docker compose --profile monitoring up -d`。

---

## 6. 测试

```powershell
$env:JAVA_HOME="$env:USERPROFILE\.jdks\jdk-21.0.12.1+1"
.\mvnw.cmd -B verify      # 单元 + 集成（Testcontainers 起真实 MySQL 8.4，不用 H2）
```

---

## 7. 文档

| 文档 | 内容 |
|---|---|
| `docs/PRD.md` | 需求唯一来源（V1.0） |
| `AGENTS.md` | 开发纪律与红线 |
| `docs/REUSE-PLAN.md` | 从 YukiReactor 复用/改造/重写的逐文件清单 |
| `docs/PHASE-S1.md` | S1 交付说明与验证记录 |
| `docs/PHASE-S2.md` | S2 交付说明：定时调度（扫描、抢占、misfire）与验证记录 |
| `docs/PHASE-S3.md` | S3 交付说明：执行器分离（注册、心跳、分组拉取、强杀接替）与验证记录 |
| `docs/PHASE-S4.md` | S4 交付说明：分片重分配、分组共享限速与并发、背压、故障注入与验证记录 |
| `docs/PHASE-S5.md` | S5 交付说明：指标与 histogram、Grafana、双角色 Compose、压测报告、演示脚本与验证记录 |
| `docs/DEPLOY.md` | 部署拓扑、启动顺序、执行器角色、运维动作 |
| `docs/CONFIG.md` | 全部配置项与环境变量速查 |
| `docs/PERFORMANCE.md` | 性能报告：baseline/aimd 两组实测数据与 AIMD 取舍解读 |

## 8. License

[MIT](LICENSE)

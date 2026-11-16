# 开发协议 v1

协议用于新系列本地开发。生产主题 ACL、平台字段和实际 MinIO 约定须在切换前联调确认。

## 身份与提交

内部 JSON 信封包含 `protocol_version: 1`、`cluster_id`、`robot_id`、`boot_id`、`to`、`kind`、`round_epoch`、`data`。sender 必须等于 MQTT 主题末尾身份；这依赖 Broker 认证和 ACL，信封本身不是数字签名。

| 字段 | 生命周期 |
| --- | --- |
| round_epoch | 每个新任务轮次增加，由平台持久化生成 |
| term | 同轮选举任期，单调持久化 |
| robot_ids | 首次指定的固定投票名单，决定多数派 |
| sessions | 激活时绑定 Agent session_id 与 consensus_boot |
| excluded | 只增不减；隔离后不再执行／投票，不降低多数派 |
| revision | 新增、掉线重分配及整轮最终补执行的方案版本 |
| commit_index | 共识提交位置，不代表物理执行完成 |
| route_name | `sw<epoch>-<mission摘要16位>-r<revision>-<robot_id>` |

Raft 通过 prevote、持久投票、日志新旧比较、当前任期提交及关联复制 ACK 维护权限。执行授权额外经过多数派 read barrier，不能仅凭 Leader 心跳或 MQTT PUBACK 续发。

## MQTT 主题

```text
<mqtt_prefix>/<cluster>/broadcast/<sender>
<mqtt_prefix>/<cluster>/to/<recipient>/<sender>
<mqtt_prefix>/<cluster>/media/broadcast/<sender>
<mqtt_prefix>/<cluster>/media/to/<recipient>/<sender>
```

普通订阅，全员收广播；不使用共享订阅或 retained 控制。控制和图片各持一个 MQTT 连接，各有有界队列；过期控制不积压，复制日志和应用 outbox 负责修复丢失。Broker 中断后授权到期请求暂停，不能继续形成多数派。

ACL 至少约束：设备只能以自己的 sender 后缀发布；只能订阅本集群广播和发给自己的定向主题。平台请求和 STS 只开放给授权部署环境；不能将开发匿名配置用于公网。

## 提交状态与执行

命令包括 MISSION、ACTIVATE、PLAN、PREPARED、START、STARTED、RESULT、EXCLUDE、PAUSE、STOPPED、ADD、REPLAN、STOP、FINISH。`MissionState.apply` 先在副本校验，成功后一次安装；重复 command id 不产生第二次状态推进，冲突内容拒绝。

Agent 仅根据本机已应用的提交决定准备／启动；YAML 生成不是物理启动。`starting` 写入磁盘后才调用 TaskHub，迟到成功响应在世代失效后触发补偿取消。

暂停确认包含该次停止后的累计导航快照。新路线开始前，已暂停的旧 TaskHub 路线必须取消。TaskHub 调用超时保留在途调用，不并发发出替代启动；记录物理状态未知，等待排空后取消。进程卡死等情形不具备硬件停机保证。

## TaskHub 累计导航接口

沿用已有服务 `StartRoute`、`GetStatus` 和 Trigger 暂停／取消。权威证据为 `/task_hub/navigation_results` 的标准 String JSON；模拟前缀下为 `<prefix>/navigation_results`。

```json
{
  "protocol_version": 1,
  "route_name": "sw101-摘要-r1-X30-A",
  "execution_id": "TaskHub本次启动唯一值",
  "snapshot_sequence": 2,
  "route_state": "COMPLETED",
  "finalized": true,
  "reached_waypoint_ids": [1],
  "skipped_waypoints": [{"waypoint_id": 2, "reason": "ABORTED timeout"}],
  "pending_waypoint_ids": []
}
```

三类必须互斥覆盖生成 YAML 的全部航点；封存后不可变，同次执行序号递增，已到达不能倒退。索引越过、move.complete 提示、挂载操作成功均不能替代导航证据。终态后最多核对十秒，缺失结果保留 UNKNOWN，不猜测跳点。

Agent 按本机 waypoint 到原始 task id 映射生成结果，可靠 outbox 重发到多数派提交 ACK；重启后旧结果 outbox 保留，但执行权不恢复。持久化失败不会发出成功确认。

## 平台输出

开发前缀默认 `fh/swarm/dev/dispatch`；services／services_reply／events／state 与设备 heartbeat 分开。外层保持 bid／tid／timestamp／method／data；事件带稳定 event_id、round_epoch、term、commit_index、revision。

| 类别 | 本版本用途 |
| --- | --- |
| services_reply | `_result: 0` 表示受理或请求事实已提交；失败带 `_error` |
| goal.events / accepted | 首次任务已进入提交账本 |
| mission.events / for_platform_task_plan | 已提交路线及原始任务ID |
| mission.events / for_platform_tasks_added | 新增任务事实 |
| mission.events / pause_task | 暂停并准备重分配 |
| mission.events / for_platform_skipped_tasks | 启动整轮唯一最终补执行 |
| mission.events / device_isolated | 成员隔离 |
| mission.events / for_platform_mission_finished | 统一终态摘要 |
| mission.events / COMPLETED、FAILED、CANCELLED | 同一终态的大写提示 |

不生成原来已取消的两类重分配／不可分配事件。状态由已提交事实恢复并重复投递，前端需按 event_id 去重。尚未引入平台应用层 ACK，不能承诺消息已经展示。旧平台 be_leader／begin_planing 等视图和新增 round_epoch 的消费仍须联合验收，不能据本地事件样例认定完全兼容。

终态字段包括 total_tasks／completed_tasks／unfinished_tasks、retry_performed／retry_limit、skipped_tasks、unassignable_tasks、unconfirmed_tasks，以及有 storage 时的 report_key。COMPLETED 依据导航到达，不代表挂载子任务全部成功。

## 图片与对象存储

图片元数据携带 round_epoch、revision、route_name、session_id、原始 task_ids 和内容摘要。按已提交计划检查归属，通过独立连接同步，不写入 Raft 日志。任一设备有图片副本，其他授权副本可补拉；源设备永久消失且从未复制的图片无法恢复。

报告单文件内嵌 JPEG。新 Leader 可接管生成／上传；本地上传任务保存自己的轮次和 key。S3 对象元数据记录 epoch、终态提交位置、内容摘要；HEAD 后使用 If-None-Match 或 If-Match 条件 PUT。旧版本和较少图片的同终态报告不能覆盖已有更完整的对象；不支持条件写入时明确失败，不退化为无条件覆盖。

报告 key 是 bucket 内对象名，不是公开 URL。STS 过期不刷新，鉴权失败保留本地报告，不修改任务终态。当前已验证本地 S3 替身的签名和条件请求；真实 MinIO 兼容性待验收。

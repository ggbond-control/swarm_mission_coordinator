# Swarm 多机任务协调

Swarm 在每台设备部署相同的六个 ROS 包。平台提供任务 JSON，具有固定多数派权限的 Leader 协调规划，设备保存已提交的任务副本并执行本机路线。云端部署平台、MQTT Broker 和对象存储。

## 模块与职责

| 包 | 职责 |
| --- | --- |
| swarm_protocol | JSON 校验、固定主题、摘要、原子存储和审计记录 |
| swarm_consensus | 固定成员 Raft、Leader 选举、日志复制、读屏障和实例隔离 |
| swarm_mission_planner | C++ 地图搜索、能力分配、路线协调及 Python ROS 适配 |
| swarm_execution_agent | TaskHub YAML、累计导航证据、执行授权和结果重发 |
| swarm_mqtt_transport | 平台及设备间 MQTT 转发，控制与图片使用独立连接 |
| swarm_mission_coordinator | 任务状态机、新增与重分配、平台事件、图片和报告 |

设备运行五个节点，swarm_protocol 为共享库。coordinator 内的 model.py 应用提交事实，coordinator.py 提出决策，ingress.py 处理请求，permits.py 管理执行授权，mission_evidence.py 管理报告与图片。ROS 适配集中在 node.py 和 api.py。

PC 端 swarm_dev_console 临时代替平台发布请求和订阅状态。六个板端包独立于调试台，平台和调试台使用相同的公开 MQTT 协议。测试范围和结果统一记录在 [TEST_LOG](TEST_LOG.md)。

## 构建与启动

设备使用 ROS 2 Jazzy。先加载 TaskHub、共享接口及 Bringup 所在工作区，再从协同工作区根目录构建。使用 rosdep 安装 package.xml 声明的系统依赖，包含 Paho、boto3、PyYAML、Pillow、Boost、OpenCV、yaml-cpp 和 nlohmann JSON。

```bash
source /opt/ros/jazzy/setup.bash
source ~/Workspace/task_ws/install/local_setup.bash
cd ~/Workspace/cooperation_ws
colcon build --packages-select swarm_protocol swarm_consensus \
  swarm_mission_planner swarm_execution_agent swarm_mqtt_transport \
  swarm_mission_coordinator --symlink-install
source install/local_setup.bash
```

Bringup 按 system.yaml、models/<model>/system.yaml、robot.yaml 合并配置。日常在 robot.yaml 选择协同模式。

```yaml
model: cyvet
robot_id: CYVET-A
system:
  cooperation:
    mode: swarm
```

disabled 关闭协同节点，coordination 启动半分布式协同，swarm 启动全分布式五节点。设备身份、能力、标称速度、MQTT 认证及 TLS 使用 Bringup 合并配置。配置由启动入口以 ROS 参数传入各节点。

```bash
~/Workspace/task_ws/src/inspection_bringup/scripts/manage_inspection_services.sh restart cooperation
```

首次更新服务脚本时执行 install cooperation。协同净空参数在 cooperation.swarm.footprint_radius 和 free_thresh 配置一次，同时传给地图预检和规划器。其他参数通常使用节点默认值。

Agent 默认接入本机 TaskHub，并在启动时取消旧路线以建立停止状态。测试通过 task_hub_prefix 指向隔离的模拟服务。存在重复控制节点或服务提供者时，Agent 拒绝控制。 [Bringup 使用说明](../inspection_bringup/README.md)

## 任务输入

平台向 fh/swarm/dispatch/services 发布以下请求。map_package 必须是各参与设备上实际存在、可读的地图目录绝对路径。地图预先部署，MQTT 只传路径和任务数据。

```json
{
  "bid": "mission-001",
  "tid": "request-001",
  "timestamp": 1795490000000,
  "method": "task.execute",
  "data": {
    "round_epoch": 101,
    "map_package": "/home/cat/Workspace/Maps/company2",
    "robot_ids": ["X30-A", "X30-B"],
    "tasks": [{
      "id": "point-1",
      "location": {
        "frame_id": "company2",
        "pos": [-5.1, 11.0, 0.0],
        "orientation": [0.0, 0.0, 0.0, 1.0]
      },
      "tasks": []
    }]
  }
}
```

round_epoch 是平台持久生成、按集群递增的正安全整数，同内容重发使用原值。名单确定本轮固定投票成员。名单全员通过服务、位姿、地图摘要和启动会话检查后才能激活。

tasks[].id 在一轮中唯一且区分大小写，新增点不得复用。规划、重分配、补执行、平台通知及报告保留该 ID。平台跨轮关联使用 round_epoch 和 task_id。位置为地图米坐标，frame_id 对应包内地图帧。orientation 缺省时使用 yaw 角度生成四元数。

每点 tasks 必填，空数组为纯导航。挂载操作包含 method、requirements 和 params，能力由 requirements 汇总，params 传给 TaskHub YAML。可选 service_seconds 表示规划估计耗时。storage 为可选报告存储配置。

| 方法 | data 内容 |
| --- | --- |
| task.execute | round_epoch、map_package、robot_ids、tasks，可选 storage |
| task.add | 当前 round_epoch、同一地图路径及原始名单、仅新增的 tasks |
| task.stop / task.reset | 当前 round_epoch，bid 对应当前任务 |

活动轮次不能被新任务覆盖。停止请求须经多数派提交及本机执行确认。新轮开始前，上一轮物理执行须已停止。

### TaskHub 输出

每台有执行路线的设备生成 runtime/agent/<route_name>.yaml，使用 route.name 和 route.waypoints 结构。Agent 获得提交授权后，通过 StartRoute 的 route_config_path 交给本机 TaskHub。每次重分配产生新修订及独立 YAML。

TaskHub 的数值航点号和操作号适用于本机路线。Agent 保存其到业务 task_id 的映射，共址任务可合并为同一航点，等待和转向检查点可没有业务 ID。

### 本机 ROS 入口

| 接口 | 类型与用途 |
| --- | --- |
| /swarm/mission/plan | inspection_interfaces/action/PlanMission，读取任务 JSON |
| /swarm/mission/replan | 同一 Action，文件仅包含当前轮新增点 |
| /swarm/mission/stop、reset | std_srvs/srv/Trigger，请求本机停止及全局取消 |
| /swarm/mission/state | std_msgs/msg/String，已提交任务摘要 |
| /swarm/agent/local_stop | std_srvs/srv/Trigger，请求本机停止 |

Action 的 mission_id 为任务标识，文件须包含 round_epoch。planned 表示计划及启动确认已提交。停止接口受理成功后，仍须等待执行停止确认和全局终态。

## MQTT 与共识

| 用途 | 固定主题 |
| --- | --- |
| 请求 / 应答 | fh/swarm/dispatch/services、services_reply |
| 事件 / 状态 | fh/swarm/dispatch/events、state |
| 设备心跳 | fh/swarm/device/<robot_id>/heartbeat |
| 内部广播 | fh/swarm/peer/inspection-swarm/broadcast/<sender> |
| 内部定向 | fh/swarm/peer/inspection-swarm/to/<recipient>/<sender> |
| 图片 | 内部前缀后增加 media，再使用 broadcast 或 to |

内部信封包含 protocol_version、cluster_id、robot_id、boot_id、to、kind、round_epoch 和 data。版本只在协议不兼容时变更。Broker 的认证与 ACL 应限制设备只能使用自己的 sender 身份。控制消息使用 QoS 1、非 retained 发布。

| 字段 | 含义 |
| --- | --- |
| round_epoch | 任务轮次 |
| term | 同轮持久化的选举任期 |
| sessions | 激活时绑定 Agent session_id 和 consensus_boot |
| excluded | 本轮隔离成员，只增不减，多数派分母固定 |
| revision | 任务方案修订 |
| commit_index | 已提交日志位置 |
| route_name | sw<epoch>-<mission 摘要前16位>-r<revision>-<robot_id> |

Raft 使用预投票、持久投票、日志比较和当前任期提交规则。执行许可经过当前任期的多数派读屏障，期限按 Agent 发起请求的单调时钟计算。

落后副本通过 AppendEntries 分批追赶，每批最多 32 条、目标 1 MiB，较大单条命令独立传输，受 8 MiB 信封预算限制。本机 consensus/state 提供水位，coordinator 经 consensus/fetch 和 consensus/entries 应用连续批次。追上本机提交水位后才具有业务决策权。

## 执行与故障处理

```text
PREPARING → PLANNING → PLANNED → ACTIVE → COMPLETED / FAILED
                         ↑         ↓
                         └─ PAUSING ┘
活动阶段 → STOPPING → CANCELLED
```

- Leader 掉线且仍有固定多数派时，其余副本选出新 Leader 并接续提交状态。失去多数派后无法提交新计划、续发执行许可或确认全局终态。
- 进程重启产生新实例，旧轮执行及投票资格隔离。未确认停止的失联任务保留 UNKNOWN，已确认到达点不重新分配。
- 新增或掉线重分配先收集停止及导航证据，再规划未完成和跳过的点。全队有效路线结束后，剩余跳点整轮仅补执行一次。
- 再次跳过、能力不足或结果未知明确结束为 FAILED。授权过期后暂停旧路线，恢复通信并提交停止事实后明确失败。
- starting 写入磁盘后才调用 TaskHub。旧世代的迟到启动成功触发补偿取消。调用超时须先排空在途服务，再处理替代路线。

软件执行许可无法覆盖设备断电、进程卡死等硬件急停需求。多设备跨图目前受 ROOT 空间协调限制，规划器会拒绝不支持的情况。

### 累计导航结果

TaskHub 通过 /task_hub/navigation_results 的标准 String JSON 提供结果，现有 StartRoute、GetStatus、Trigger 暂停和取消服务继续使用。

```json
{
  "protocol_version": 1,
  "route_name": "sw101-摘要-r1-X30-A",
  "execution_id": "本次路线启动的唯一值",
  "snapshot_sequence": 2,
  "route_state": "COMPLETED",
  "finalized": true,
  "reached_waypoint_ids": [1],
  "skipped_waypoints": [{"waypoint_id": 2, "reason": "ABORTED timeout"}],
  "pending_waypoint_ids": []
}
```

三类清单互斥并覆盖全部航点，同次执行序号递增，到达记录不能倒退。封存后不可变。Agent 使用映射转换为业务 ID，通过持久化 outbox 重发至多数派提交 ACK。终态后最多核对十秒，缺失结果保留 UNKNOWN。导航到达是完成判据，挂载操作结果不参与补执行判定。

## 平台输出

外层包含 bid、tid、timestamp、method 和 data。事件带稳定 event_id、round_epoch、term、commit_index 和 plan_revision，平台按 event_id 去重。

| 事件 | 内容 |
| --- | --- |
| services_reply | _result 为 0 表示已受理，失败包含 _error |
| goal.events / accepted | 首发请求已提交 |
| mission.events / for_platform_task_plan | 路线及原始 task_ids |
| mission.events / for_platform_tasks_added | 新增 task_ids |
| mission.events / pause_task | 暂停并准备重分配 |
| mission.events / for_platform_skipped_tasks | 整轮最终补执行的 skipped_tasks[].task_id |
| mission.events / device_isolated | 设备隔离 |
| mission.events / for_platform_mission_finished | 统一终态摘要 |
| mission.events / COMPLETED、FAILED、CANCELLED | 配套终态事件 |

状态包含地图路径、到达计数、completed_task_ids、跳点及结果未知清单。设备心跳包含位姿、能力、TaskHub 状态、共识角色、任期及提交水位。终态有 storage 时包含 report_key。完成点数依据导航到达，不代表挂载操作全部成功。

Leader 每两秒发送常规心跳和任务状态。新提交事件增量发送，接管时以及每 30 秒重发已有事件以补偿丢失，保持原 event_id。for_platform_reallocation_tasks 和 for_platform_unassignable_tasks 已取消。MQTT 入队或 PUBACK 不代表平台已经显示。

## 图片与报告

storage 包含 bucket、endpoint、objectKeyPrefix 和 stsCredentials。终态返回 <objectKeyPrefix>/<bid>.html，本地 HTML 生成和上传均在后台。只有当前具有多数派权限的 Leader 可上传，最多尝试四次，间隔 5、15、30 秒。

图片按轮次、修订、路线、会话及任务映射归属，HTML 内嵌 JPEG。coordinator.media_roots 指定本机可读取目录。单张图片最多 512 KiB，每轮最多 200 张。设备间通过原有独立图片连接补同步，空图片清单不发送。源设备从未复制的图片在永久离线后无法恢复。

最近八轮可接收迟到图片并更新原 key。上传使用版本元数据及条件 PUT，旧版本或图片较少的同终态报告不能覆盖更完整对象。凭据只保存在 runtime/coordinator/private_uploads。STS 不自动刷新，上传失败保留本地报告和任务结论。

平台和调试台凭存储权限读取 report_key 对应对象。调试台使用独立只读账号，通过 MinIO 下载 HTML。 [调试台说明](../swarm_dev_console/README.md)

## 运行资料与迁移

Bringup 通过 cooperation.swarm.data_root 配置单个板端运行目录，默认位于 ~/Workspace/cooperation_ws/src/swarm_mission_coordinator/runtime。独立节点默认位于 ~/.ros/swarm/<robot_id>/runtime，相邻 reports 存放 HTML。

| 路径 | 内容 |
| --- | --- |
| coordinator/ | 请求、提交审计、规划、图片、历史模型及私有上传任务 |
| agent/ | TaskHub YAML、航点映射、结果 outbox 和执行记录 |
| consensus/ | 任期、投票和复制日志 |
| transport/ | 连接及发送摘要 |
| ros_logs/ | ROS 节点输出与 launch.log |

结构化审计按轮次、修订分段。队列溢出或磁盘写入失败会打印丢失计数。资料由维护者手工归档。

```bash
ros2 run swarm_mission_coordinator swarm_collect_evidence \
  ~/Workspace/cooperation_ws/src/swarm_mission_coordinator/runtime \
  /tmp/swarm-evidence.tar.gz --round 101
```

导出包包含文件摘要、时间区间、地图摘要及缺失项，排除凭据和活动恢复文件。各设备分别导出同一轮次，可在调试台合并查看。

旧版分散目录先停任务及协同进程，再运行迁移预检。参数仅填写实际存在的源目录。

```bash
ros2 run swarm_mission_coordinator swarm_migrate_runtime \
  --runtime swarm_mission_coordinator/runtime \
  --coordinator swarm_mission_coordinator/runtime \
  --agent swarm_execution_agent/runtime \
  --consensus swarm_consensus/runtime \
  --transport swarm_mqtt_transport/runtime
```

确认后为同一命令增加 --apply。迁移逐文件校验并生成 migration.json，目标路径重合时保留 runtime-before-*。回滚须恢复匹配版本及状态，旧任期不得覆盖升级后已推进的任期。

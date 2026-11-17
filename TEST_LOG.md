# Swarm 本地验证记录

本文件记录测试范围、结果与复现方法；使用说明放在 [README](README.md)，协议放在 [PROTOCOL](PROTOCOL.md)。各包不重复维护跨包计数。下述 runtime 文件为本机证据，默认不进入 Git。

## 2026-11-17

### 固定主题与配置精简

固定默认值统一维护在 `swarm_protocol/topics.py`。平台使用全新
`fh/swarm/dispatch/{services,services_reply,events,state}`；设备心跳为
`fh/swarm/device/<robot_id>/heartbeat`；内部使用 `fh/swarm/peer/inspection-swarm/...`。
不复用 fleet 主题，默认主题和集群标识不带日期／版本号；内部消息协议版本校验保留。

Bringup 日常配置仅需 `cooperation.mode: swarm`，复用已有 MQTT 认证、TLS、设备身份、
能力和速度。删除 `allow_insecure_remote` 运行参数及远程明文连接额外许可检查，TLS
仍校验证书。`cooperation.swarm` 保留为可选高级覆盖，默认不填写。
disabled／coordination 不导入 swarm 运行依赖；未改导航、TaskHub、传感器或单机窗口逻辑。

### 本地结果

证据统一位于 `runtime/validation/fixed-topics-20261117/`，以下是本轮独立结果，不与历史复测累加。

| 范围 | 结果 | 证据 |
| --- | --- | --- |
| 工作区根目录构建 | 7 包通过，3.69 秒 | `build.log` |
| 六包单元回归 | 108 通过、10 跳过，9.67 秒 | `unit.log` |
| Bringup 定向配置／模式／窗口检查 | 16 通过、75 未选，2.04 秒 | `bringup.log` |
| 隔离真实 ROS／MQTT 及 C++ TaskHub／模拟导航 | 10 通过，139.67 秒 | `integration.log` |
| Bringup 完整配置检查 | 91 通过、10 失败，7.18 秒 | `bringup-full.log` |

Bringup 的 10 项失败与 11 月 16 日修改前 HEAD 已复现项相同，涉及型号参数、导航配置与
路径归属；本次没有修改这些配置。新增测试验证固定主题、无额外明文许可、两条 MQTT
连接的 TLS 证书校验，以及不向旧 fleet 主题发布；完整配置套件尚未全绿。

### 双开发板部署与无运动闭环

CYVET-A／B（121.41.75.74:20006／20005）各完成四包根目录构建与 system 重启。
六个运行文件与本地 SHA256 一致；`robot.yaml` 只移除旧 swarm 日期／主题覆盖和额外
明文许可，最终为 `cooperation: {mode: swarm}`。导航、服务、其他单机配置与备份一致。
两板 system 服务均为 active；观察到新主题心跳及内部广播，未观察到两板旧 fleet 心跳。
证据：`CYVET-*-deploy.log`、`boards-observation.json`、`CYVET-*-final-check.log`。

本次两板没有新鲜导航位姿，因此未向真实 CYVET 身份提交路线，也未注入定位。
在各板独立 ROS 域 191／192 启动模拟 TaskHub 和模拟身份 SWARM-VERIFY-A／B，
通过实际 MQTT Broker 的固定新主题完成空任务闭环：

- 任务 `fixed-topics-empty-1794879665446`，round_epoch `1794879665446`。
- 结果 COMPLETED 0/0；Leader 为 SWARM-VERIFY-B、term 1，两副本 commit_index 均为 5。
- 两个模拟 TaskHub 的路线启动次数均为 0、route 均为 null；未调用真实运动服务。
- 已退出隔离测试进程、删除含认证信息的临时配置；实际设备服务继续运行。

任务输入、MQTT 终态、两板状态副本和模拟审计分别见 `board-round-input.json`、
`board-round-result.json`、`board-round-mqtt.jsonl`、`CYVET-*-mock-result.json`。
板端备份为 `/home/cat/.ros/swarm-backups/fixed-topics-20261117/`（0700）。
本轮板端修改文件仅为：

- `swarm_protocol/swarm_protocol/topics.py`（新增）。
- `swarm_mqtt_transport/swarm_mqtt_transport/node.py`。
- `swarm_mission_coordinator/swarm_mission_coordinator/{node,launching,demo}.py`。
- `inspection_bringup/scripts/cooperation.py`、`config/robot.yaml`。

本次验证了新主题通信、双板共识与空任务收尾；有任务点的规划执行沿用本地隔离集成验证，
不将本次板端空任务算作真实导航验收。平台前后端尚未按新主题联调，需接入上述固定主题。
未提交或暂存 Git。

## 2026-11-16

### 本地环境与隔离

ROS 2 Jazzy／Python 3.12，本机 loopback Mosquitto 与 S3 HTTP 替身。ROS／MQTT 集成使用 181–183 三个独立 ROS 域和 18 个真实进程；C++ TaskHub 对接使用 187 域和模拟 NavigateToPose。本小节的隔离测试没有连接开发板、生产 Broker、生产 MinIO，没有启动真实运动；下方单独记录后续双机实测。所有构建位于工作区根目录 build／install／log。

### 覆盖

| 范围 | 验证内容 |
| --- | --- |
| 协议与存储 | 输入校验、大小写、重复 JSON、有限数值、凭据隔离与原子文件 |
| 共识 | 1–5 成员、固定多数派、旧 ACK／旧实例拒绝、日志冲突、快照追赶、磁盘异常、重启隔离；5 个随机种子的丢包／重排／重复／分区模拟 |
| 规划 | 栅格、净空、地图旋转与切换点、非法起点、能力、操作保留、串行依赖、冲突拒绝 |
| 执行 | 同位置任务、等待路线、累计证据、结果乱序、封存、UNKNOWN、授权过期、迟到启动补偿、outbox 重启、暂停后取消 |
| 业务 | 首轮、新增、掉线隔离、Leader 接管、整轮一次补执行、失败／取消、连续轮次、初始受理期间取消 |
| 报告和图片 | 自包含 HTML、后台上传、重试与恢复、条件写入、较少图片不覆盖较多图片；独立媒体同步、迟到旧图补旧报告而不污染新轮 |
| 原系统 | 原 Server／Agent 源码未修改，完整回归 706 项通过 |

### 结果与证据

- 六个新包单元回归：**98 项通过**。默认跳过需要显式启用的 ROS 集成；其结果单独列出。证据：`runtime/validation/20261116-unit-delivery2.{log,xml}`。
- 三设备真实 ROS／MQTT 七场景：**7 项通过，115.12 秒**。证据：`20261116-ros-delivery.{log,xml}` 和 `ros-20261116-delivery/`，包含“Action 初始受理期间取消”。
- 真实 C++ TaskHub 与本地模拟导航：**3 项通过，28.48 秒**，分别为全部到达、首轮跳点后补执行成功、补执行仍跳点后 FAILED。证据：`20261116-native-final.{log,xml}`。
- 原 Server／Agent：**706 项通过，398.28 秒**。证据：`20261116-legacy-regression.{log,xml}`。
- Bringup 本次新增检查：**2 项通过**。证据：`20261116-bringup.log`。现有配置套件的不相关失败见本日下方“统一配置及 CYVET 双机实测”记录。
- 安全 launch 冒烟：五个新节点成功启动并退出，未启动导航或 TaskHub；证据 `launch-smoke-final/launch.log`。
- TaskHub 原累计导航结果测试：**11 项通过**，证据 `20261116-taskhub-unit.log`。

SDK 有 datetime 弃用警告；ROS 测试环境保留了已弃用但仍生效的 ROS_LOCALHOST_ONLY，均非测试失败。最终构建（7 包成功）和各项结果集中见 `runtime/validation/DELIVERY.md`。

### 本轮测试发现并修正

- rclpy 的只读 `Node.clients` 属性与 Agent 成员重名，改为独立成员。
- Action 包装须保留异步回调，不能用返回协程的普通 lambda 阻塞结果。
- TaskHub 已暂停路线不能被新 StartRoute 直接替换，先取消并核对累计证据。
- 断网恢复时，OFFLINE 处理可能早于“授权过期已停止”的证据；停止记录增加明确 cause，在普通重分配前拒绝继续执行。修复后断网恢复不重新启动旧轮。
- 受理期间 stop 必须绑定正在准入的新轮，不能绑定旧终态或在 MISSION 尚未提交时被丢弃。
- 迟到图片使用已归档的旧路线及历史模型验证，补充原轮报告；当前轮不接收旧图。
- TaskHub 控制调用超时保留在途调用，围栏失效并等待补偿取消，不并发发出替代调用。

失败测试证据保留，后续通过记录不覆盖失败目录。首次 launch 夹具向整个进程组发信号、同时 launch 又转发 SIGINT，造成重复中断清理；更正为只通知 launch supervisor 后复测。早期 native 测试夹具传入错误的 DiagnosticStatus level 类型引发 C 断言，修正为 ROS 消息常量；不是 TaskHub 业务缺陷。

### 复现

先按 README 从工作区根目录构建，再在 src 执行：

```bash
source /opt/ros/jazzy/setup.bash
source ../install/setup.bash
python3 -m pytest -q swarm_protocol/test swarm_consensus/test \
  swarm_mission_planner/test swarm_execution_agent/test \
  swarm_mqtt_transport/test swarm_mission_coordinator/test
```

真实 ROS 集成需显式选择并使用已安装的本机 mosquitto：

```bash
SWARM_RUN_ROS=1 SWARM_BROKER=/usr/sbin/mosquitto \
  python3 -m pytest -q swarm_mission_coordinator/test/test_ros_mqtt.py
SWARM_RUN_ROS=1 python3 -m pytest -q \
  swarm_mission_coordinator/test/test_native_taskhub.py
```

若指定 `SWARM_TEST_EVIDENCE`，每次使用新的空目录；测试自行管理 loopback Broker 和子进程。集成测试使用固定隔离域，请避免在同域运行其他节点。

### 尚未验收

真实平台字段与前端流程兼容、复杂实际地图规划性能、多机复杂物理运动、5G 丢包／时钟／负载、真实 MinIO 条件写入和 ACL、长时日志增长及磁盘上限尚未验收。自建 Raft 未经形式化验证；本地通过不等于生产可替换。源设备未曾复制就永久丢失的图片无法恢复。

### 提交前结构复核

本次按包精简 `.gitignore` 和 setup：保留 Python／pytest／setuptools 缓存规则，只有协调器另忽略源码 `/runtime/` 集成证据；各包不再统一忽略不存在的 `reports/`。移除五个包对空 launch／config 目录的安装规则。默认独立启动数据目录按 robot_id 隔离，与统一入口一致，真实报告只在协调器数据根目录下生成。

运行代码统一展开压缩写法、排序导入并删除未使用的导入／变量／参数；`MissionState` 仍是业务状态的唯一来源，没有另建状态副本。`storage_upload.py` 独立处理条件上传，报告渲染和后台任务仍归 `reporting.py`。

规划器复核确认它是简化独立实现，并非旧 C++ 算法的等价移植。新增 spawn 计算进程（默认 45 秒），超时回收后再受理；新请求可以替代过期计算，重复请求不重复计算。搜索预算耗尽保留资源错误，不混同物理不可达；自由阈值等值可通行。尚未实现 ROOT 空间多机跨图检查，涉及该情况明确拒绝输出执行计划。具体功能差异和后续板端性能验收见规划器 README／TEST_LOG。

最终本地验证：

- 单元回归 **105 通过、10 跳过**；跳过项在下面独立集成套件运行。证据 `20261116-review-unit-final.{log,xml}`。
- 三设备真实 ROS／MQTT **7 通过，116.01 秒**；证据 `20261116-review-ros.{log,xml}` 与 `ros-20261116-review/`。
- C++ TaskHub／模拟导航 **3 通过，30.11 秒**；证据 `20261116-review-native.{log,xml}`。
- 本地合成规划基准每种 2 次，128／256／512 格、5 设备、20 点均完成。**不是开发板性能验收**，详见规划器 TEST_LOG 和 `review-20261116/benchmark.json`。
- Ruff 的导入／未使用变量／未定义名称检查及格式检查通过；Python 编译和 Markdown 链接检查通过。构建结果见 `20261116-review-build-final.log`。

以上结构复核阶段未连接开发板、生产 MQTT／MinIO，未提交或重新暂存 Git。旧协同源码不修改；旧包的 706 项回归沿用同日记录，不重复计入本次新系列测试数量。

### 统一配置及 CYVET 双机实测

设备入口统一为 Bringup `cooperation.mode: disabled | coordination | swarm`，不再维护设备专用 swarm YAML、旧 Agent／选举配置开关、协同 launch 模式参数或 `allow_motion`。原四个协同包继续用于 coordination，不与 swarm 同时启动。详细配置见 README；Bringup 检查结果在本节下方集中记录。

本轮重新验证：七包根目录构建通过；六包单元回归 **105 通过、10 跳过**；隔离真实 ROS／MQTT 七场景及 C++ TaskHub 三场景合计 **10 通过，140.72 秒**；Bringup 定向配置 **9 通过**、构建与服务清理脚本 **62 通过**。Bringup 完整配置检查 **77 通过／10 失败**，修改前 HEAD 复现相同 10 项失败。上述结果与前面同日记录是复测，不叠加计数。证据在 `runtime/validation/boards-20261116/`。

实机为 CYVET-A／B（121.41.75.74:20006／20005），ROS 2 Jazzy。保留现有导航，使用 company2 地图，两狗初始相距约 10m；通过独立 MQTT 主题 `fh/swarm/lab/20261116`、平台主题 `fh/swarm/lab/20261116/dispatch` 发送测试请求，没有向生产平台任务入口发任务。

| 板端检查 | 结果 |
| --- | --- |
| disabled | 两板 system 服务正常，协同进程均为 0 |
| coordination | 两板各运行原选举、planner、Agent 三节点；swarm 为 0 |
| swarm | 两板各运行 consensus、coordinator、transport、planner、Agent 五节点 |
| 空任务轮 `cyvet-empty-20261116` | 16:07:27–28（北京时间）结束，COMPLETED 0/0；CYVET-A 为 Leader |
| 纯导航轮 `cyvet-live-20261116` | 16:08:41 受理，16:08:49 结束，COMPLETED 2/2；CYVET-B 为 Leader，term 2 |
| 状态复制 | 两板最终 commit_index 均为 16，任务状态一致；Agent finalized |
| TaskHub 证据 | 各一原始任务点，均导航到达，封存 COMPLETED；无 skipped／pending，无需补执行 |

纯导航目标取各自实时位姿 x+0.5m，并先对实际地图规划预检；导航以自身到达容差判定，不将目标位移等同于精确实测移动距离。没有在本轮实机运行中注入 Leader 掉线；两轮不同 Leader 不能当作运行中接管验收。

报告位于 CYVET-B：`/home/cat/.ros/swarm/CYVET-B/coordinator/reports/round-1794816513544/cyvet-live-20261116.html`。本地副本、平台终态 JSON、两板共识日志和 TaskHub 结果在上述证据目录。任务 epoch 分别为 `1794816438862`、`1794816513544`。

两板备份：`/home/cat/.ros/swarm-backups/modes-20261116/`（0700）。最终保留 `mode: swarm` 和独立 lab 主题；测试路线均已结束，没有修改生产 Server。板端更改文件如下（测试文档只在本地维护）：

- Bringup：`launch/inspection_system.launch.py`、`scripts/bringup_config.py`、新增 `scripts/cooperation.py`、`scripts/start_inspection_tmux.sh`、`config/system.yaml`、`config/robot.yaml`、`config/models/cyvet/system.yaml`。
- Coordinator：`setup.py`、新增 `swarm_mission_coordinator/launching.py`、`swarm_mission_coordinator/demo.py`。
- Agent：`swarm_execution_agent/node.py`。
- 删除旧独立配置／入口：Bringup `config/swarm/cyvet-a.yaml`／`cyvet-b.yaml` 及存在的 `device.example.yaml`；Coordinator `config/device.example.yaml`、`launch/swarm_device.launch.py`。

TaskHub 对未安装传感器的停止清理仍存在服务不可用警告，本轮未修改其源码；两条纯导航路线均取得封存成功证据。不将本次验收扩展为平台前端兼容、挂载算法、实机故障注入、复杂地图性能或长期稳定性验收。

### tmux 单机演示窗口兼容修正

保留原来 TaskHub、云台、充电、各传感器的独立窗口；平台与算法通信继续共用 platform 窗口。协同节点独占 cooperation 窗口，disabled 时没有该窗口，coordination／swarm 分别只启动一套三／五节点，不重复启动。模式仍来自配置，未新增模式或执行开关。

实现通过 `inspection_modules.launch.py` 复用完整系统 launch 的单机模块构造，通过 `cooperation.launch.py` 复用协同节点构造。原 `inspection_system.launch.py` 默认仍启动完整系统，systemd 用法和非协同模块的配置、参数、传感器 launch 均保留。

- 本地 Bringup 根目录构建通过；tmux／模式定向 **8 项通过**。测试检查三种 mode 的窗口、每个窗口真实 launch action 的节点／include／condition、协同只启动一次、无单机模块时仅启动协同，以及原完整入口继续带上正确协同栈。
- 本地完整 config／launch **80 通过／10 失败**；失败仍为前述相同的 10 个既有配置断言，不新增回归失败。
- CYVET-A／B 部署后均构建通过；各验证三种 mode 的真实启动 action，单机模块窗口没有协同节点。
- 两板各在隔离 ROS 域 191／192 启动真实 tmux 会话，coordination 各三节点、swarm 各五节点均正常运行并退出。仅启用协同，MQTT 指向本机未开放端口，不连接真实 TaskHub／导航或生产编队，不下发运动。测试客户端产生的服务名不能证明服务端存在，夹具改为逐节点核对真实服务端，以确认没有 TaskHub 服务提供者。
- 测试会话、节点和临时配置已清理，现有 inspection-system.service 始终 active，robot.yaml 未改动。当前两板仍运行原独立 lab 主题下的 swarm 完成状态。本次没有重复真实运动测试；上方双机 2/2 记录属于此前测试。

本次板端仅更新以下四个 Bringup 运行文件：

1. `scripts/start_inspection_tmux.sh`：恢复单机分窗，协同单独启动。
2. `launch/inspection_system.launch.py`：抽出共用协同节点构造，默认完整入口保持原行为。
3. 新增 `launch/inspection_modules.launch.py`：供单机模块窗口复用原逻辑。
4. 新增 `launch/cooperation.launch.py`：仅启动配置选定的协同栈。

本地另更新测试和使用文档。板上备份位于 `/home/cat/.ros/swarm-backups/tmux-windows-20261116/`，证据在 `runtime/validation/boards-20261116/tmux-*.log`、`CYVET-*-tmux-*.log`；最终文件哈希在 `tmux-final-check.log`。本次没有 stage／commit，保留用户已有暂存区。

### 比赛单机代码改动进一步收敛

按比赛代码稳定性要求，恢复 tmux 脚本内部 `system` 分类名称，保持原有各单机模块窗口和参数生成循环；单机窗口仍通过新增的薄入口排除重复协同启动。协同节点构造全部移入本轮新增 `inspection_bringup/scripts/cooperation.py`，`cooperation.launch.py` 直接调用，不再加载整个单机系统文件；完整系统入口只保留必要协同接入调用及前述 mode 配置对接。不修改 TaskHub、导航、传感器、云台或充电源码，不恢复旧协同配置兼容路径。

与 Bringup HEAD `1812c78` 对比 Python AST：**13 个原有辅助函数、28 项单机启动赋值及单机 action 构造一致**；包括 TaskHub 参数和节点、算法 MQTT／HTTP 参数和节点、原模块开关、传感器／云台／充电接入。平台 MQTT 参数中仅 `robot_heartbeat_topic`、`enable_mission_execution_agent`、`fleet_state_topic` 三项协同转发字段改变；不把它们描述为完全未修改通信入口。比对证据为 `minimal-hooks-ast-check.json`。

本地根目录 Bringup 构建通过、模式／tmux 定向 **8 项通过**。两板同步四个本轮改动的运行文件（`scripts/cooperation.py`、`scripts/start_inspection_tmux.sh`、`launch/inspection_system.launch.py`、`launch/cooperation.launch.py`），保留 `robot.yaml` 和现有服务。两板各重跑三种 mode 启动图检查，以及隔离 ROS 域下真实 tmux 的 coordination 三节点／swarm 五节点启动退出；未接入真实 TaskHub 或导航，未发送运动任务。

备份目录：`/home/cat/.ros/swarm-backups/minimal-hooks-20261116/`。结果见 `minimal-hooks-*.log`、`CYVET-*-minimal-hooks.log`。本轮仅做上述定向复测，前述全套 Bringup 的既有 10 项失败没有宣称修复。用户已有暂存区保持原样，没有 stage／commit。

## 2026-11-13

形成部门评审方案及早期本地原型／测试资料；历史原型证据保存在 runtime。以 11 月 16 日已核验实现和记录为准，不将旧运行截图当作当前验收结果。

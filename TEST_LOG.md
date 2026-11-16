# Swarm 本地验证记录

本文件记录测试范围、结果与复现方法；使用说明放在 [README](README.md)，协议放在 [PROTOCOL](PROTOCOL.md)。各包不重复维护跨包计数。下述 runtime 文件为本机证据，默认不进入 Git。

## 2026-11-16

### 环境与隔离

ROS 2 Jazzy／Python 3.12，本机 loopback Mosquitto 与 S3 HTTP 替身。ROS／MQTT 集成使用 181–183 三个独立 ROS 域和 18 个真实进程；C++ TaskHub 对接使用 187 域和模拟 NavigateToPose。没有连接开发板、生产 Broker、生产 MinIO，没有启动真实运动。所有构建位于工作区根目录 build／install／log。

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
- Bringup 本次新增检查：**2 项通过**。证据：`20261116-bringup.log`。现有配置套件的不相关失败见 [Bringup TEST_LOG](../inspection_bringup/TEST_LOG.md)。
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

真实平台字段与前端流程兼容、实际地图规划性能、多机物理运动、5G 丢包／时钟／负载、真实 MinIO 条件写入和 ACL、长时日志增长及磁盘上限尚未验收。自建 Raft 未经形式化验证；本地通过不等于生产可替换。源设备未曾复制就永久丢失的图片无法恢复。

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

未连接开发板、生产 MQTT／MinIO，未提交或重新暂存 Git。旧协同源码不修改；旧包的 706 项回归沿用同日记录，不重复计入本次新系列测试数量。

## 2026-11-13

形成部门评审方案及早期本地原型／测试资料；历史原型证据保存在 runtime。以 11 月 16 日已核验实现和记录为准，不将旧运行截图当作当前验收结果。

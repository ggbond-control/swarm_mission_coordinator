# Swarm 测试记录

测试结果按日期倒序记录，同日最终结果覆盖中间复测计数。运行说明及协议见 [README](README.md)。runtime 下保存代表性证据，文件由本机维护，不进入 Git。

## 2026-11-24

本次审查覆盖七个 Swarm 包的职责、源文件引用、测试归属、平台 MQTT 流量、文档和运行资料。

| 范围 | 验证内容 |
| --- | --- |
| 协议与共识 | 严格 JSON、绝对地图路径、存储配置、固定多数派、重复及旧 ACK、实例隔离、落后日志追赶 |
| 规划器 | 地图和起点诊断、能力匹配、通道边界、多机跨图保护、工作进程超时回收 |
| Agent | 共址点映射、封存结果、未知结果、授权到期、迟到启动补偿、重启 outbox |
| 协调器 | 首发、新增、掉线重分配、保底补执行、停止确认、连续轮次、事件稳定 ID |
| 报告与资料 | MinIO 条件写入、重试、旧版本隔离、图片、凭据脱敏、运行目录迁移与证据校验 |
| 调试台 | 按预览原样发布 data、显式生成轮次、接收及查询不发布控制、断开后设备自主完成、浏览器布局和回放 |

代码整理将 Agent 独立测试移回 Agent 包，将输入校验移到 protocol 包，报告缓存测试与调试台 HTTP/MQTT 联调分开。保留真实 ROS 集成和真实 C++ TaskHub 配模拟导航的覆盖。

通信审查将 Leader 事件改为增量发送，接管及每 30 秒补发历史。状态与设备心跳继续使用现有公开主题。图片同步跳过空清单及本轮重复归档。调试台仅在用户确认发布时使用 services 主题。

本轮完整回归 148 项通过，无跳过，包含 11 项隔离 ROS/MQTT 集成；耗时 209.45 秒。板端六包及 inspection_bringup 共七包构建通过。测试使用本机 Broker、模拟位姿及导航，不连接开发板或生产服务。18 条警告来自 botocore 现有 datetime 弃用提示。最终日志、JUnit 结果和 ROS 证据位于 runtime/validation/review-20261124；调试台浏览器证据保存在其自身 runtime 下。

室内 company2 七点样例已通过四节点模拟任务与 TaskHub YAML 映射校验，记录保存在 runtime/validation/company2-map-path-20261124-QgR55R。

## 2026-11-23

C++ 规划核心迁入 swarm_mission_planner，六包单元及隔离 ROS 回归通过，PC 调试台完成 HTTP/MQTT 和浏览器验证。

六台设备的 30 点、100 点合成规划各运行 20 次，任务归属和顺序与原 C++ CLI 一致。端到端 P95 分别约 1.06 秒和 2.95 秒，数据保存在 runtime/validation/planner-benchmark-20261123。

六副本、20 次修订、每次 100 点的复制测试，将共识状态消息由约 9.1 MB 缩为 249 B。落后节点分 10 条消息追平，最大约 907 KB。对照数据保存在 runtime/validation/reporting-20261123。

本机真实 MinIO 验证条件创建与更新、旧版本拒绝覆盖、Leader 失权延后上传、只读 STS 下载和禁止写入。隔离三节点 Leader 接管后完成终态提交与报告上传。代表性报告及结果摘要保存在同一目录。

## 2026-11-19

设备描述统一到 Bringup 三层 YAML，Bridge、TaskHub 和协同使用同一快照。Swarm 回归 110 项通过，隔离 ROS/MQTT 7 项及真实 C++ TaskHub 配模拟导航 3 项通过。半分布式完整回归 706 项通过。

Bringup 全量仍有 10 项既有失败，Bridge 有 3 项既有失败，均在修改前版本复现。TaskHub 有 1 组测试因本机缺少 inspection_charge_executor 未运行。设备描述迁移证据归档在 runtime/archives/device-description-20261119.tar.gz。

## 2026-11-18

拆分 task_ws 与 cooperation_ws，增加独立协同服务。迁移保留设备身份、任期、投票及执行资料，停止服务后执行。 [Bringup 说明](../inspection_bringup/README.md)

## 2026-11-17

固定 Swarm 平台、心跳和内部主题，不含日期或版本。配置复用现有身份、MQTT 认证与 TLS。disabled、coordination 和 swarm 三种模式分别验证。

CYVET-A/B 观察到新主题和服务正常运行，在独立 ROS 域使用模拟身份完成空任务，未启动真实运动。本地 108 项单元和 10 项集成通过。证据归档在 runtime/archives/fixed-topics-20261117.tar.gz。

## 2026-11-16

完成固定多数派、隔离、导航证据、重分配、保底补执行、报告和图片的早期隔离验证。后续 CYVET-A/B 各执行一个近距离纯导航点，结果封存并完成整轮 2/2。实机未注入掉线、导航失败或重启。板端历史证据归档在 runtime/archives/boards-20261116.tar.gz。

## 复现完整回归

先按 README 从工作区根目录构建并加载 install/setup.bash。安装 python3-pytest、python3-paho-mqtt、python3-boto3 和 mosquitto 后，在 src 下执行：

```bash
export PYTHONPATH="$PWD/swarm_dev_console:${PYTHONPATH:-}"
export ROS_AUTOMATIC_DISCOVERY_RANGE=LOCALHOST ROS_LOCALHOST_ONLY=1
export SWARM_RUN_ROS=1 PYTHONDONTWRITEBYTECODE=1
python3 -m pytest -p no:cacheprovider -q \
  swarm_protocol/test swarm_consensus/test swarm_execution_agent/test \
  swarm_mission_coordinator/test swarm_mission_planner/test \
  swarm_mqtt_transport/test swarm_dev_console/test \
  inspection_bringup/test/cooperation_workspace_test.py \
  inspection_bringup/test/config_test.py::Configuration::test_swarm_parameters_share_identity_mqtt_and_device_capabilities
```

ROS 集成使用独立域、本机 Broker 和临时数据目录；缺少依赖时会跳过，应核对最终计数。本轮无跳过。Firefox 检查脚本及截图见调试台 runtime/validation/review-20261124。

## 运行资料留存

当前保留最新完整回归、室内七点成功样例、规划基准和报告摘要。历史实机及设备描述资料经 SHA-256 校验后压缩。重复运行目录、失败的测试脚本迭代、下载工具及浏览器缓存已清理。

清理前的详细测试流水保存在 runtime/archives/test-log-before-review-20261124.md，目录清单见 runtime/retention.json。原测试范围中的生产平台、真实 5G 链路、复杂实机多机运动和长期磁盘增长仍需现场验收。

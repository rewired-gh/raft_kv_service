# raft-kv-service

[English](README.en.md) | 简体中文

一个以“把 Raft 真正学明白”为目标、用 Rust 从零实现的分布式持久化 KV 微服务。
它的产品范围刻意保持很小：只提供线性一致的 Create、Read、Update、Delete；但在共识、
持久化、故障恢复、形式化验证、测试和运维方面，尽量做到完整、严谨且可复现。

这个仓库主要面向两类开发者：

- 想系统学习 Raft、复制状态机、快照、成员变更和工程化验证的人；
- 想快速启动真实多节点服务，亲手验证选举、故障转移、恢复和线性一致性的人。

> 这是一个玩具项目，但不是一份玩具实现。它不追求成为通用数据库，而是用尽可能清晰
> 的边界，展示一个分布式系统如何从规范、模型一路走到可运行的软件和证据。

## 项目背景

这个项目带有一点特殊的 hackathon 色彩：从需求讨论、规格设计、Raft 与存储实现，
到形式化模型、模糊测试、容器和文档，几乎全部工作都在少于 24 小时内完成。

更特别的是，整个过程几乎全程由作者通过手机远程控制电脑上的 Codex 推进。作者负责
提出目标、审视取舍和持续反馈，Codex 负责在真实工作区中设计、编码、运行验证、发现
反例并迭代修正。它既是一个 Raft 学习项目，也是一次关于“人如何通过移动端与编码
智能体高强度协作”的实验。

“短时间完成”并不代表省略正确性工作。仓库保留了规范、ADR、模型、测试分层和机器
可读的验证证据；所有验证声明也明确写出适用范围，不把有限状态模型检查描述成对全部
Rust 执行的数学证明。

## 与众不同的地方

- **不使用现成 Raft 库**：选举、日志复制、持久化屏障、快照和联合共识均自行实现。
- **规范先行且证据闭环**：已接受的 1.0 规范与 M0–M7 需求，通过 `verification/`
  映射到代码、模型、测试和证据；另有测试防止清单悄悄失效。
- **有限但诚实的形式化验证**：TLA+/TLC 覆盖选举、日志、CRUD、快照、成员变更与公平
  活性实例；Apalache 检查归纳不变量的初始化、保持和安全蕴含义务。
- **模型与实现之间有可检查的桥梁**：确定性 Raft 核心不依赖 socket、磁盘、线程或
  墙上时钟；协议 trace 检查持久化顺序、消息因果和快照边界。
- **真实进程故障模糊测试**：自动运行 3/5 节点、并发 CRUD、分区、延迟、丢包、重复、
  崩溃、重启和快照传输中断，再用精确线性一致性检查器验证历史。
- **可精确重放输入**：campaign schema 2 保存工作负载和类型化故障/进程调度，同时
  明确不声称能够决定性复现 OS 线程与网络调度。
- **持久化语义属于协议本身**：SQLite 事务、hard state/log 单调性、原子应用、请求
  去重、快照发布和进程独占都有契约与崩溃测试。
- **生产形态但保持最小化**：独立 client/operational/peer 监听器、TLS/mTLS、容量
  边界、OpenMetrics、非 root 镜像、SBOM/provenance 和供应链策略均已具备。

## 能力与边界

服务提供单一 KV 命名空间及线性一致的 CRUD，支持：

- 静态 1/3/5 节点部署与 leader 转发；
- 持久化日志、崩溃恢复、逻辑快照与落后 follower 安装快照；
- staging 节点及 Raft joint consensus 动态成员变更；
- 基于 client ID 与单调 sequence 的持久化去重；
- 结构化日志、运行状态、指标、TLS/mTLS 和兼容性拒绝规则。

它有意不支持多键事务、扫描、watch、TTL、分片/Multi-Raft、拜占庭容错或永久丢失
多数派数据后的自动恢复。完整边界见 [`docs/specification.md`](docs/specification.md)。

## 快速开始

前置条件：Rust 1.98.0、Docker Engine 28+ 与 Compose v2、Java 25、`curl`、
OpenSSL，以及可选的 GNU/BSD Make。

```sh
cp .env.example .env
make setup
make run
```

默认 client API 为 `http://localhost:8080`：

```sh
curl --fail http://localhost:8080/health/ready

CLIENT_ID=018f47a7-48c1-7d9a-9db6-4d143ba4c273
curl --fail-with-body -X PUT \
  -H 'Content-Type: application/octet-stream' \
  -H 'If-None-Match: *' \
  -H "Raft-KV-Client-ID: ${CLIENT_ID}" \
  -H 'Raft-KV-Request-Sequence: 1' \
  --data-binary 'hello' \
  http://localhost:8080/v1/keys/greeting
curl --fail-with-body http://localhost:8080/v1/keys/greeting
```

运行三节点 Docker 实验：

```sh
docker compose up --build
```

client listener 位于 `localhost:8081`、`:8082`、`:8083`；带认证的 operational
listener 位于 `:8181`、`:8182`、`:8183`；peer gRPC 只在 Compose 私有网络中。

```sh
docker compose restart node2              # 保留数据并重启
curl -H 'Authorization: Bearer local-development-only' \
  http://localhost:8181/operational/v1/status
make reset-local NODE=node2 CONFIRM=yes   # 显式销毁 node2 本地卷
```

## 如何学习和验证

建议按以下顺序阅读：

1. [`docs/specification.md`](docs/specification.md)：行为、安全性质和完成定义；
2. [`docs/roadmap.md`](docs/roadmap.md)：M0–M7 交付路径；
3. [`docs/adr/`](docs/adr/)：关键架构决策；
4. `src/raft/`：纯核心、runtime、simulator 和 trace；
5. `formal/`：对应各里程碑的 TLA+ 模型；
6. `verification/`：需求到代码、模型、测试和运行证据的映射。

```sh
make check          # fmt + Clippy + 测试 + 快速形式化检查
make formal-full    # 完整 TLC 矩阵 + Apalache 归纳义务
make fuzz           # 真实多进程故障 campaign，保留可重放产物
make mutation       # Raft 与存储 mutation testing
make coverage       # 行/分支覆盖率及真实进程 campaign
make supply-chain   # RustSec、许可证和依赖来源策略
```

发布覆盖率门槛为：普通非生成 Rust 代码至少 80% line / 70% branch，关键共识和持久化
代码至少 90% line / 80% branch。形式化声明边界见
[`docs/adr/0007-verification-closure.md`](docs/adr/0007-verification-closure.md)；故障
campaign 与重放见 [`docs/fuzzing.md`](docs/fuzzing.md)；HTTP 契约见
[`docs/openapi.yaml`](docs/openapi.yaml)。

## 配置与部署

默认配置位于 `config/default.toml`，环境变量采用 `RAFT_KV__SECTION__FIELD` 覆盖；
完整示例见 `.env.example`。影响复制状态机行为的容量设置必须在所有 voter 上一致，
并会写入 SQLite、进入快照及在 peer RPC 中协商。

明文模式只用于 loopback 或隔离实验。分布式部署应配置 client TLS、operational mTLS、
peer mTLS，并使用外部认证授权层保护 client API。生产镜像以 UID/GID 10001 运行；
建议使用只读根文件系统、可写 `/var/lib/raft-kv` 数据卷和只读 secret mount。详见
[`docs/supply-chain-policy.md`](docs/supply-chain-policy.md)。

## 项目状态

规范 1.0 的 M0–M7 已完成。项目适合学习、演示和受控环境实验；真实业务部署仍应根据
自身威胁模型、容量目标、备份策略和平台进行独立评审。最受欢迎的贡献，是把发现的
反例转化为可复现 trace、测试或形式化 counterexample。

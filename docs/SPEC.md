# IM 消息内核 · 项目规格（Spec）

> 本文档是项目的唯一权威规格：定位、范围、架构、协议、可靠性模型、路线图。
> 后续开发以本文档为准；偏离本文档的设计变更须先更新本文档。
> 最后更新：2026-11-19（M2.3 完成）

## 1. 定位

企业协作平台（飞书类）的**消息内核子系统**，不是完整飞书。Go 后端求职简历项目，
要求每个设计决策能回答：为什么是这个方案、数字怎么算、挂了怎么办、比简单方案好在哪。

形态：分布式精简版——`gateway`（接入层）+ `logic`（业务层）两个服务，M3 起 gateway 多实例。

## 2. 范围

**做**：单聊、小群群聊（≤500 人）、在线状态/typing、多端同步、人机混合群聊（M5）。

**不做**（非目标）：已读回执、撤回、富媒体、音视频、组织架构、消息搜索、Kafka、
万人群、消息漫游之外的社交功能（好友关系简化为直接可聊）。

## 3. 量化目标（简历可写、验收可测）

| 指标 | 目标 | 验证方式 |
|---|---|---|
| 单机长连接 | ≥ 2 万 | M4 压测（imcli 种子客户端） |
| 在线推送延迟 | P99 < 100ms | M4 Prometheus 直方图 |
| 可靠性 | 故障注入零丢消息 | M2.6 杀进程/断网 + 重连补齐断言 |
| 分布式正确性 | 双 gateway 跨实例投递正确 | M3 双实例 smoke |

## 4. 架构

```
客户端(imcli/agent) ──WS(protobuf Frame)──> gateway ──gRPC──> logic ──> MySQL(消息真相)
     ^                                       |                  └──> Redis(seq 发号/路由)
     └────────── 推送(gRPC Push) ────────────┘
```

- **gateway**：持有连接，只做接入/心跳/背压/帧分发/身份强制，不碰 DB。无状态化方向演进。
- **logic**：校验、幂等、发号、落库、路由推送、同步查询。碰 DB 的慢操作与连接收发资源隔离。
- **单一契约**：`api/proto/im/v1/im.proto` 同时定义 WS 帧与 gRPC 服务，生成代码不手改。
- 拆分动机：DB IO（毫秒级）与连接收发（微秒级）混部会耗尽线程池雪崩。

## 5. 可靠性模型（核心设计）

**先落库拿 seq，推送只是通知；客户端凭 seq 水位增量同步兜底。**

- 推送通道**允许丢**：push 失败仅记录，`delivered=false` 不算发送失败。因此不需要 Kafka。
- 发送路径：`验证 → 推导 conv_id → 查重 → 领号 → 落库 → 推送 → 返回 seq`。
  - 查重在领号前：落库成功但响应丢失的重试 → 幂等返回原 seq，不领新号。
  - 落库失败的重试 → 领新号，旧号成**永久空洞**，无害（同步是范围拉取，不校验连续性）。
  - 落库失败**绝不推送**（否则推送与存储永久分歧）。
  - INSERT 撞 `uk_conv_msg` → 二次查询返回赢家 seq（并发重试收敛）。
- 幂等：客户端生成 `msg_id`（crypto/rand），DB `UNIQUE(conv_id, msg_id)` 最终防线。
- 防重号纵深：Redis INCR 原子性（第一道）+ `UNIQUE(conv_id, seq)`（第二道，触发即告警）。

### 5.1 定序语义

- seq 保证**会话级一致性全序**：所有端看到的顺序一致；**不是**发送者意图序。
- 同一 TCP 连接内字节流有序，网络抖动只延迟不乱序；乱序发生在跨连接/多端/重试场景，可接受。
- 因果序（prev_msg_id 依赖链）不做，过度设计。
- 有序性作用域是会话，全局有序是过度约束且制造发号热点。

### 5.2 客户端水位同步

- 水位 = 客户端本地持久化的整数/会话："seq ≤ N 的消息我全有了"（连续前缀语义）。
- 上线/重连：`SYNC{conv_id, since=水位}` → `seq > since ORDER BY seq LIMIT n` 游标分页，
  `has_more` 用多查一条判定；循环拉到不满页为止。**禁 OFFSET**。
- 水位推进规则：同步响应来自范围查询、自带覆盖证明 → 直接推到本批最大 seq；
  单条推送无覆盖证明 → `seq > 水位+1` 说明有洞，暂存并触发 SYNC 补齐，水位只推进连续前缀。
- 纪律：**先写本地缓存，再推水位**（崩溃时宁可重复不可丢；重复按 (conv_id, seq) 去重）。
- 空洞语义："连续覆盖"指存在的消息都收到，不存在的废号不需要等。
- limit 服务端钳制：0→默认 100，上限 200。[PRODUCTION NOTE] 生产加字节数双上限防超帧。

## 6. 协议（api/proto/im/v1/im.proto）

- `Envelope{msg_id, type, from, to, content, timestamp_ms, hop(M5), seq}`：系统内唯一消息体。
- `Frame{oneof: message / sync_req / sync_resp}`：WS 二进制帧唯一格式。
- `Logic.SendMessage`：返回 `{msg_id, delivered, seq}`；`Logic.Sync`：成员校验 + 分页。
- `Gateway.Push`：logic → 接收者所在 gateway 定向投递。
- 发送回执：gateway 把带 seq 的原消息回显发送者（From==自己即回执）。
- 身份强制：`Envelope.From` 与 `SyncRequest.user_id` 均由 gateway 改写为连接身份，客户端伪造无效。

## 7. 存储

### 7.1 MySQL（消息真相，读扩散时间线）

```sql
messages(
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  conv_id VARCHAR(64) NOT NULL,      -- 单聊 "p:"+sorted(uidA,uidB)；群聊 "g:{groupID}"(M4)
  seq BIGINT NOT NULL,
  msg_id CHAR(16) NOT NULL,
  sender VARCHAR(32) NOT NULL,
  content VARCHAR(4096) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  UNIQUE KEY uk_conv_seq (conv_id, seq),   -- 定序防线 + 范围查询索引
  UNIQUE KEY uk_conv_msg (conv_id, msg_id) -- 幂等防线
)
```

- 读扩散：一条消息只存一份，单聊群聊同表；用户侧只存读水位（客户端）。
- `(conv_id, seq)` B+ 树复合索引：范围扫描天然有序，无 filesort。
- 无物理外键（写热路径），引用完整性由鉴权/成员校验层保证。
- users 表 M2.4 随鉴权引入。

### 7.2 Redis

- **seq 发号**：`seq:{conv_id}` → INCR，**永不设 TTL**。Lua 两段式：正常路径一次 EVAL
  （-1 哨兵探测键缺失）；恢复路径查 DB `MAX(seq)` 初始化再 INCR，防 seq 回退重号。
- **全局路由表**（M3）：`uid → gateway 实例`，注册覆盖 / 心跳 TTL 续期 / 断开时比较 value 再删。
- 不做消息缓存（量级不需要，见演进方向）。

## 8. 连接管理（gateway）

- 每连接 readPump/writePump 两 goroutine；gorilla/websocket 写不并发安全，
  所有写经 `sendCh`（容量 256）由 writePump 串行化。
- **背压**：trySend 非阻塞，队列满即断开慢消费者——宁可断一条，不拖垮整机。
- 心跳：Ping 30s / Pong 超时 60s（读 deadline 续期）/ 写超时 10s / RPC 超时 3s / 帧上限 64KB。
  应用层心跳而非 TCP keepalive：检测业务假死、保活 NAT 映射、携带续期语义。
- 关闭：sync.Once + close(closeCh) + ws.Close()（唤醒阻塞读）。
- hub 本地路由 `map[uid]*conn` + RWMutex；remove 只删自己（防误删新连接）；
  M1 单端新登录踢旧，M3 多端 uid→连接集合扇出。
- 客户端重连（M2.5）：指数退避 + 随机 jitter，防断线风暴；重连后带水位一次性同步。
- 服务端保护（M4）：新建连接准入限流 + 单机连接数上限，宁可拒绝一部分不能全倒。

## 9. 安全

- M2.4：users 表 + register/login HTTP（bcrypt，golang.org/x/crypto）+ JWT 签发
  （golang-jwt/v5 ≥5.2.2）+ gateway 握手校验。
- 会话成员校验：logic.Sync 校验连接身份 ∈ 会话成员，防越权读他人会话（已实现）。
- [PRODUCTION NOTE] 项：CheckOrigin 校验、gRPC mTLS、密钥不落盘明文、WS 握手 token 传递方式。

## 10. 路线图与状态

| 里程碑 | 内容 | 状态 |
|---|---|---|
| M1 | 骨架：WS 接入 + gRPC 双服务 + 单聊推送 + imcli | ✅ 2026-11-18 |
| M2.1 | Docker MySQL/Redis + messages 表 + store + seq 发号器 | ✅ 2026-11-18 |
| M2.2 | 发送路径编排：查重→领号→落库→推送 + seq 回执 | ✅ 2026-11-18 |
| M2.3 | 同步协议：Frame oneof + Logic.Sync + 游标分页 + 成员校验 | ✅ 2026-11-19 |
| M2.4 | 鉴权：users 表 + register/login + bcrypt + JWT 握手 | 待做 |
| M2.5 | 客户端：水位文件 + SYNC 循环 + 连续前缀推进 + **退避重连+jitter** | 待做 |
| M2.6 | 端到端验收：故障注入（杀 gateway/断连）零丢消息 + 离线补齐 smoke | 待做 |
| M3 | 分布式：Redis 路由表（TTL 续期/比较删除）+ 跨实例定向 Push + 多端扇出 | 待做 |
| M4 | 群聊（g:conv，≤500 人）+ presence/typing + **准入限流/连接上限** + Prometheus 压测 | 待做 |
| M5 | 人机混聊：agent 同协议接入 + @mention 路由 + 流式分片 + hop 防循环 | 待做 |

## 11. 验证标准（每个里程碑必须全过）

1. `go build ./...` + `go vet ./...`
2. 全量 `go test -race -count=2 ./...`（Windows 需 mingw gcc，CGO_ENABLED=1）
3. 集成测试隔离：每测试独占 conv_id/键，清理覆盖**所有**触碰的资源；禁 TRUNCATE/FlushDB
4. smoke 双进程 + **DB 落库断言**（不只 grep 日志；警惕端口占用导致打在旧进程上的假阳性）
5. 环境前置：`docker compose ps` 确认 MySQL/Redis healthy
6. 不 git commit，除非用户明确要求

## 12. 依赖（8 个，全部活跃维护）

gorilla/websocket(BSD-2)、redis/go-redis/v9(BSD-2)、grpc(Apache-2.0)、protobuf(BSD-3)、
go-sql-driver/mysql(MPL-2.0)、jmoiron/sqlx(MIT)、golang-jwt/jwt/v5(MIT)、prometheus/client_golang(Apache-2.0)。
M2.4 新增 golang.org/x/crypto（bcrypt）。

**不用**：Gin（stdlib net/http，Go 1.22 路由）、zap（log/slog）、Viper（encoding/json）、
uuid/snowflake（crypto/rand）、Kafka（推送允许丢，同步兜底）。

## 13. 演进方向（明确不实现，面试可讲）

- 万人群推拉结合：只推"有新消息"信令，客户端按水位拉——≤500 人小群直接推即可。
- 分级存储：Redis 缓存每会话最近 N 条 + 拉取上限防穿透——2 万连接量级 MySQL 直查够用。
- 独立状态服务 / IPCONFIG IP 调度 / 一致性哈希路由——路由三方案权衡见面试文档。
- 已读回执、消息搜索、富媒体——非目标。

## 14. 参考资料

- [面试要点-Plato分布式IM模拟面试.md](../面试要点-Plato分布式IM模拟面试.md)：追问话术与估算框架
- [系统设计-10亿流量IM设计要点.md](../系统设计-10亿流量IM设计要点.md)：演进方法论与 trade-off 模板
# 踩坑记录（M1 → M2.6）

> 每条按 **现象 → 根因 → 修复 → 教训** 记录，只收真实发生过的 bug 和坑，不含假设。
> 用途：面试复盘素材 + 后续里程碑的前车之鉴。最后更新：2026-11-20（M2.6 完成）。

---

## 一、环境与基础设施

### E1. MySQL 连接拒绝（两次，原因不同）

- **现象**：集成测试/Smoke 启动即 `connection refused`。
- **根因**：第一次是容器隔夜被杀（exit 137，内存压力）；第二次是机器重启后 Docker Desktop 根本没启动，daemon 不在。
- **修复**：`docker compose up -d`；daemon 不在时先启动 Docker Desktop，轮询 `docker info` 就绪后再 compose up。
- **教训**：**每轮验证前先 `docker compose ps`**，已写进 SPEC 验证标准。"服务连不上"先分清是容器挂了还是 daemon 挂了，两者修法不同。

### E2. schema 改了但库里没有新表

- **现象**：M2.4 加了 users/refresh_tokens 表，代码报 table doesn't exist。
- **根因**：`docker-entrypoint-initdb.d` 的初始化脚本**只在数据卷为空时执行一次**，已有卷不会重跑。
- **修复**：`docker exec -i im-mysql mysql -uroot -pimdev123 im < scripts/schema.sql`（CREATE IF NOT EXISTS 幂等，可重复执行）。
- **教训**：容器初始化钩子 ≠ 迁移工具。schema 变更必须手动/工具显式应用，别指望重启容器。

### E3. Windows 下 -race 跑不起来

- **现象**：`go test -race` 报 gcc 缺失。
- **根因**：race detector 依赖 CGO，Windows 默认没有 C 编译器。
- **修复**：scoop 装 mingw，`export PATH="$PATH:/c/Users/81487/scoop/apps/mingw/current/bin"` + `CGO_ENABLED=1`。
- **教训**：验证命令的环境依赖要写进文档（SPEC 验证标准第 2 条），换机器不抓瞎。

---

## 二、测试与验证方法论（本项目最贵的一类坑）

### B1. 旧进程占端口 → Smoke 假阳性（M2.2）

- **现象**：SMOKE PASS，但 messages 表是空的。
- **根因**：M1 的旧 logic.exe 还占着 9002 端口；新 logic 绑定失败直接退出，smoke 全程打在**旧进程**上——旧进程没有落库逻辑，日志却一切正常。
- **修复**：taskkill 旧进程重跑；smoke 增加 DB 终态断言（seq == Redis 水位）。
- **教训**：**日志 grep 不算验证，终态才算**。端口被占 = 测错了对象。验证前检查端口干净，验证后查 DB 行数/内容。

### B2. 固定文案 grep 命中历史消息 → 假阳性（M2.5）

- **现象**：fault/smoke 第一轮 PASS，但 DB 里根本没有本轮发的消息。
- **根因**：DB 跨轮累积历史，`grep "hello from alice"` 命中的是上一轮落库的同名消息，本轮消息其实丢了（见 B5）。
- **修复**：消息标记每轮唯一（`hello-<epoch>`）；终态断言"本轮消息在 DB 恰好 1 条"。
- **教训**：B1 的变种——**断言必须能区分"本轮产生的"和"历史遗留的"**。唯一标记 + 计数断言，二者缺一不可。

### B3. 测试清理漏了第二个会话 → -count=2 失败（M2.3，同类问题出现两次）

- **现象**：`TestListMessagesRangeOrderLimit` 单跑过，`-count=2` 第二遍报 duplicate seq。
- **根因**：测试用了两个 conv_id，cleanup 只清了主会话，`conv+":other"` 的数据留到第二遍撞唯一键。
- **修复**：测试前 DELETE + `t.Cleanup` 覆盖**每一个**触碰的 conv_id。
- **教训**：清理清单 = 资源触碰清单，写测试时先列"我碰了哪些键/表/会话"。禁 TRUNCATE/FlushDB（会误伤并行测试），按精确键清理。

### B4. fake 共享指针别名 → 数据竞争隐患（M2.3）

- **现象**：review 时发现 loopbackLogic 把收到的 env 指针原样 deliver 出去。
- **根因**：readPump 后续会改写同一个 Envelope（强制 From 等），fake 与真实代码共享可变指针，-race 下是竞争。
- **修复**：fake deliver 前 `proto.Clone`。
- **教训**：测试替身也要遵守所有权边界——**交出去的对象要么克隆要么不可变**，否则测试自己制造竞争。

### B9. 多端语义下发送者收两帧，测试只消费一帧 → 后续断言错位（M3）

- **现象**：TestMultiDeviceFanout 里 bob 读"下一条消息"，读到的却是自己刚发的那条（seq 旧一轮）。
- **根因**：M3 扇出双方后，发送设备会收到**本地回执 + 扇出副本两帧**（同 msg_id 同 seq，客户端按 seq 去重）。测试替身忠实模拟了扇出，但断言还停留在 M1"发送者只收一帧回执"的心智模型，队列里剩一帧，后面所有读取整体错位。
- **修复**：发送方读两帧并都断言；端到端测试里 alice 的回执断言改为循环读两帧。
- **教训**：**语义变更要同步更新测试的消费模型**。协议从"一问一答"变成"一问多帧"时，每个读取点的期望帧数都要重新推导——帧错位不会报错，只会让后面的断言拿错数据。

### B10. 缓存路径少算一层目录 → 水位断言读空文件（M3）

- **现象**：smoke2 所有投递断言都过，终态却报 `bob1 watermark '' != db max`。
- **根因**：imcli 缓存布局是 `-dir/<uid>/<conv>.json` 两层；smoke2 给每台设备传了 `-dir $CACHE/bob1`，断言却去读 `$CACHE/bob1/p_alice_bob.json`——少了 uid 层，sed 读不存在的文件输出空串。
- **修复**：断言路径改为 `$CACHE/$d/$u/p_alice_bob.json`（设备目录与用户名是两层）。
- **教训**：B1/B2 同族——**断言本身读错了对象**。空串和"水位为 0"要区分：`[ -n "$WM" ]` 先断言文件读到了，再比较值；路径拼接直接复用真实布局公式，不凭记忆手写。

---

## 三、客户端并发 bug（M2.5 / M2.6，故障注入抓出来的）

### B5. stdin EOF 与最后一帧写入竞态 → 消息静默丢失（M2.5）

- **现象**：`(sleep 1; echo hello) | imcli`，hello 没落库；单测全绿、smoke 因 B2 假阳性显示 PASS。
- **根因**：echo 后 stdin 立即 EOF，主 goroutine 把帧塞进 sendCh 后马上 `close()`；writePump 的 select 里 `sendCh` 和 `done` 同时就绪，**Go 随机选**——选中 done，最后一帧被丢弃（约 50% 概率）。
- **修复**：正常退出先 `flush()`（轮询等 sendCh 排空 + 留一个写周期）再 close。
- **教训**：**channel 关闭与在途数据竞态**，和服务端 graceful shutdown 要 drain 请求是同一族问题。"发出去了"≠"写进 socket 了"，退出路径必须给在途数据留路。

### B6. 重连循环饿死 stdin → 隐式排队（M2.6）

- **现象**：故障注入第一轮 FAIL——gateway 宕机期间发的 `gwdown` 消息居然落库了，违反"断线期间发送立即报错不排队"的设计。
- **根因**：主 goroutine 兼任重连循环，退避 sleep 期间没人 select lines；无缓冲 lines channel 把断线期输入**阻塞在 scanner goroutine 里**，重连成功后被当正常消息发出——设计说不排队，实现却隐式排了队。
- **修复**：重连拆进独立 `manageConn` goroutine，当前连接用 `atomic.Pointer[session]` 发布；主 goroutine 永远留在 stdin select 上，`cur.Load()==nil` 立即报错。
- **教训**：**一个 goroutine 干两件会互相阻塞的事（等网络 + 等用户输入），必有一件被饿死**。解法不是加缓冲（那会把 bug 变成更隐蔽的排队），而是拆职责 + 原子发布共享状态。单测全绿 + smoke 全绿都没抓到它，真实故障一注入就露馅——这就是 M2.6 存在的意义。

---

## 四、代码质量小坑（低级但反复出现）

### B7. rune 拼数字（M2.3 一次、M2.5 一次）

- **现象**：测试里写 `string(rune('0'+n%10))` 造 ID，第二次又写成 `string(rune('a'+seq%26))`。
- **根因**：图省事手写字符运算，超过个位数/边界就错，且可读性差。
- **修复**：`strconv.FormatInt` / `strconv.FormatUint`。
- **教训**：同一个坑踩两次说明没形成检查习惯——**数字转字符串永远 strconv**，review 时见到 rune 运算直接打回。

### B8. crypto/rand 与 math/rand/v2 包名冲突（M2.5）

- **现象**：编译失败，两个包都叫 rand。
- **修复**：`crand "crypto/rand"` 别名；消息 ID 用 crypto/rand，退避 jitter 用 math/rand/v2（无安全需求）。
- **教训**：随机数分两种——**安全用途（token/ID）必须 crypto/rand，统计用途（jitter/采样）用 math/rand**，别名把意图写在导入上。

---

## 五、概念坑（认知纠偏，面试容易被追问）

| 曾经的误解 | 正确理解 |
|---|---|
| URL query 里的 token 能被篡改 | JWT 有 HMAC 签名，篡改即失效；query 的真风险是**日志泄露与重放**，靠短 TTL + 生产一次性 ticket 缓解 |
| WebSocket 基于 HTTP | 只有握手是 HTTP，升级后是独立的全双工协议；gRPC 才是 HTTP/2 + protobuf |
| 浏览器 WS 可以把 token 放请求头 | `new WebSocket(url)` API 无法设置 header，所以选 query（或首帧鉴权，但有未鉴权窗口） |
| 收到推送 seq=15 就能把水位推到 15 | 单条推送只有**存在证明**；SYNC 范围查询结果集才带**不存在证明**（覆盖证明），才能直推本批最大 |
| 水位 = 会话最大 seq | 水位 = **连续覆盖下界**；推送有洞时水位不动，pending 缓冲 + SYNC 补齐后才推进 |
| 断线重连后直接收推送就行 | 必须先 SYNC：断线窗口的推送已永久丢失（推送通道允许丢），安静场景下没有新推送就永远补不回来 |

---

## 六、重复出现的模式（复盘总结）

1. **假阳性家族（B1、B2）**：验证打在错误对象上（旧进程）或断言区分度不足（历史同名）。
   对策已固化为 SPEC 验证标准：端口检查 + 唯一标记 + DB 终态断言 + 计数断言。
2. **退出/关闭路径丢数据（B5）**：正常路径测得再多，退出路径的竞态只有故障注入能抓。
3. **清理/边界遗漏（B3、B6）**：测试清理漏资源、goroutine 职责漏分离——都是"清单没列全"。
4. **低级手法复发（B7）**：rune 拼数字两次。习惯要靠 review 检查项固化，不能靠记性。

**一句话总结**：本项目最贵的坑不是算法写错，而是**"验证本身不可信"**——假阳性让丢消息的 bug 穿着 PASS 的外衣活过了单测和 smoke，直到终态断言和故障注入把它揪出来。可靠性系统的验收，必须先保证验收工具自己的可靠性。
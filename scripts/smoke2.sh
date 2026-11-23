#!/usr/bin/env bash
# M3.3 双实例验收：跨实例投递 + 多端同步（接收方/发送方）+ gateway 崩溃后路由自愈重连
# 拓扑：logic + gw-1(8081) + gw-2(8083)；bob 两台设备分属两台 gateway，alice 主设备在 gw-1
# 每个设备独立 -dir：两个进程不能共享同一份缓存文件
set -u
cd "$(dirname "$0")/.."

CACHE=.imcli_smoke2
rm -rf "$CACHE"
RUN=$(date +%s)
X1="cross-$RUN"; S1="sendersync-$RUN"; X3="gwback-$RUN"
CONV="p:alice:bob"
GW1=ws://127.0.0.1:8081/ws
GW2=ws://127.0.0.1:8083/ws

docker compose up -d 2>/dev/null   # 幂等：容器已在运行则无操作

# 防旧进程假阳性：启动前所有端口必须空闲
for p in 8081 8083 9002 8082; do
  if netstat -ano | grep LISTENING | grep -Eq ":$p[[:space:]]"; then
    echo "SMOKE2 FAIL: port $p already occupied (stale process?)"; exit 1
  fi
done

./bin/logic.exe   > /tmp/s2_logic.log 2>&1 & LPID=$!
./bin/gateway.exe > /tmp/s2_gw1.log   2>&1 & G1PID=$!
./bin/gateway.exe -config configs/config.gw2.json > /tmp/s2_gw2.log 2>&1 & G2PID=$!
G2PID2=""
sleep 1

AUTH=http://127.0.0.1:8082
curl -s -X POST "$AUTH/register" -d '{"username":"alice","password":"alice123"}' > /dev/null
curl -s -X POST "$AUTH/register" -d '{"username":"bob","password":"bob12345"}' > /dev/null

cleanup() { kill "$LPID" "$G1PID" "$G2PID" "$G2PID2" ${B1PID:-} ${B2PID:-} ${A1PID:-} ${APID:-} 2>/dev/null; wait 2>/dev/null; }
fail() {
  echo "SMOKE2 FAIL: $1"
  for f in /tmp/s2_*.log; do
    [ -f "$f" ] && { echo "=== $f ==="; cat "$f"; }
  done
  cleanup; exit 1
}
dbq() { docker exec im-mysql mysql -uroot -pimdev123 im -N -e "$1" 2>/dev/null; }
rq()  { docker exec im-redis redis-cli "$@"; }

# 三台长连接设备：bob×2 分属 gw-1/gw-2，alice 主设备在 gw-1
(sleep 60) | ./bin/imcli.exe -user bob -peer alice -password bob12345 -addr "$GW1" -dir "$CACHE/bob1" > /tmp/s2_bob1.log 2>&1 & B1PID=$!
(sleep 60) | ./bin/imcli.exe -user bob -peer alice -password bob12345 -addr "$GW2" -dir "$CACHE/bob2" > /tmp/s2_bob2.log 2>&1 & B2PID=$!
(sleep 60) | ./bin/imcli.exe -user alice -peer bob -password alice123 -addr "$GW1" -dir "$CACHE/alice1" > /tmp/s2_alice1.log 2>&1 & A1PID=$!
sleep 2

# 路由表终态：bob 的两条连接分别登记在 gw-1/gw-2（connID 为 field，各占一条）
HLEN=$(rq HLEN route:bob)
[ "$HLEN" = "2" ] || fail "route:bob should have 2 fields, got '$HLEN'"
HVALS=$(rq HVALS route:bob)
echo "$HVALS" | grep -q '^gw-1|' || fail "route:bob missing gw-1 entry: $HVALS"
echo "$HVALS" | grep -q '^gw-2|' || fail "route:bob missing gw-2 entry: $HVALS"

# X1 跨实例 + 接收方多端：alice 经 gw-1 发送，bob 两台设备（一台在 gw-2）都必须收到
(sleep 1; echo "$X1"; sleep 2) | ./bin/imcli.exe -user alice -peer bob -password alice123 -addr "$GW1" -dir "$CACHE/asend" > /tmp/s2_a1.log 2>&1 & APID=$!
sleep 4; wait "$APID" 2>/dev/null
grep -q "$X1" /tmp/s2_bob1.log || fail "X1: bob dev1 (same gw) missed"
grep -q "$X1" /tmp/s2_bob2.log || fail "X1: bob dev2 (cross gw) missed"

# S1 发送方多端：alice 第二台设备经 gw-2 发送，alice 主设备（gw-1）必须收到同步副本
(sleep 1; echo "$S1"; sleep 2) | ./bin/imcli.exe -user alice -peer bob -password alice123 -addr "$GW2" -dir "$CACHE/asend2" > /tmp/s2_a2.log 2>&1 & APID=$!
sleep 4; wait "$APID" 2>/dev/null
grep -q "$S1" /tmp/s2_alice1.log || fail "S1: sender's other device missed sync copy"
grep -q "$S1" /tmp/s2_bob1.log  || fail "S1: bob dev1 missed"
grep -q "$S1" /tmp/s2_bob2.log  || fail "S1: bob dev2 missed"

# F1 杀 gw-2 → 重启：bob dev2 退避重连并先 SYNC，恢复后的消息必须送达
kill "$G2PID" 2>/dev/null; wait "$G2PID" 2>/dev/null
sleep 2
./bin/gateway.exe -config configs/config.gw2.json > /tmp/s2_gw2b.log 2>&1 & G2PID2=$!
sleep 5
(sleep 1; echo "$X3"; sleep 2) | ./bin/imcli.exe -user alice -peer bob -password alice123 -addr "$GW1" -dir "$CACHE/asend" > /tmp/s2_a3.log 2>&1 & APID=$!
sleep 4; wait "$APID" 2>/dev/null
grep -q "$X3" /tmp/s2_bob2.log || fail "F1: bob dev2 after gw-2 restart missed"
grep -q "$X3" /tmp/s2_bob1.log || fail "F1: bob dev1 missed"
[ "$(grep -c '\[connected\]' /tmp/s2_bob2.log)" -ge 2 ] || fail "F1: bob dev2 did not reconnect"

# 终态断言：三台设备水位 == DB 最大 seq；本轮每个标记恰好落库一次
# 缓存布局是 -dir/<user>/<conv>.json，设备目录与用户名是两层
DBMAX=$(dbq "SELECT COALESCE(MAX(seq),0) FROM messages WHERE conv_id='$CONV'")
for pair in bob1:bob bob2:bob alice1:alice; do
  d=${pair%%:*}; u=${pair##*:}
  WM=$(sed -n 's/.*"watermark":\([0-9]*\).*/\1/p' "$CACHE/$d/$u/p_alice_bob.json")
  [ -n "$WM" ] && [ "$WM" = "$DBMAX" ] || fail "final: $d watermark '$WM' != db max '$DBMAX'"
done
for m in "$X1" "$S1" "$X3"; do
  C=$(dbq "SELECT COUNT(*) FROM messages WHERE conv_id='$CONV' AND content='$m'")
  [ "$C" = "1" ] || fail "final: marker '$m' count=$C, want 1"
done

cleanup
echo "SMOKE2 PASS: cross-gw delivery + both-side multi-device sync + gw crash reconnect, watermark=$DBMAX on all 3 devices"
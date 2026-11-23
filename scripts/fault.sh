#!/usr/bin/env bash
# M2.6 故障注入验收：零丢消息 = 凡落库的消息，任意故障组合后接收方都能凭水位补齐；
# 故障期间未落库的消息绝不入库（落库失败绝不推送）。
# 故障面：gateway 进程 / logic 进程 / Redis(SIGKILL+删键) / MySQL(stop/start)
set -u
cd "$(dirname "$0")/.."

CACHE=.imcli_fault
rm -rf "$CACHE"
RUN=$(date +%s)
B0="base-$RUN";  G1="gwdown-$RUN";    G2="gwback-$RUN"
L1="logicdown-$RUN"; L2="logicback-$RUN"; R1="redisback-$RUN"
M1="mysqldown-$RUN"; M2="mysqlback-$RUN"; Z1="final-$RUN"
CONV="p:alice:bob"

docker compose up -d 2>/dev/null   # 幂等：容器已在运行则无操作

./bin/logic.exe   > /tmp/f_logic.log   2>&1 & LPID=$!
./bin/gateway.exe > /tmp/f_gateway.log 2>&1 & GPID=$!
LPID2=""; GPID2=""
sleep 1

AUTH=http://127.0.0.1:8082
curl -s -X POST "$AUTH/register" -d '{"username":"alice","password":"alice123"}' > /dev/null
curl -s -X POST "$AUTH/register" -d '{"username":"bob","password":"bob12345"}' > /dev/null

cleanup() { kill "$LPID" "$LPID2" "$GPID" "$GPID2" ${BPID:-} ${APID:-} 2>/dev/null; wait 2>/dev/null; }
fail() {
  echo "FAULT FAIL: $1"
  for f in /tmp/f_bob.log /tmp/f_a0.log /tmp/f_a1.log /tmp/f_a2.log /tmp/f_a3.log /tmp/f_a4.log /tmp/f_a5.log /tmp/f_logic.log /tmp/f_gateway.log; do
    [ -f "$f" ] && { echo "=== $f ==="; cat "$f"; }
  done
  cleanup; exit 1
}
dbq() { docker exec im-mysql mysql -uroot -pimdev123 im -N -e "$1" 2>/dev/null; }

# bob 全程在线，唯一经历 gateway 重启的长连接
(sleep 80) | ./bin/imcli.exe -user bob -peer alice -password bob12345 -dir "$CACHE" > /tmp/f_bob.log 2>&1 & BPID=$!
sleep 1

# P0 基线：健康态在线投递
(sleep 1; echo "$B0"; sleep 2) | ./bin/imcli.exe -user alice -peer bob -password alice123 -dir "$CACHE" > /tmp/f_a0.log 2>&1 & APID=$!
sleep 4; wait "$APID" 2>/dev/null
grep -q "$B0" /tmp/f_bob.log || fail "P0: baseline online delivery"

# F1 gateway 挂掉→重启：down 期间发送必须失败不入库，恢复后投递正常
kill "$GPID" 2>/dev/null; wait "$GPID" 2>/dev/null
(sleep 1; echo "$G1"; sleep 5; echo "$G2"; sleep 2) | ./bin/imcli.exe -user alice -peer bob -password alice123 -dir "$CACHE" > /tmp/f_a1.log 2>&1 & APID=$!
sleep 3
./bin/gateway.exe > /tmp/f_gateway2.log 2>&1 & GPID2=$!
sleep 6; wait "$APID" 2>/dev/null
grep -q "$G2" /tmp/f_bob.log || fail "F1: post-restart delivery (bob reconnect)"
grep -q "连接不可用" /tmp/f_a1.log || fail "F1: send during outage should be rejected"

# F2 logic 挂掉→重启：发送失败回系统帧不入库，恢复后 seq 连续
(sleep 3; echo "$L1"; sleep 5; echo "$L2"; sleep 2) | ./bin/imcli.exe -user alice -peer bob -password alice123 -dir "$CACHE" > /tmp/f_a2.log 2>&1 & APID=$!
sleep 1
kill "$LPID" 2>/dev/null; wait "$LPID" 2>/dev/null
sleep 2
./bin/logic.exe > /tmp/f_logic2.log 2>&1 & LPID2=$!
sleep 8; wait "$APID" 2>/dev/null
grep -q "$L2" /tmp/f_bob.log || fail "F2: post-restart delivery"
grep -q "消息发送失败" /tmp/f_a2.log || fail "F2: send during outage should get failure notice"

# F3 Redis SIGKILL→重启→删 seq 键：发号器必须从 DB MAX(seq) 恢复，不回退
docker kill -s KILL im-redis >/dev/null 2>&1
docker start im-redis >/dev/null 2>&1
for i in $(seq 1 20); do
  [ "$(docker exec im-redis redis-cli ping 2>/dev/null)" = "PONG" ] && break
  sleep 1
done
docker exec im-redis redis-cli DEL "seq:$CONV" >/dev/null 2>&1
(sleep 1; echo "$R1"; sleep 2) | ./bin/imcli.exe -user alice -peer bob -password alice123 -dir "$CACHE" > /tmp/f_a3.log 2>&1 & APID=$!
sleep 4; wait "$APID" 2>/dev/null
grep -q "$R1" /tmp/f_bob.log || fail "F3: delivery after redis key loss (seq recovery from DB)"

# F4 MySQL stop→start：down 期间消息绝不入库，恢复后正常落库
(sleep 3; echo "$M1"; sleep 15; echo "$M2"; sleep 2) | ./bin/imcli.exe -user alice -peer bob -password alice123 -dir "$CACHE" > /tmp/f_a4.log 2>&1 & APID=$!
sleep 1
docker stop im-mysql >/dev/null 2>&1
sleep 5
docker start im-mysql >/dev/null 2>&1
for i in $(seq 1 30); do
  [ "$(dbq 'SELECT 1')" = "1" ] && break
  sleep 1
done
sleep 12; wait "$APID" 2>/dev/null
grep -q "$M2" /tmp/f_bob.log || fail "F4: delivery after mysql restart"
grep -q "消息发送失败" /tmp/f_a4.log || fail "F4: send during outage should get failure notice"

# F5 终态健康投递
(sleep 1; echo "$Z1"; sleep 2) | ./bin/imcli.exe -user alice -peer bob -password alice123 -dir "$CACHE" > /tmp/f_a5.log 2>&1 & APID=$!
sleep 4; wait "$APID" 2>/dev/null
grep -q "$Z1" /tmp/f_bob.log || fail "F5: final delivery"

kill "$BPID" 2>/dev/null; wait "$BPID" 2>/dev/null

# ---- 终态断言（验证最终状态，不靠日志 grep）----
DBMAX=$(dbq "SELECT COALESCE(MAX(seq),0) FROM messages WHERE conv_id='$CONV'")
WM=$(sed -n 's/.*"watermark":\([0-9]*\).*/\1/p' "$CACHE/bob/p_alice_bob.json")
RWM=$(docker exec im-redis redis-cli GET "seq:$CONV" 2>/dev/null)
[ -n "$WM" ] && [ "$WM" = "$DBMAX" ] || fail "final: bob cache watermark '$WM' != db max '$DBMAX'"
[ "$RWM" = "$DBMAX" ] || fail "final: redis watermark '$RWM' != db max '$DBMAX'"

for m in "$B0" "$G2" "$L2" "$R1" "$M2" "$Z1"; do
  [ "$(dbq "SELECT COUNT(*) FROM messages WHERE conv_id='$CONV' AND content='$m'")" = "1" ] || fail "final: healthy message '$m' not persisted exactly once"
  grep -q "$m" "$CACHE/bob/p_alice_bob.json" || fail "final: '$m' missing from bob cache"
done
for m in "$G1" "$L1" "$M1"; do
  [ "$(dbq "SELECT COUNT(*) FROM messages WHERE conv_id='$CONV' AND content='$m'")" = "0" ] || fail "final: outage message '$m' leaked into DB"
done
DUP=$(dbq "SELECT seq FROM messages WHERE conv_id='$CONV' GROUP BY seq HAVING COUNT(*)>1")
[ -z "$DUP" ] || fail "final: duplicate seq found: $DUP"

cleanup
echo "FAULT PASS: gateway/logic/redis/mysql 四面故障注入，落库消息零丢失，水位三方对齐 (cache=redis=db=$DBMAX)"
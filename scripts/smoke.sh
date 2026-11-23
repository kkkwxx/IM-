#!/usr/bin/env bash
# M2.5 集成冒烟：在线投递 → 重启续传 → 离线追赶 → 杀 gateway 退避重连
# imcli 走密码模式：自己 /login、保存 refresh_token、重启时 /refresh 续期
set -u
cd "$(dirname "$0")/.."

CACHE=.imcli_smoke
rm -rf "$CACHE"

# 每轮唯一标记：DB 里有历史同名消息，固定文案会让 grep 命中历史造成假阳性
RUN=$(date +%s)
H="hello-$RUN"; O1="offline-$RUN-1"; O2="offline-$RUN-2"; O3="offline-$RUN-3"; A="after-reconnect-$RUN"

./bin/logic.exe   > /tmp/im_logic.log   2>&1 & LPID=$!
./bin/gateway.exe > /tmp/im_gateway.log 2>&1 & GPID=$!
GPID2=""
sleep 1

AUTH=http://127.0.0.1:8082
curl -s -X POST "$AUTH/register" -d '{"username":"alice","password":"alice123"}' > /dev/null
curl -s -X POST "$AUTH/register" -d '{"username":"bob","password":"bob12345"}' > /dev/null

cleanup() { kill "$LPID" "$GPID" "$GPID2" ${BPID:-} ${APID:-} 2>/dev/null; wait 2>/dev/null; }
fail() {
  echo "SMOKE FAIL: $1"
  for f in /tmp/im_bob.log /tmp/im_bob2.log /tmp/im_bob3.log /tmp/im_bob4.log /tmp/im_alice.log /tmp/im_gateway.log /tmp/im_gateway2.log /tmp/im_logic.log; do
    [ -f "$f" ] && { echo "=== $f ==="; cat "$f"; }
  done
  cleanup; exit 1
}

# 阶段1：bob 在线，alice 发一条（password 模式自动登录）
(sleep 8) | ./bin/imcli.exe -user bob -peer alice -password bob12345 -dir "$CACHE" > /tmp/im_bob.log 2>&1 & BPID=$!
sleep 1
(sleep 1; echo "$H") | ./bin/imcli.exe -user alice -peer bob -password alice123 -dir "$CACHE" > /tmp/im_alice.log 2>&1 & APID=$!
sleep 3
grep -q "$H" /tmp/im_bob.log || fail "phase1: bob did not receive online push"
[ -f "$CACHE/bob/refresh_token" ] || fail "phase1: refresh_token file not saved"

# 阶段2：重启 bob —— 历史应从缓存渲染，SYNC 应为 +0（refresh_token 续期也在此验证）
kill "$BPID" 2>/dev/null; wait "$BPID" 2>/dev/null
sleep 1
(sleep 4) | ./bin/imcli.exe -user bob -peer alice -password bob12345 -dir "$CACHE" > /tmp/im_bob2.log 2>&1 & BPID=$!
sleep 3
grep -q "$H" /tmp/im_bob2.log || fail "phase2: history not rendered from cache"
grep -q "+0 new" /tmp/im_bob2.log || fail "phase2: sync after restart should return 0 new"

# 阶段3：bob 离线期间 alice 发 3 条，bob 重启后应靠 SYNC 全部补回
kill "$BPID" 2>/dev/null; wait "$BPID" 2>/dev/null
(sleep 1; echo "$O1"; echo "$O2"; echo "$O3"; sleep 2) | ./bin/imcli.exe -user alice -peer bob -password alice123 -dir "$CACHE" > /tmp/im_alice3.log 2>&1 & APID=$!
sleep 4
(sleep 4) | ./bin/imcli.exe -user bob -peer alice -password bob12345 -dir "$CACHE" > /tmp/im_bob3.log 2>&1 & BPID=$!
sleep 3
grep -q "$O3" /tmp/im_bob3.log || fail "phase3: offline messages not caught up via sync"

# 阶段4：杀 gateway → 重启 → bob 应退避重连，并收到重连后的新消息
kill "$BPID" 2>/dev/null; wait "$BPID" 2>/dev/null
(sleep 14) | ./bin/imcli.exe -user bob -peer alice -password bob12345 -dir "$CACHE" > /tmp/im_bob4.log 2>&1 & BPID=$!
sleep 2
kill "$GPID" 2>/dev/null; wait "$GPID" 2>/dev/null
sleep 2
./bin/gateway.exe > /tmp/im_gateway2.log 2>&1 & GPID2=$!
sleep 3
(sleep 1; echo "$A"; sleep 2) | ./bin/imcli.exe -user alice -peer bob -password alice123 -dir "$CACHE" > /tmp/im_alice4.log 2>&1 & APID=$!
sleep 4
grep -q "$A" /tmp/im_bob4.log || fail "phase4: message after gateway restart not delivered"
[ "$(grep -c "\[connected\]" /tmp/im_bob4.log)" -ge 2 ] || fail "phase4: bob did not reconnect"

# 终态断言：bob 缓存水位 == DB 该会话最大 seq；本轮 hello 必须真实落库
DBMAX=$(docker exec im-mysql mysql -uroot -pimdev123 im -N -e "SELECT COALESCE(MAX(seq),0) FROM messages WHERE conv_id='p:alice:bob'" 2>/dev/null)
WM=$(sed -n 's/.*"watermark":\([0-9]*\).*/\1/p' "$CACHE/bob/p_alice_bob.json")
[ -n "$WM" ] && [ "$WM" = "$DBMAX" ] || fail "final: cache watermark '$WM' != db max seq '$DBMAX'"
HCOUNT=$(docker exec im-mysql mysql -uroot -pimdev123 im -N -e "SELECT COUNT(*) FROM messages WHERE conv_id='p:alice:bob' AND content='$H'" 2>/dev/null)
[ "$HCOUNT" = "1" ] || fail "final: phase1 message '$H' not persisted (count=$HCOUNT)"

cleanup
echo "=== bob4.log ==="; cat /tmp/im_bob4.log
echo "SMOKE PASS: online push + restart resume + offline catch-up + reconnect, watermark=$WM == db max"
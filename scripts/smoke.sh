#!/usr/bin/env bash
# M1 集成冒烟测试：logic + gateway 双进程，alice 发消息给在线的 bob
set -u
cd "$(dirname "$0")/.."

./bin/logic.exe   > /tmp/im_logic.log   2>&1 & LPID=$!
./bin/gateway.exe > /tmp/im_gateway.log 2>&1 & GPID=$!
sleep 1

# bob 先上线并保持 6 秒
(sleep 6) | ./bin/imcli.exe -user bob -peer alice > /tmp/im_bob.log 2>&1 & BPID=$!
sleep 1
# alice 上线 1 秒后发一条消息
(sleep 1; echo "hello from alice") | ./bin/imcli.exe -user alice -peer bob > /tmp/im_alice.log 2>&1 & APID=$!

sleep 4
kill "$LPID" "$GPID" "$BPID" "$APID" 2>/dev/null
wait 2>/dev/null

echo "=== bob.log ===";   cat /tmp/im_bob.log
echo "=== alice.log ==="; cat /tmp/im_alice.log
echo "=== gateway.log ==="; cat /tmp/im_gateway.log

if grep -q "hello from alice" /tmp/im_bob.log; then
  echo "SMOKE PASS: bob received alice's message"
else
  echo "SMOKE FAIL"; exit 1
fi
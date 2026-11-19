package main

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	"im/api/imv1"
)

// imcli 是 M1 的手动验证客户端，也是 M4 压测客户端的种子。
// 用法: imcli -user alice -peer bob
func main() {
	addr := flag.String("addr", "ws://127.0.0.1:8081/ws", "gateway websocket address")
	user := flag.String("user", "", "my user id")
	peer := flag.String("peer", "", "peer user id")
	flag.Parse()

	if *user == "" || *peer == "" {
		fmt.Fprintln(os.Stderr, "usage: imcli -user alice -peer bob")
		os.Exit(1)
	}

	u := *addr + "?user_id=" + url.QueryEscape(*user)
	ws, _, err := websocket.DefaultDialer.Dial(u, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "dial:", err)
		os.Exit(1)
	}
	defer ws.Close()

	go readLoop(ws)

	fmt.Printf("connected as %q -> %q. type a message and press enter:\n", *user, *peer)
	sc := bufio.NewScanner(os.Stdin)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		env := &imv1.Envelope{
			MsgId:   newID(),
			Type:    imv1.MsgType_MSG_TYPE_TEXT,
			From:    *user,
			To:      *peer,
			Content: line,
		}
		data, err := proto.Marshal(&imv1.Frame{Payload: &imv1.Frame_Message{Message: env}})
		if err != nil {
			fmt.Fprintln(os.Stderr, "marshal:", err)
			continue
		}
		ws.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if err := ws.WriteMessage(websocket.BinaryMessage, data); err != nil {
			fmt.Fprintln(os.Stderr, "write:", err)
			return
		}
	}
	// 正常退出前发送关闭帧，完成关闭握手，避免服务端记录 1006 异常断开
	ws.WriteMessage(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
}

func readLoop(ws *websocket.Conn) {
	for {
		_, data, err := ws.ReadMessage()
		if err != nil {
			fmt.Println("\nconnection closed:", err)
			os.Exit(0)
		}
		frame := &imv1.Frame{}
		if err := proto.Unmarshal(data, frame); err != nil {
			fmt.Fprintln(os.Stderr, "unmarshal:", err)
			continue
		}
		switch p := frame.Payload.(type) {
		case *imv1.Frame_Message:
			fmt.Printf("\n[%s] %s\n> ", p.Message.From, p.Message.Content)
		case *imv1.Frame_SyncResp:
			// [M2.5] 这里将写入本地缓存并推进水位；当前仅打印验证协议连通
			fmt.Printf("\n[sync] %d messages, has_more=%v\n> ", len(p.SyncResp.Messages), p.SyncResp.HasMore)
		}
	}
}

func newID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}
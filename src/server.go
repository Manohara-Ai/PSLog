package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
)

const socketPath = "/tmp/pslog.sock"

type Message struct {
	Type string `json:"type"`

	Topic     string   `json:"topic"`
	Port      uint16   `json:"port"`
	Qos       string   `json:"qos"`
	Auth      *string  `json:"auth"`
	Persist   bool     `json:"persist"`
	Exec      string   `json:"exec"`
	ChildArgs []string `json:"child_args"`

	Fos    string `json:"fos"`
	Format string `json:"format"`
}


func main() {
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		fmt.Println("failed to remove old socket:", err)
		return
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		fmt.Println("failed to listen:", err)
		return
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("accept error:", err)
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		lenBuf := make([]byte, 4)
		_, err := io.ReadFull(conn, lenBuf)
		if err != nil {
			return
		}

		length := binary.BigEndian.Uint32(lenBuf)

		data := make([]byte, length)
		_, err = io.ReadFull(conn, data)
		if err != nil {
			return
		}

		var msg Message
		if err := json.Unmarshal(data, &msg); err != nil {
			fmt.Println("invalid json:", err)
			continue
		}

		handleMessage(conn, msg)
	}
}

func handleMessage(conn net.Conn, msg Message) {
	switch msg.Type {

	case "Ping":
		fmt.Println("Ping received")
		sendResponse(conn, map[string]string{"type": "Pong"})

	case "Pub":
		fmt.Println("PUB request")
		fmt.Printf("  %+v\n", msg)

	case "Sub":
		fmt.Println("SUB request")
		fmt.Printf("  %+v\n", msg)

	case "Scan":
		fmt.Println("SCAN request")

	case "Close":
		fmt.Println("Client requested close")
		conn.Close()
		return

	default:
		fmt.Println("unknown message type:", msg.Type)
	}
}

func sendResponse(conn net.Conn, v interface{}) {
	data, err := json.Marshal(v)
	if err != nil {
		return
	}

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))

	conn.Write(lenBuf)
	conn.Write(data)
}
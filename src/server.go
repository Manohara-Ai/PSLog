package main

import "C"
import (
	"bufio"
	"fmt"
	"io"
	"net"
)

//export startGoServer
func startGoServer(port int) {
	addr := fmt.Sprintf(":%d", port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Println("failed to create listener, err:", err)
		return
	}
	defer listener.Close()

	fmt.Printf("listening on %s\n", listener.Addr())

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("failed to accept connection, err:", err)
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	for {
		bytes, err := reader.ReadBytes(byte('\n'))
		if err != nil {
			if err != io.EOF {
				fmt.Println("failed to read data, err:", err)
			}
			return
		}

		fmt.Printf("request: %s", bytes)

		line := fmt.Sprintf("Echo: %s", bytes)
		fmt.Printf("response: %s", line)

		_, err = conn.Write([]byte(line))
		if err != nil {
			fmt.Println("failed to write data, err:", err)
			return
		}
	}
}

func main() {}
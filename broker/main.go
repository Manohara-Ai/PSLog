/*
 * PSLOG BROKER: Lightweight Pub/Sub log routing server
 *
 * PURPOSE:
 * Handles log streaming between publishers and subscribers using a
 * Unix socket based control plane and TCP-based data fanout.
 *
 * DATA STRUCTURES:
 * - Message: wire protocol for Pub/Sub/Log communication
 * - LogEntry: structured log payload (timestamp, level, message)
 * - ConnMeta: tracks connection role (pub/sub), topic, persistence flag
 * - TopicState: holds subscribers and optional ring buffer of logs
 *
 * GLOBAL STATE:
 * - connState: maps connection → metadata
 * - topics: maps topic → subscribers + buffer
 * - mu: protects all shared state
 *
 * MAIN FLOW:
 * 1. Start Unix socket server (/tmp/pslog.sock)
 * 2. Accept connections (one goroutine per connection)
 * 3. Read framed messages in a loop
 * 4. Route via handleMessage:
 *      - Pub → register publisher
 *      - Sub → connect subscriber + replay buffer
 *      - Log → append buffer + fanout to subscribers
 *      - Close → cleanup connection
 *
 * NOTES:
 * - In-memory only (no persistence beyond optional per-topic buffer)
 * - Fanout is push-based over TCP
 * - Buffer acts as a fixed-size ring (FIFO eviction)
 */

package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
)

const socketPath = "/tmp/pslog.sock"
const maxBufferSize = 1000

type LogEntry struct {
    TS      uint64 `json:"ts"`
    Level   string `json:"level"`
    Message string `json:"message"`
}

type Message struct {
	Type string `json:"type"`

	Topic   string `json:"topic,omitempty"`
	Log     *LogEntry `json:"log,omitempty"`
	Persist bool   `json:"persist,omitempty"`
	Port    uint16 `json:"port,omitempty"`
	IP      string `json:"ip,omitempty"`
}

type ConnMeta struct {
	Role    string
	Topic   string	
	Persist bool
}

type TopicState struct {
	Subscribers []net.Conn
	Buffer      []*LogEntry
	Publisher 	net.Conn
}

var (
	mu        sync.Mutex
	connState = make(map[net.Conn]ConnMeta)
	topics    = make(map[string]*TopicState)
)

func main() {
	os.Remove(socketPath)

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		brokerErr("STARTUP", err)
		return
	}
	defer listener.Close()

	brokerInfo("READY", fmt.Sprintf("Listening on %s", socketPath))

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go handleConnection(conn)
	}
}

// handleConnection
// Purpose:
//     Manages a single client lifecycle over the Unix socket.
// Workflow:
//     Continuously reads framed messages and dispatches them to handler.
// Arguments:
//     conn -> active client connection
// Returns:
//     None
// Behavior:
//     Blocking loop until connection breaks or read fails
// Failure Modes:
//     - connection reset
//     - malformed frame
// Notes:
//     Each connection runs in a dedicated goroutine
func handleConnection(conn net.Conn) {
	defer cleanupConnection(conn)
	defer conn.Close()

	for {
		msg, err := readMsg(conn)
		if err != nil {
			return
		}

		handleMessage(conn, msg)
	}
}

// handleMessage
// Purpose:
//     Core broker router for Pub/Sub/Log control messages.
// Workflow:
//     Matches message type and executes routing logic.
// Arguments:
//     conn -> source connection
//     msg  -> decoded broker message
// Returns:
//     None
// Behavior:
//     Updates global topic state and performs fanout if required
// Failure Modes:
//     - missing topic metadata
//     - invalid subscriber target
// Notes:
//     Holds global mutex during state mutation
func handleMessage(conn net.Conn, msg Message) {
	switch msg.Type {

	case "Ping":
		sendResponse(conn, map[string]string{"type": "Pong"})

	case "Pub":
		mu.Lock()

		t, ok := topics[msg.Topic]
		if !ok {
			t = &TopicState{}
			topics[msg.Topic] = t
		}

		connState[conn] = ConnMeta{
			Role:    "pub",
			Topic:   msg.Topic,
			Persist: msg.Persist,
		}

		if t.Publisher != nil {
			mu.Unlock()
			brokerErr("PUB_REJECT", fmt.Errorf("publisher already exists for topic"))
			return
		}

		t.Publisher = conn
		mu.Unlock()

		brokerInfo("PUBLISH", msg.Topic)

	case "Sub":
		if msg.IP == "" {
			brokerErr("SUB_FAIL", fmt.Errorf("received SUB with empty IP"))
			return
		}

		target := net.JoinHostPort(msg.IP, fmt.Sprintf("%d", msg.Port))
		
		tcpConn, err := net.Dial("tcp", target)
		if err != nil {
			brokerErr("DIAL_FAIL", fmt.Errorf("target %s: %v", target, err))
			return
		}

		mu.Lock()
		t, ok := topics[msg.Topic]
		if !ok {
			t = &TopicState{}
			topics[msg.Topic] = t
		}
		t.Subscribers = append(t.Subscribers, tcpConn)
		buffer := append([]*LogEntry{}, t.Buffer...)
		mu.Unlock()

		for _, log := range buffer {
			sendLog(tcpConn, msg.Topic, log)
		}

		brokerInfo("SUBSCRIBE", fmt.Sprintf("%s -> %s", msg.Topic, target))

	case "Scan":
		mu.Lock()
		var active []string

		for topic, t := range topics {
			if t.Publisher != nil {
				active = append(active, topic)
			}
		}

		mu.Unlock()

		sendResponse(conn, map[string]interface{}{
			"type":   "Scan",
			"topics": active,
		})

	case "Log":
		mu.Lock()
		meta, ok := connState[conn]
		if !ok {
			mu.Unlock()
			return
		}

		t := topics[meta.Topic]
		if t == nil {
			mu.Unlock()
			return
		}

		if meta.Persist {
			if len(t.Buffer) >= maxBufferSize {
				t.Buffer = t.Buffer[1:]
			}
			t.Buffer = append(t.Buffer, msg.Log)
		}

		var activeSubs []net.Conn
		for _, sub := range t.Subscribers {
			err := sendLog(sub, meta.Topic, msg.Log)
			if err == nil {
				activeSubs = append(activeSubs, sub)
			} else {
				sub.Close()
			}
		}
		t.Subscribers = activeSubs
		mu.Unlock()

	case "Close":
		return
	}
}

// cleanupConnection
// Purpose:
//     Removes all broker state associated with a disconnected client.
// Workflow:
//     Deletes connection metadata from global registry.
// Arguments:
//     conn -> disconnected client
// Returns:
//     None
// Behavior:
//     Ensures no stale pub/sub references remain
// Failure Modes:
//     None (safe no-op if missing entry)
// Notes:
//     Does NOT remove subscriber entries from topic list (handled elsewhere)
func cleanupConnection(conn net.Conn) {
	mu.Lock()
	defer mu.Unlock()
	meta, ok := connState[conn]
	if ok && meta.Role == "pub" {
		if t := topics[meta.Topic]; t != nil {
			if t.Publisher == conn {
				t.Publisher = nil
			}
		}
	}
	delete(connState, conn)
}

// extractIP
// Purpose:
//     Extracts IP portion from "ip:port" string.
// Workflow:
//     Splits address string and returns first segment.
// Arguments:
//     addr -> network address string
// Returns:
//     IP string
// Behavior:
//     Pure string transformation
// Failure Modes:
//     - malformed input may return full string or empty segment
// Notes:
//     No validation performed
func extractIP(addr string) string {
	parts := strings.Split(addr, ":")
	return parts[0]
}

// sendLog
// Purpose:
//     Sends a log message to a subscriber over a connection.
// Workflow:
//     Wraps the log entry into a Message and forwards it via sendResponse.
// Arguments:
//     conn  -> active subscriber connection
//     topic -> topic associated with the log
//     log   -> pointer to log payload
// Returns:
//     error if the write to the connection fails
// Behavior:
//     Fire-and-forget fanout helper (no retries or acknowledgements)
// Failure Modes:
//     - broken pipe (remote side closed)
//     - write on closed or invalid connection
// Notes:
//     Used only within the broker fanout loop
func sendLog(conn net.Conn, topic string, log *LogEntry) error {
    resp := Message{
        Type:  "Log",
        Topic: topic,
        Log:   log,
    }
    return sendResponse(conn, resp)
}

// sendResponse
// Purpose:
//     Serializes and sends a framed JSON message over a stream.
// Workflow:
//     JSON encode → prefix length → write to socket.
// Arguments:
//     conn -> active connection
//     v    -> message payload
// Returns:
//     error if serialization or write fails
// Behavior:
//     Ensures message framing over byte stream
// Failure Modes:
//     - write failure
//     - marshal failure
// Notes:
//     Core transport primitive of broker
func sendResponse(conn net.Conn, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))

	if _, err := conn.Write(lenBuf); err != nil {
		return err
	}
	if _, err := conn.Write(data); err != nil {
		return err
	}
	return nil
}

// readMsg
// Purpose:
//     Reads a single framed broker message from stream.
// Workflow:
//     Read length prefix → read payload → decode JSON.
// Arguments:
//     conn -> active connection
// Returns:
//     decoded Message + error
// Behavior:
//     Blocking read until full frame arrives
// Failure Modes:
//     - partial read
//     - malformed JSON
// Notes:
//     Assumes trusted framing protocol
func readMsg(conn net.Conn) (Message, error) {
	var msg Message

	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		return msg, err
	}

	length := binary.BigEndian.Uint32(lenBuf)

	data := make([]byte, length)
	if _, err := io.ReadFull(conn, data); err != nil {
		return msg, err
	}

	err := json.Unmarshal(data, &msg)
	return msg, err
}

/*
 * Prints formatted broker info logs.
 * Used for normal operational events like PUB/SUB/connection updates.
 * Output is color-coded for visibility in terminal.
 */
func brokerInfo(action string, details string) {
	fmt.Printf("\033[1;34m[BROKER]\033[0m %-10s | %s\n", action, details)
}

/*
 * Prints formatted broker error logs.
 * Used for failures such as connection issues or send/receive errors.
 * Output is color-coded in red for quick identification.
 */
func brokerErr(action string, err error) {
	fmt.Printf("\033[1;31m[ERROR]\033[0m %-10s | %v\n", action, err)
}
package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

const defaultControlPlaneURL = "http://127.0.0.1:8080"

type LogPayload struct {
	Timestamp uint64 `json:"ts"`
	Level     string `json:"level"`
	Message   string `json:"message"`
}

type Message struct {
	Type  string     `json:"type"`
	Topic string     `json:"topic,omitempty"`
	Log   LogPayload `json:"log,omitempty"`
}

type TopicHub struct {
	mu          sync.RWMutex
	subscribers map[string][]*net.UDPAddr
	activeTopics map[string]bool
}

var hub = TopicHub{
	subscribers:  make(map[string][]*net.UDPAddr),
	activeTopics: make(map[string]bool),
}

func (h *TopicHub) RegisterTopic(topic string, controlPlaneURL string) {
	h.mu.Lock()
	alreadyExists := h.activeTopics[topic]
	if !alreadyExists {
		h.activeTopics[topic] = true
	}
	h.mu.Unlock()

	if !alreadyExists {
		fmt.Printf("[BROKER] New active topic detected: '%s'. Announcing to Control Plane...\n", topic)
		go notifyTopicToControlPlane(controlPlaneURL, topic)
	}
}

func (h *TopicHub) AddSubscriber(topic string, addr *net.UDPAddr) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, existing := range h.subscribers[topic] {
		if existing.String() == addr.String() {
			return
		}
	}

	h.subscribers[topic] = append(h.subscribers[topic], addr)
	h.activeTopics[topic] = true
	fmt.Printf("[BROKER] Registered UDP subscriber %s for topic '%s'\n", addr.String(), topic)
}

func (h *TopicHub) GetSubscribers(topic string) []*net.UDPAddr {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.subscribers[topic]
}

func (h *TopicHub) GetActiveTopics() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	topics := make([]string, 0, len(h.activeTopics))
	for topic := range h.activeTopics {
		topics = append(topics, topic)
	}
	return topics
}

func registerWithControlPlane(controlPlaneURL string) {
	client := http.Client{Timeout: 2 * time.Second}
	endpoint := fmt.Sprintf("%s/api/v1/control/register-broker?id=broker-1&mgmt=http://127.0.0.1:60759", controlPlaneURL)

	resp, err := client.Post(endpoint, "application/json", nil)
	if err != nil {
		fmt.Printf("[BROKER WARN] Control Plane offline at %s. Operating in standalone mode.\n", controlPlaneURL)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		fmt.Println("[BROKER] Successfully registered with Spring Control Plane!")
	}
}

func notifyTopicToControlPlane(controlPlaneURL string, topic string) {
	client := http.Client{Timeout: 2 * time.Second}
	endpoint := fmt.Sprintf("%s/api/v1/control/topics/announce?topic=%s&broker_id=broker-1", controlPlaneURL, topic)

	resp, err := client.Post(endpoint, "application/json", nil)
	if err != nil {
		return
	}
	defer resp.Body.Close()
}

func startUnixServer(socketPath string, udpConn *net.UDPConn, controlPlaneURL string) {
	_ = os.Remove(socketPath)

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		fmt.Printf("[BROKER ERROR] Failed to listen on Unix socket %s: %v\n", socketPath, err)
		os.Exit(1)
	}
	defer listener.Close()
	defer os.Remove(socketPath)

	fmt.Printf("[BROKER] Listening for publishers on Unix Socket: %s\n", socketPath)

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go handlePublisherConn(conn, udpConn, controlPlaneURL)
	}
}

func handlePublisherConn(conn net.Conn, udpConn *net.UDPConn, controlPlaneURL string) {
	defer conn.Close()

	var currentTopic string

	for {
		var length uint32
		if err := binary.Read(conn, binary.BigEndian, &length); err != nil {
			if err != io.EOF && currentTopic != "" {
				fmt.Printf("[BROKER] Publisher disconnected from topic '%s'\n", currentTopic)
			}
			break
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(conn, payload); err != nil {
			break
		}

		var msg Message
		if err := json.Unmarshal(payload, &msg); err != nil {
			continue
		}

		if msg.Type == "Pub" {
			currentTopic = msg.Topic
			fmt.Printf("[BROKER] Publisher active for topic: '%s'\n", currentTopic)
			hub.RegisterTopic(currentTopic, controlPlaneURL)

		} else if msg.Type == "Log" {
			if currentTopic == "" {
				continue
			}

			outStr := fmt.Sprintf("[%s] %s\n", msg.Log.Level, msg.Log.Message)
			outBytes := []byte(outStr)

			subscribers := hub.GetSubscribers(currentTopic)
			for _, subAddr := range subscribers {
				_, _ = udpConn.WriteToUDP(outBytes, subAddr)
			}
		}
	}
}

func startManagementAPI() {
	http.HandleFunc("/api/v1/broker/register", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		topic := r.URL.Query().Get("topic")
		ip := r.URL.Query().Get("ip")
		portStr := r.URL.Query().Get("port")

		if topic == "" || ip == "" || portStr == "" {
			http.Error(w, "Missing query parameters", http.StatusBadRequest)
			return
		}

		udpAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%s", ip, portStr))
		if err != nil {
			http.Error(w, "Invalid UDP address", http.StatusBadRequest)
			return
		}

		hub.AddSubscriber(topic, udpAddr)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"SUCCESS"}`))
	})

	http.HandleFunc("/api/v1/broker/topics", func(w http.ResponseWriter, r *http.Request) {
		topics := hub.GetActiveTopics()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(topics)
	})

	fmt.Println("[BROKER] Management HTTP API listening on :60759")
	if err := http.ListenAndServe(":60759", nil); err != nil {
		fmt.Printf("[BROKER ERROR] HTTP API server failed: %v\n", err)
	}
}

func main() {
	socketPath := "/tmp/pslog.sock"
	controlPlaneURL := defaultControlPlaneURL

	udpConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("0.0.0.0"), Port: 0})
	if err != nil {
		fmt.Printf("[BROKER ERROR] Failed to open UDP socket: %v\n", err)
		os.Exit(1)
	}
	defer udpConn.Close()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\n[BROKER] Shutting down...")
		_ = os.Remove(socketPath)
		os.Exit(0)
	}()

	go startManagementAPI()

	go registerWithControlPlane(controlPlaneURL)

	startUnixServer(socketPath, udpConn, controlPlaneURL)
}
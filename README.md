<p align="center">
  <img src="resources/logo.png" alt="PSLog Logo" width="220">
  <br>
  <b>High-Performance Distributed Telemetry & Real-Time Log Streaming</b>
  <br><br>
  <img src="https://img.shields.io/badge/license-MIT-green.svg">
  <img src="https://img.shields.io/badge/CLI-C%2B%2B20-blue.svg">
  <img src="https://img.shields.io/badge/Broker-Go-00ADD8.svg">
  <img src="https://img.shields.io/badge/Control_Plane-Spring_Boot-6DB33F.svg">
</p>

---

## Overview

**PSLog** is a lightweight, hybrid distributed log streaming system designed for low-latency log transport and process telemetry. It splits responsibilities across three decoupled layers:

1. **C++ CLI Engine (`pslog`):** Ingests process stdout/stderr using framed JSON over IPC, and renders incoming log streams directly from UDP sockets.
2. **Go Networking Broker (`broker`):** An in-memory routing hub that accepts publisher streams via Unix domain sockets and fans them out to active subscribers over UDP datagrams.
3. **Spring Boot Control Plane (`control-plane`):** A REST management server managing global broker registration, subscriber handshake routing, and active topic discovery.

---

## Architecture

```text
                                 +-----------------------+
                                 |  Spring Control Plane |
                                 |   (:8080 HTTP REST)   |
                                 +-----------+-----------+
                                             |
            1. POST /subscribe               | 2. POST /api/v1/broker/register
            (?topic=X&port=54321)            |    (?topic=X&ip=127.0.0.1&port=54321)
                                             v
+-------------------+   4. Direct UDP Stream +-------------------+
|  C++ CLI          |<-------------------|    Go Broker      |
|  (pslog sub/pub)  |   (Zero-overhead   |   (:60759 / UDP)  |
+---------+---------+    log fanout)     +---------+---------+
          |                                        ^
          | 3. Framed JSON Logs                    |
          +----------------------------------------+
            (via Unix Socket: /tmp/pslog.sock)
```

### Component Breakdown

- **Unix Domain Socket (`/tmp/pslog.sock`):** Used for publisher-to-broker IPC with a 4-byte big-endian framing protocol to prevent log tearing.

- **UDP Data Plane:** Low-latency broadcast engine. Subscribers receive log lines directly from the broker via UDP datagrams.

- **HTTP Control Plane:** Spring Boot provides central routing metadata. If the broker is offline when a publisher starts, the C++ CLI automatically spawns the Go broker in the background.

---

## Prerequisites

Ensure your build environment has the following installed:

- **C++ Compiler:** GCC or Clang (C++17 or higher) & CMake `3.20+`
- **Go:** `1.20+`
- **Java:** JDK 17, 21, or 25
- **Build Tools:** `make`, `curl`

---

## Building & Configuration

### Interactive Build Script

PSLog includes an interactive build script that compiles all three components and writes your system configuration (`~/.pslog.conf`):

```bash
git clone https://github.com/Manohara-Ai/PSLog.git
cd PSLog
chmod +x build.sh
./build.sh
```

During setup, press **[ENTER]** to accept default settings or enter custom values:

- **Control Plane Address:** `127.0.0.1:8080`
- **Unix Socket Path:** `/tmp/pslog.sock`
- **Broker Management Port:** `60759`

### Adding to PATH (Optional)

Add the compiled binary directory to your active shell session:

```bash
export PATH="$(pwd)/bin:$PATH"
```

## Usage

### 1. Launch Control Plane

Start the Spring Boot Control Plane on `:8080`:

```bash
cd control-plane
./mvnw spring-boot:run
```

### 2. Publish Logs (`pslog pub`)

Run any executable command and stream its stdout/stderr to a topic stream. If the Go broker daemon isn't running, `pslog` auto-spawns it.

```bash
pslog pub <topic> "<command> [args...]"
```

*Example:*

```bash
pslog pub auth-service "ping -c 10 8.8.8.8"
```

### 3. Subscribe to Logs (`pslog sub`)

Open another terminal tab and listen to live log datagrams for a given topic:

```bash
pslog sub <topic>
```

*Example:*

```bash
pslog sub auth-service
```

### 4. Scan Active Topics (`pslog scan`)

Query the Spring Control Plane to list all active streaming channels:

```bash
pslog scan
```

*Example Output:*

```text
Active PSLog Topics:
  • auth-service
  • payment-gateway
```

## Configuration (`.pslog.conf`)

The system reads settings from `~/.pslog.conf` (or `./pslog.conf` as a fallback):

```ini
# PSLog System Configuration
CONTROL_PLANE_ADDR=127.0.0.1:8080
BROKER_ADDR=/tmp/pslog.sock
BROKER_MGMT_PORT=60759
```

## Repository Structure

```text
PSLog/
├── bin/                 # Compiled binaries (pslog, broker)
├── broker/              # Go networking engine (Unix socket & UDP fanout)
│   ├── go.mod
│   └── main.go
├── cli/                 # C++ CLI source engine
│   ├── CMakeLists.txt
│   └── src/             # main.cpp, protocol.hpp, pub/sub handlers
├── control-plane/       # Spring Boot HTTP REST management server
│   ├── pom.xml
│   └── src/
├── build.sh             # Interactive master build & configuration script
└── README.md
```

## License

This project is licensed under the [MIT License](LICENSE).
<p align="middle">
  <img src="resources/logo.png" alt="PSLog" width="240" style="vertical-align: middle; margin-right: 12px;">
  <span style="font-size: 22px; font-weight: 600;"></span><br>
  <span style="font-size: 14px;">
    A lightweight rust based service for streaming logs.
  </span><br>
  <img src="https://img.shields.io/badge/license-MIT-green">
  <img src="https://img.shields.io/badge/language-rust-orange">
  <img src="https://img.shields.io/badge/backend-go-blue">
</p>

---
# PSLog

A lightweight Rust-based service for streaming logs across processes via GO broker in real time.

---

## Overview

PSLog splits the work between two specialized components:

Control Plane (Unix Sockets): The CLI communicates with the Broker over /tmp/pslog.sock for high-speed, low-latency registration and log ingestion.

Data Plane (TCP): The Broker pushes logs to subscribers over TCP, enabling remote monitoring across the network.

<pre>

[ Process ]
    |
    | (stdout)
    v
[ Rust CLI (Publisher) ]
    |
    | (Unix Domain Socket)
    v
[ Go Broker ]
    |-------------------|
    |                   |
    v                   v
[ Subscriber A ]   [ Subscriber B ]
</pre>


---

## Installation

1. Prerequisites: Ensure you have the Rust toolchain and Go installed.

2. Build & Install
    ```
    git clone https://github.com/Manohara-Ai/PSLog
    cd pslog
    ```

3. Build the Rust CLI
    ```
    cargo build --release
    ```

4. Build the Go Broker
    ```
    go build -o pslog_server server.go
    ```

---

## Usage

PSLog provides three operational modes: publish logs from processes, subscribe to live streams, and inspect active topics.

### General Help

```
pslog --help
```

### Publish Logs

Run a process and stream its stdout logs to a topic:

```
pslog pub --exec <EXECUTABLE> [ARGS...] --topic <TOPIC> --persist
```

Options:
- --topic     Target topic name (optional)
- --port      Broker port (default: 60759)
- --qos       Quality of Service: high | auto | poor
- --auth      Authentication token (optional)
- --persist   Store logs in broker buffer
- --exec      Executable to run (required)
- ARGS...     Arguments passed to the executable

Example:
```
pslog pub --exec python3 -- script.py --persist
```

### Subscribe to Logs

Listen to a live log stream from a topic:

```
pslog sub --topic <TOPIC>
```

Options:
- --topic     Topic to subscribe to (required)
- --port      Subscriber port (default: 60759)
- --fos       Mode: sync | auto
- --format    Output format: text | json | pretty
- --ip        Override subscriber IP (optional)

Example:
```
pslog sub --topic script.py
```

### Scan Topics

List all active topics:

```
pslog scan
```
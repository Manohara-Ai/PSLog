/*
 * PSLOG CLIENT: CLI for log streaming system
 *
 * PURPOSE:
 * Provides Pub/Sub interface to stream process logs via PSLOG broker.
 *
 * MODES:
 * PUB  - Runs a process and streams stdout logs to broker
 * SUB  - Subscribes to a topic and receives live logs via TCP
 * SCAN - Queries broker for active topics
 *
 * FLOW:
 * PUB:
 * - Resolve topic
 * - Connect to broker (Unix socket)
 * - Spawn process
 * - Stream stdout logs line-by-line
 *
 * SUB:
 * - Start TCP listener on given port
 * - Register with broker (IP + port)
 * - Receive, format and print logs
 *
 * DESIGN:
 * - Unix socket for control plane
 * - TCP for data plane
 * - Line-based log streaming
 */

mod cli;

#[cfg(test)]
mod tests; 

use clap::Parser;
use cli::{Cli, Commands, WireMessage, LogEntry, LogLevel};

use std::os::unix::net::UnixStream;
use std::io::{Write, Read, BufRead};
use std::{thread, time::Duration};

use std::path::PathBuf;
use std::process::{Command, Stdio};

use std::net::{TcpListener, UdpSocket};

use colored::*;
use chrono::Local;

use crate::cli::LogFormat;

fn main() {
    let args = Cli::parse();

    match args.command {
        Commands::Pub { topic, port, qos, auth, persist, exec, child_args } => {
            let topic = resolve_topic(topic, &exec, &child_args);
            let mut stream = ensure_server();

            term_log("INFO", "PUB", &format!("Streaming topic: {}", topic.cyan()));

            let init = WireMessage::Pub {
                topic,
                port,
                qos,
                auth,
                persist,
            };
            send_msg(&mut stream, &init);

            let mut child = Command::new(exec)
                .args(child_args)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();

            let stdout = child.stdout.take().unwrap();
            let stderr = child.stderr.take().unwrap();

            let mut stream_out = stream.try_clone().unwrap();
            
            thread::spawn(move || {
            let reader = std::io::BufReader::new(stdout);

            for line in reader.lines() {
                let line = line.unwrap();

                let log = LogEntry {
                    ts: now(),
                    level: infer_level(&line, false),
                    message: line,
                };

                    send_msg(&mut stream_out, &WireMessage::Log { log });
                }
            });

            let reader = std::io::BufReader::new(stderr);
            for line in reader.lines() {
                let line = line.unwrap();

                let log = LogEntry {
                    ts: now(),
                    level: infer_level(&line, true),
                    message: line,
                };

                send_msg(&mut stream, &WireMessage::Log { log });
            }

            send_msg(&mut stream, &WireMessage::Close);
        }

        Commands::Sub { topic, port, fos, format, ip } => {
            term_log("DEBUG", "FORMAT", &format!("{:?}", format));
            let mut stream = ensure_server();

            let listener = TcpListener::bind(("0.0.0.0", port)).expect("failed to bind TCP listener");

            let target_ip = ip.unwrap_or_else(|| {
                get_local_ip().unwrap_or_else(|| "127.0.0.1".to_string())
            });

            term_log("SUCCESS", "SUB", &format!("Listening on {}:{}", target_ip.yellow(), port));
            term_log("INFO", "TOPIC", &topic.magenta().to_string());

            send_msg(&mut stream, &WireMessage::Sub {
                topic: topic.clone(),
                port,
                fos,
                ip: target_ip,
            });

            let (mut conn, _) = listener.accept().unwrap();
            
            loop {
                match read_msg(&mut conn) {
                    WireMessage::Log { log } => {

                        match format {
                            LogFormat::Json => {
                                println!("{}", serde_json::to_string(&log).unwrap());
                            }

                            LogFormat::Pretty => {
                                println!("{}", serde_json::to_string_pretty(&log).unwrap());
                            }

                            LogFormat::Text => {
                                let level_str = match log.level {
                                    LogLevel::ERROR => "ERROR".red(),
                                    LogLevel::WARN  => "WARN".yellow(),
                                    LogLevel::INFO  => "INFO".green(),
                                    LogLevel::DEBUG => "DEBUG".blue(),
                                    LogLevel::TRACE => "TRACE".white(),
                                };

                                println!(
                                    "{} [{}] {}",
                                    "│".bright_black(),
                                    level_str,
                                    log.message
                                );
                            }
                        }
                    }
                    _ => break,
                }
            }
        }

        Commands::Scan => {
            let mut stream = ensure_server();
            send_msg(&mut stream, &WireMessage::Scan { topics: None });

            let msg = read_msg(&mut stream);

            match msg {
                WireMessage::Scan { topics: Some(t) } => {
                    for topic in t {
                        println!("{}", topic);
                    }
                }
                _ => print!(""),
            }
        }
    }
}

/// Returns the current system timestamp in seconds since UNIX epoch.
///
/// Workflow:
/// 1. Fetch current system time.
/// 2. Compute duration since UNIX_EPOCH.
/// 3. Convert duration to seconds.
///
/// Arguments:
/// - None.
///
/// Returns:
/// - `u64` representing seconds since UNIX epoch.
///
/// Behavior:
/// - Uses system clock as time source.
/// - Provides coarse-grained (second-level) timestamp.
///
/// Panics:
/// - If system time is earlier than UNIX_EPOCH.
///
/// Notes:
/// - Relies on system clock accuracy.
/// - Suitable for logging and ordering events.
fn now() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Infers log level from a given log line and stream type.
///
/// Workflow:
/// 1. Check if input originated from stderr.
/// 2. Normalize line to lowercase.
/// 3. Match keywords to determine log level.
///
/// Arguments:
/// - `line`: Log line content.
/// - `is_stderr`: Indicates if line came from stderr.
///
/// Returns:
/// - `LogLevel` inferred from content or stream.
///
/// Behavior:
/// - stderr lines are always classified as ERROR.
/// - Keyword-based matching for "error", "warn", "debug".
/// - Defaults to INFO if no match found.
///
/// Panics:
/// - None.
///
/// Notes:
/// - Simple heuristic-based classification.
/// - May produce false positives/negatives for ambiguous text.
fn infer_level(line: &str, is_stderr: bool) -> LogLevel {
    if is_stderr {
        return LogLevel::ERROR;
    }

    let l = line.to_lowercase();

    if l.contains("error") {
        LogLevel::ERROR
    } else if l.contains("warn") {
        LogLevel::WARN
    } else if l.contains("debug") {
        LogLevel::DEBUG
    } else {
        LogLevel::INFO
    }
}

/// Resolves the machine’s primary local IP address.
///
/// Workflow:
/// 1. Binds a UDP socket to an ephemeral local port (`0.0.0.0:0`).
/// 2. "Connects" the UDP socket to a public endpoint (`8.8.8.8:80`).
///    - This does NOT send packets; it only forces OS routing resolution.
/// 3. Queries the socket’s local address after routing is determined.
/// 4. Extracts and returns the IPv4/IPv6 address portion.
///
/// Arguments:
/// - None.
///
/// Returns:
/// - `Some(String)` containing the local IP address if resolution succeeds.
/// - `None` if:
///     - socket binding fails
///     - route resolution fails
///     - local address cannot be retrieved
///
/// Behavior:
/// - Does not rely on external services beyond a dummy UDP route check.
/// - Does not require actual packet transmission.
/// - Typically returns LAN IP (e.g., `192.168.x.x`) or interface IP.
///
/// Panics:
/// - Never explicitly panics (all errors are handled via `Option`).
///
/// Notes:
/// - Uses a well-known trick (UDP "fake connect") to determine outbound interface.
/// - Works even without internet connectivity in many LAN setups.
/// - The chosen IP is the OS-selected egress interface for external traffic.
/// - Useful for service advertisement (e.g., broker → subscriber registration).
fn get_local_ip() -> Option<String> {
    let socket = UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("8.8.8.8:80").ok()?;
    socket.local_addr().ok().map(|addr| addr.ip().to_string())
}

/// Resolves the final topic name based on CLI input.
///
/// Workflow:
/// 1. Use explicit `--topic` if provided.
/// 2. If executable is Python, use script name (first arg).
/// 3. Otherwise, fallback to executable name.
///
/// Arguments:
/// - `topic`: Optional topic override from CLI.
/// - `exec`: Executable name/path.
/// - `child_args`: Arguments passed to the executable.
///
/// Returns:
/// - Final resolved topic string.
///
/// Behavior:
/// - Deterministic mapping from CLI input → topic name.
///
/// Panics:
/// - None.
///
/// Notes:
/// - Python detection is string-based (`starts_with("python")`).
fn resolve_topic(topic: Option<String>, exec: &str, child_args: &[String]) -> String {
    match topic {
        Some(t) => t,
        None => {
            if exec.starts_with("python") && !child_args.is_empty() {
                child_args[0].clone()
            } else {
                exec.to_string()
            }
        }
    }
}

/// Resolves the filesystem path to the `pslog_server` executable.
///
/// Workflow:
/// 1. Check `PSLOG_SERVER_PATH` env override.
/// 2. Check system install path.
/// 3. Check same directory as current executable.
/// 4. Fallback to `$PATH` resolution.
///
/// Arguments:
/// - None.
///
/// Returns:
/// - Path to the server binary.
///
/// Behavior:
/// - First-match resolution strategy.
///
/// Panics:
/// - None.
///
/// Notes:
/// - Only checks for existence, not executability or permissions.
fn resolve_server_path() -> PathBuf {
    if let Ok(path) = std::env::var("PSLOG_SERVER_PATH") {
        return PathBuf::from(path);
    }

    let system_path = PathBuf::from("/usr/local/lib/pslog/pslog_server");
    if system_path.exists() {
        return system_path;
    }

    if let Ok(exe_path) = std::env::current_exe() {
        if let Some(dir) = exe_path.parent() {
            let local = dir.join("pslog_server");
            if local.exists() {
                return local;
            }
        }
    }

    PathBuf::from("pslog_server")
}

/// Spawns the PSLog Go server as a background process.
///
/// Workflow:
/// 1. Resolve binary path.
/// 2. Spawn process using `Command::spawn()`.
///
/// Arguments:
/// - None.
///
/// Returns:
/// - None.
///
/// Behavior:
/// - Non-blocking (returns immediately).
/// - Does not track or manage the child process.
/// - Does not verify if a server is already running.
///
/// Panics:
/// - If the binary cannot be found or executed.
///
/// Notes:
/// - Intended to be called only after a failed connection attempt.
fn start_pslog_server() {
    let server_path = resolve_server_path();

    Command::new(server_path)
        .spawn()
        .expect("failed to start PSLog server");
}

/// Ensures a PSLog server is running and returns a connected stream.
///
/// Workflow:
/// 1. Attempt connection via `try_ping()`.
/// 2. If successful → reuse connection.
/// 3. Otherwise → spawn server.
/// 4. Retry connection with backoff.
///
/// Arguments:
/// - None.
///
/// Returns:
/// - Connected `UnixStream`.
///
/// Behavior:
/// - Retry loop: 20 attempts with 50ms delay (~1s total).
/// - Reuses verified connections only.
///
/// Panics:
/// - If server cannot be reached after retries.
///
/// Notes:
/// - Assumes socket path: `/tmp/pslog.sock`.
/// - Does not distinguish failure causes.
fn ensure_server() -> UnixStream {
    let socket = "/tmp/pslog.sock";

    if let Ok(stream) = try_ping(socket) {
        return stream;
    }

    start_pslog_server();

    for _ in 0..20 {
        if let Ok(stream) = try_ping(socket) {
            return stream;
        }
        thread::sleep(Duration::from_millis(50));
    }

    panic!("Failed to connect to PSLog server");
}

/// Attempts to connect to the server and validate it via Ping/Pong.
///
/// Workflow:
/// 1. Connect to Unix socket.
/// 2. Send `Ping` (length-prefixed).
/// 3. Read response frame.
/// 4. Validate `{ type: "Pong" }`.
///
/// Arguments:
/// - `socket`: Path to Unix socket.
///
/// Returns:
/// - `Ok(UnixStream)` if valid server responds.
/// - `Err(())` otherwise.
///
/// Behavior:
/// - Ensures liveness and protocol correctness.
/// - Rejects stale or invalid endpoints.
///
/// Panics:
/// - None.
///
/// Notes:
/// - Uses JSON framing with 4-byte big-endian length prefix.
/// - Uses dynamic JSON validation (`serde_json::Value`).
fn try_ping(socket: &str) -> Result<UnixStream, ()> {
    if let Ok(mut stream) = UnixStream::connect(socket) {
        let ping = serde_json::to_vec(&WireMessage::Ping).unwrap();
        let len = (ping.len() as u32).to_be_bytes();

        if stream.write_all(&len).is_err() { return Err(()); }
        if stream.write_all(&ping).is_err() { return Err(()); }

        let mut len_buf = [0u8; 4];
        if stream.read_exact(&mut len_buf).is_err() { return Err(()); }

        let resp_len = u32::from_be_bytes(len_buf);

        let mut data = vec![0u8; resp_len as usize];
        if stream.read_exact(&mut data).is_err() { return Err(()); }

        let msg: serde_json::Value = match serde_json::from_slice(&data) {
            Ok(v) => v,
            Err(_) => return Err(()),
        };

        if msg.get("type") == Some(&serde_json::Value::String("Pong".to_string())) {
            return Ok(stream);
        }
    }

    Err(())
}

/// Sends a framed `WireMessage` over a UnixStream.
///
/// Workflow:
/// 1. Serialize message to JSON.
/// 2. Prefix with 4-byte big-endian length.
/// 3. Write frame to stream.
///
/// Arguments:
/// - `stream`: Active UnixStream.
/// - `msg`: Message to send.
///
/// Returns:
/// - None.
///
/// Behavior:
/// - Guarantees message boundary preservation.
/// - Uses length-prefix framing.
///
/// Panics:
/// - If serialization fails.
/// - If write operation fails.
///
/// Notes:
/// - Required because Unix sockets are byte streams (not message-based).
fn send_msg(stream: &mut UnixStream, msg: &WireMessage) {
    let data = serde_json::to_vec(msg).unwrap();
    let len = (data.len() as u32).to_be_bytes();

    stream.write_all(&len).unwrap();
    stream.write_all(&data).unwrap();
}

/// Reads a single framed `WireMessage` from a reader.
///
/// Workflow:
/// 1. Read 4-byte length prefix.
/// 2. Read exact payload bytes.
/// 3. Deserialize JSON into `WireMessage`.
///
/// Arguments:
/// - `stream`: Active reader (UnixStream or TcpStream).
///
/// Returns:
/// - Parsed `WireMessage`.
///
/// Behavior:
/// - Blocks until full message is received.
/// - Assumes sender follows framing protocol.
///
/// Panics:
/// - If stream closes unexpectedly.
/// - If length prefix is invalid.
/// - If deserialization fails.
///
/// Notes:
/// - No size limits enforced (potential DoS risk).
/// - Should add max frame size for production.
fn read_msg<R: Read>(stream: &mut R) -> WireMessage {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).expect("Stream closed");

    let len = u32::from_be_bytes(len_buf);
    let mut data = vec![0u8; len as usize];

    stream.read_exact(&mut data).expect("Failed to read message body");

    serde_json::from_slice(&data).expect("Failed to deserialize message")
}

/// Formats and prints terminal logs with color-coded levels.
///
/// Workflow:
/// 1. Generate current timestamp (HH:MM:SS).
/// 2. Map log level to corresponding ANSI color/style.
/// 3. Format and print structured log line.
///
/// Arguments:
/// - `level`: Log severity ("INFO", "SUCCESS", "ERROR", etc.).
/// - `component`: Source module or system component name.
/// - `message`: Log content to display.
///
/// Returns:
/// - None.
///
/// Behavior:
/// - Prepends timestamp to each log entry.
/// - Applies color-coded labels based on level.
/// - Produces consistent, human-readable terminal output.
///
/// Panics:
/// - None.
///
/// Notes:
/// - Uses ANSI escape codes for styling.
/// - Intended for CLI/debug output only.
fn term_log(level: &str, component: &str, message: &str) {
    let time = Local::now().format("%H:%M:%S").to_string();
    let prefix = match level {
        "INFO" => " INFO ".on_blue().white().bold(),
        "SUCCESS" => " DONE ".on_green().white().bold(),
        "ERROR" => " FAIL ".on_red().white().bold(),
        _ => " LOG  ".on_white().black().bold(),
    };
    
    println!(
        "{} {} {} {}",
        time.dimmed(),
        prefix,
        component.bright_black().bold(),
        message
    );
}
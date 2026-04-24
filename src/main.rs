mod cli;

#[cfg(test)]
mod tests; 

use clap::Parser;
use cli::{Cli, Commands, WireMessage};

use std::os::unix::net::UnixStream;
use std::io::{Write, Read, BufRead};
use std::{thread, time::Duration};

use std::path::PathBuf;
use std::process::{Command, Stdio};

fn main() {
    let args = Cli::parse();

    match args.command {
        Commands::Pub { topic, port, qos, auth, persist, exec, child_args } => {
            let topic = resolve_topic(topic, &exec, &child_args);
            let mut stream = ensure_server();

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
                .spawn()
                .unwrap();

            let stdout = child.stdout.take().unwrap();
            let reader = std::io::BufReader::new(stdout);

            for line in reader.lines() {
                let line = line.unwrap();
                send_msg(&mut stream, &WireMessage::Log { log: line });
            }

            send_msg(&mut stream, &WireMessage::Close);
        }

        Commands::Sub { topic, port, fos, format } => {
            let mut stream = ensure_server();

            send_msg(&mut stream, &WireMessage::Sub { topic, port, fos });

            loop {
                if let WireMessage::Log { log } = read_msg(&mut stream) {
                    println!("{}", log);
                }
            }
        }

        Commands::Scan => {
            let mut stream = ensure_server();
            send_msg(&mut stream, &WireMessage::Scan);

            loop {
                println!("{:?}", read_msg(&mut stream));
            }
        }
    }
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

/// Reads a single framed `WireMessage` from a UnixStream.
///
/// Workflow:
/// 1. Read 4-byte length prefix.
/// 2. Read exact payload bytes.
/// 3. Deserialize JSON into `WireMessage`.
///
/// Arguments:
/// - `stream`: Active UnixStream.
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
fn read_msg(stream: &mut UnixStream) -> WireMessage {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).unwrap();

    let len = u32::from_be_bytes(len_buf);
    let mut data = vec![0u8; len as usize];

    stream.read_exact(&mut data).unwrap();

    serde_json::from_slice(&data).unwrap()
}
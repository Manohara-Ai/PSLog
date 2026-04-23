mod cli;

#[cfg(test)]
mod tests; 

use clap::Parser;
use cli::{Cli, Commands, WireMessage};

use std::os::unix::net::UnixStream;
use std::io::{Write, Read};
use std::{thread, time::Duration};

use std::path::PathBuf;
use std::process::Command;

fn main() {
    let args = Cli::parse();

    let wire_msg = match args.command {
        Commands::Pub { topic, port, qos, auth, persist, exec, child_args } => {
            let topic = resolve_topic(topic, &exec, &child_args);

            WireMessage::Pub {
                topic,
                port,
                qos,
                auth,
                persist,
                exec,
                child_args,
            }
        }

        Commands::Sub { topic, port, fos, format } => {
            WireMessage::Sub {
                topic,
                port,
                fos,
                format,
            }
        }

        Commands::Scan => WireMessage::Scan,
    };

    let mut stream = ensure_server();

    let data = serde_json::to_vec(&wire_msg).unwrap();
    let len = (data.len() as u32).to_be_bytes();

    stream.write_all(&len).unwrap();
    stream.write_all(&data).unwrap();

    let close = serde_json::to_vec(&WireMessage::Close).unwrap();
    let len = (close.len() as u32).to_be_bytes();

    stream.write_all(&len).unwrap();
    stream.write_all(&close).unwrap();
}

/// Resolves the final topic name based on CLI input.
/// 
/// Rules:
/// 1. If an explicit `--topic` is provided, use it.
/// 2. If no topic is provided and the executable is 'python', use the script name (first arg).
/// 3. Otherwise, default to the executable name itself.
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
/// Resolution order (first match wins):
/// 1. `PSLOG_SERVER_PATH` environment variable (explicit override for dev/testing).
/// 2. System install location: `/usr/local/lib/pslog/pslog_server`.
/// 3. Same directory as the current executable (useful for dev builds like `target/debug/`).
/// 4. Fallback to `pslog_server` (expects it to be available in `$PATH`).
///
/// This allows the CLI to work across:
/// - development builds
/// - local installs
/// - production deployments
///
/// Note: This function does NOT verify executability beyond existence.
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

/// Spawns the PSLog Go server as a child process.
///
/// Uses `resolve_server_path()` to locate the binary and starts it
/// in the background using `Command::spawn()`.
///
/// Behavior:
/// - Non-blocking: returns immediately after spawning.
/// - Does NOT check if a server is already running.
/// - Does NOT manage lifecycle (no handle retained).
///
/// Errors:
/// - Panics if the binary cannot be found or executed.
///
/// Intended to be called only after a failed connection attempt
/// (i.e., when no active server is detected).
fn start_pslog_server() {
    let server_path = resolve_server_path();

    Command::new(server_path)
        .spawn()
        .expect("failed to start PSLog server");
}

/// Ensures that a PSLog server is running and returns a connected UnixStream.
///
/// Workflow:
/// 1. Attempt to connect to an existing server via `try_ping()`.
/// 2. If successful → reuse that connection.
/// 3. If not → spawn a new server using `start_pslog_server()`.
/// 4. Retry connection (with small delays) until the server becomes available.
///
/// Retry policy:
/// - Up to 20 attempts
/// - 50ms delay between attempts (~1 second total wait)
///
/// Returns:
/// - A connected `UnixStream` ready for communication.
///
/// Panics:
/// - If the server cannot be reached after retries.
///
/// Note:
/// - Assumes server binds to `/tmp/pslog.sock`.
/// - Does not differentiate between failure modes (e.g., permission, crash).
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

/// Attempts to connect to the PSLog server and verify liveness via a Ping/Pong handshake.
///
/// Protocol:
/// - Sends a length-prefixed `WireMessage::Ping`.
/// - Expects a length-prefixed JSON response with `{ "type": "Pong" }`.
///
/// Steps:
/// 1. Connect to Unix socket.
/// 2. Send Ping message (framed with 4-byte length prefix).
/// 3. Read response length (4 bytes, big-endian).
/// 4. Read response payload.
/// 5. Deserialize and validate response type.
///
/// Returns:
/// - `Ok(UnixStream)` if a valid Pong is received (connection is reusable).
/// - `Err(())` if:
///     - connection fails
///     - write/read fails
///     - response is malformed
///     - response type is not "Pong"
///
/// Notes:
/// - This prevents false positives (e.g., stale sockets or wrong processes).
/// - Uses `serde_json::Value` for flexible validation; can be replaced with a strict type later.
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
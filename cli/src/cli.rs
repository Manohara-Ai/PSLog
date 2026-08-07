/*
 * PSLOG: A lightweight rust based service for streaming logs
 * OPERATIONAL MODES & ARGUMENTS:
 * 
 * 1. PUB (Publish) - Spawns a child process and streams its logs.
 * --topic   : Target topic name                              [Default: executable name or script name]
 * --port    : Target port number                             [Default: 60759]
 * --qos     : Quality of Service (high, auto, poor)          [Default: auto]
 * --auth    : Authentication token for restricted topics     [Default: None]
 * --persist : Boolean flag to keep logs in buffer memory     [Default: false]
 * --exec    : Path to the executable to run                  [Required]
 * --        : Separator for raw child process arguments      [Default: Empty List]
 * 
 * 2. SUB (Subscribe) - Listens to a live log streams.
 * --topic   : The specific topic to monitor                  [Required]
 * --fos     : Frequency of Service (sync, auto)              [Default: auto]
 * --format  : Output visualization (json, text, pretty)      [Default: text]
 * 
 * 3. SCAN - Discovery utility to list all active log topics. [No Arguments]
 */

use clap::{Parser, Subcommand, ValueEnum};
use serde::{Serialize, Deserialize};

#[derive(ValueEnum, Clone, Debug, Serialize, Deserialize)]
#[allow(non_camel_case_types)]
pub enum Qos {
    high,
    auto,
    poor,
}

#[derive(ValueEnum, Clone, Debug, Serialize, Deserialize)]
#[allow(non_camel_case_types)]
pub enum Fos {
    sync,
    auto,
}

#[derive(ValueEnum, Clone, Debug, Serialize, Deserialize)]
#[allow(non_camel_case_types)]
pub enum LogFormat {
    Json,
    Text,
    Pretty,
}

#[derive(Parser)]
#[command(name = "pslog", version = "1.0", about = "A lightweight rust based service for streaming logs")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Publish logs from a running process
    Pub {
        /// Target topic name
        #[arg(long)]
        topic: Option<String>,

        /// Port number for publisher
        #[arg(long, default_value_t = 60759)]
        port: u16,

        /// Quality of Service (high, auto, poor)
        #[arg(long, value_enum, default_value = "auto")]
        qos: Qos,

        /// Auth token (optional)
        #[arg(long)]
        auth: Option<String>,

        /// Persist logs in broker buffer
        #[arg(long)]
        persist: bool,

        /// Executable to run
        #[arg(long)]
        exec: String,

        /// Child process arguments
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        child_args: Vec<String>,
    },

    /// Subscribe to a log topic
    Sub {
        /// Topic to subscribe to
        #[arg(long)]
        topic: String,

        /// Port to listen on
        #[arg(long, default_value_t = 60759)]
        port: u16,

        /// Subscription mode
        #[arg(long, value_enum, default_value = "auto")]
        fos: Fos,

        /// Output format
        #[arg(long, value_enum, default_value = "text")]
        format: LogFormat,

        /// Override subscriber IP
        #[arg(long)]
        ip: Option<String>,
    },

    /// List active topics
    Scan,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum LogLevel {
    TRACE,
    DEBUG,
    INFO,
    WARN,
    ERROR,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LogEntry {
    pub ts: u64,
    pub level: LogLevel,
    pub message: String,
}

#[derive(Serialize, Debug, Deserialize)]
#[serde(tag = "type")]
pub enum WireMessage {
    Pub {
        topic: String,
        port: u16,
        qos: Qos,
        auth: Option<String>,
        persist: bool,
    },

    Sub {
        topic: String,
        port: u16,
        fos: Fos,
        ip: String,
    },

    Log {
        log: LogEntry,
    },

    Scan {
        topics: Option<Vec<String>>,
    },

    Ping,

    Close,
}
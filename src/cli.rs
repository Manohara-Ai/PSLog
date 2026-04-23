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
use serde::Serialize;

#[derive(ValueEnum, Clone, Debug, Serialize)]
#[allow(non_camel_case_types)]
pub enum Qos {
    high,
    auto,
    poor,
}

#[derive(ValueEnum, Clone, Debug, Serialize)]
#[allow(non_camel_case_types)]
pub enum Fos {
    sync,
    auto,
}

#[derive(ValueEnum, Clone, Debug, Serialize)]
#[allow(non_camel_case_types)]
pub enum LogFormat {
    json,
    text,
    pretty,
}

#[derive(Parser)]
#[command(name = "pslog", version = "1.0", about = "A lightweight rust based service for streaming logs")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    Pub {
        #[arg(long)]
        topic: Option<String>,

        #[arg(long, default_value_t = 60759)]
        port: u16,

        #[arg(long, value_enum, default_value = "auto")]
        qos: Qos,

        #[arg(long)]
        auth: Option<String>,

        #[arg(long)]
        persist: bool,

        #[arg(long)]
        exec: String,

        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        child_args: Vec<String>,
    },

    Sub {
        #[arg(long)]
        topic: String,

        #[arg(long, default_value_t = 60759)]
        port: u16,

        #[arg(long, value_enum, default_value = "auto")]
        fos: Fos,

        #[arg(long, value_enum, default_value = "text")]
        format: LogFormat,
    },

    Scan,
}

#[derive(Serialize)]
#[serde(tag = "type")]
pub enum WireMessage {
    Pub {
        topic: String,
        port: u16,
        qos: Qos,
        auth: Option<String>,
        persist: bool,
        exec: String,
        child_args: Vec<String>,
    },

    Sub {
        topic: String,
        port: u16,
        fos: Fos,
        format: LogFormat,
    },

    Scan,

    Ping,

    Close,
}
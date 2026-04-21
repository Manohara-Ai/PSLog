/*
 * PSLOG: A lightweight rust based service for streaming logs
 * OPERATIONAL MODES & ARGUMENTS:
 * 
 * 1. PUB (Publish) - Spawns a child process and streams its logs.
 * --topic   : Target topic name                              [Default: executable name or script name]
 * --qos     : Quality of Service (High, Auto, Poor)          [Default: Auto]
 * --auth    : Authentication token for restricted topics     [Default: None]
 * --persist : Boolean flag to keep logs in buffer memory     [Default: false]
 * --exec    : Path to the executable to run                  [Required]
 * --        : Separator for raw child process arguments      [Default: Empty List]
 * 
 * 2. SUB (Subscribe) - Listens to a live log streams.
 * --topic   : The specific topic to monitor                  [Required]
 * --fos     : Frequency of Service (Sync, Auto)              [Default: Auto]
 * --format  : Output visualization (Json, Text, Pretty)      [Default: Text]
 * 
 * 3. SCAN - Discovery utility to list all active log topics. [No Arguments]
 */

use clap::{Parser, Subcommand, ValueEnum};

#[derive(ValueEnum, Clone, Debug)]
#[allow(non_camel_case_types)]
pub enum Qos {
    High,
    Auto,
    Poor,
}

#[derive(ValueEnum, Clone, Debug)]
#[allow(non_camel_case_types)]
pub enum Fos {
    Sync,
    Auto,
}

#[derive(ValueEnum, Clone, Debug)]
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
    Pub {
        #[arg(long)]
        topic: Option<String>,

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

        #[arg(long, value_enum, default_value = "auto")]
        fos: Fos,

        #[arg(long, value_enum, default_value = "text")]
        format: LogFormat,
    },

    Scan,
}
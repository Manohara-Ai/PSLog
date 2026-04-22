mod cli;

#[cfg(test)]
mod tests; 

use clap::Parser;
use cli::{Cli, Commands};
use std::os::raw::c_int;

#[link(name = "pslog")]
unsafe extern "C" {
    unsafe fn startGoServer(port: c_int);
}

fn main() {
    let args = Cli::parse();

    let data_port: c_int = 9540;
    unsafe {
        startGoServer(data_port);
    }

    match args.command {
        Commands::Pub { topic, qos, auth, persist, exec, child_args } => {
            // TODO
        }
        Commands::Sub { topic, .. } => {
            // TODO
        }
        Commands::Scan => {
            // TODO
        }
    }
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
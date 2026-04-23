mod cli;

#[cfg(test)]
mod tests; 

use clap::Parser;
use cli::{Cli, Commands, WireMessage};
use std::os::raw::c_int;

/*
#[link(name = "pslog")]
unsafe extern "C" {
    unsafe fn startGoServer(port: c_int);
}
*/

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

    let json = serde_json::to_string(&wire_msg).unwrap();
    println!("{}", json);
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
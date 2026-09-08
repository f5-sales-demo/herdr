use crate::api::client::ApiClient;
use crate::api::schema::{
    ExecutionCommand, ExecutionListParams, ExecutionStartParams, ExecutionTarget,
    ExecutionWaitParams, Method, Request,
};

pub(super) fn run(args: &[String]) -> std::io::Result<i32> {
    let Some(action) = args.first().map(String::as_str) else {
        return usage();
    };
    let method = match action {
        "get" | "cancel" if args.len() == 2 => {
            let target = ExecutionTarget {
                execution_id: args[1].clone(),
            };
            if action == "get" {
                Method::ExecutionGet(target)
            } else {
                Method::ExecutionCancel(target)
            }
        }
        "list" => {
            let since_revision = match args.get(1..).unwrap_or_default() {
                [] => 0,
                [flag, value] if flag == "--since" => value
                    .parse()
                    .map_err(|_| std::io::Error::other("--since must be an integer"))?,
                _ => return usage(),
            };
            Method::ExecutionList(ExecutionListParams { since_revision })
        }
        "wait" if args.len() == 2 => Method::ExecutionWait(ExecutionWaitParams {
            after_revision: args[1]
                .parse()
                .map_err(|_| std::io::Error::other("revision must be an integer"))?,
            timeout_ms: 30_000,
        }),
        "start" => parse_start(&args[1..])?,
        "help" | "--help" | "-h" => return usage_ok(),
        _ => return usage(),
    };
    let response = ApiClient::local()
        .request_value(&Request {
            id: format!("cli:execution:{action}"),
            method,
        })
        .map_err(std::io::Error::other)?;
    println!(
        "{}",
        serde_json::to_string_pretty(&response).map_err(std::io::Error::other)?
    );
    Ok(if response.get("error").is_some() {
        1
    } else {
        0
    })
}

fn parse_start(args: &[String]) -> std::io::Result<Method> {
    let Some(id) = args.first() else {
        return Err(std::io::Error::other("execution id required"));
    };
    let mut cwd = None;
    let mut shell = None;
    let mut index = 1;
    while index < args.len() {
        match args[index].as_str() {
            "--cwd" if index + 1 < args.len() => {
                cwd = Some(args[index + 1].clone());
                index += 2;
            }
            "--shell" if index + 1 < args.len() => {
                shell = Some(args[index + 1].clone());
                index += 2;
            }
            "--" => {
                index += 1;
                break;
            }
            _ => {
                return Err(std::io::Error::other(
                    "expected --cwd PATH [--shell bash|zsh] -- COMMAND",
                ))
            }
        }
    }
    let cwd = cwd.ok_or_else(|| std::io::Error::other("--cwd is required"))?;
    let tail = args[index..].to_vec();
    let command = if let Some(shell) = shell {
        if tail.len() != 1 {
            return Err(std::io::Error::other(
                "shell mode requires exactly one command-text argument",
            ));
        }
        ExecutionCommand::Shell {
            shell,
            text: tail[0].clone(),
        }
    } else {
        ExecutionCommand::Argv { argv: tail }
    };
    Ok(Method::ExecutionStart(ExecutionStartParams {
        execution_id: id.clone(),
        cwd,
        workspace_id: None,
        label: None,
        command,
    }))
}
fn usage() -> std::io::Result<i32> {
    print_usage();
    Ok(2)
}
fn usage_ok() -> std::io::Result<i32> {
    print_usage();
    Ok(0)
}
fn print_usage() {
    eprintln!("herdr execution commands:\n  herdr execution start <id> --cwd <absolute-path> [--shell bash|zsh] -- <argv...|command-text>\n  herdr execution get <id>\n  herdr execution list [--since <revision>]\n  herdr execution wait <after-revision>\n  herdr execution cancel <id>");
}

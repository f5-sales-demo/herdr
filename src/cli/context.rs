use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::api::schema::{Method, PaneTarget, Request};

const FORMAT_VERSION: u32 = 1;
const DEFAULT_TTL: Duration = Duration::from_secs(60);
const MAX_TTL: Duration = Duration::from_secs(300);
const HANDOFF_DIR: &str = "worker-context-handoffs";
const CONTEXT_KEYS: &[&str] = &[
    crate::HERDR_ENV_VAR,
    crate::api::SOCKET_PATH_ENV_VAR,
    crate::integration::HERDR_WORKSPACE_ID_ENV_VAR,
    crate::integration::HERDR_TAB_ID_ENV_VAR,
    crate::integration::HERDR_PANE_ID_ENV_VAR,
    "HERDR_BIN_PATH",
];

#[derive(Debug, Serialize, Deserialize)]
struct Handoff {
    version: u32,
    expires_at_ms: u128,
    environment: BTreeMap<String, String>,
}

pub(super) fn run_context_command(args: &[String]) -> io::Result<i32> {
    match args.first().map(String::as_str) {
        Some("issue") => issue(&args[1..]),
        Some("exec") => execute(&args[1..]),
        Some("help" | "--help" | "-h") => {
            print_help();
            Ok(0)
        }
        _ => {
            print_help();
            Ok(2)
        }
    }
}

fn issue(args: &[String]) -> io::Result<i32> {
    let ttl = parse_issue_args(args)?;
    let handoff = Handoff {
        version: FORMAT_VERSION,
        expires_at_ms: now_ms()?.saturating_add(ttl.as_millis()),
        environment: pane_environment()?,
    };
    let token = uuid::Uuid::new_v4().simple().to_string();
    write_handoff(&token, &handoff)?;
    println!("{token}");
    Ok(0)
}

fn execute(args: &[String]) -> io::Result<i32> {
    let Some((token, rest)) = args.split_first() else {
        print_help();
        return Ok(2);
    };
    let Some(rest) = rest.strip_prefix(&["--".to_string()]) else {
        eprintln!("usage: herdr context exec <capability> -- <command> [args...]");
        return Ok(2);
    };
    let Some((program, program_args)) = rest.split_first() else {
        eprintln!("usage: herdr context exec <capability> -- <command> [args...]");
        return Ok(2);
    };
    let handoff = consume_handoff(token)?;
    ensure_live_pane(&handoff.environment)?;
    let mut child = Command::new(program);
    child.args(program_args);
    for key in CONTEXT_KEYS {
        child.env_remove(key);
    }
    child.envs(&handoff.environment);
    Ok(child.status()?.code().unwrap_or(1))
}

fn parse_issue_args(args: &[String]) -> io::Result<Duration> {
    match args {
        [] => Ok(DEFAULT_TTL),
        [flag, value] if flag == "--ttl-seconds" => {
            let seconds = value.parse::<u64>().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "--ttl-seconds must be an integer",
                )
            })?;
            let ttl = Duration::from_secs(seconds);
            if ttl.is_zero() || ttl > MAX_TTL {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "--ttl-seconds must be between 1 and 300",
                ));
            }
            Ok(ttl)
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: herdr context issue [--ttl-seconds 1..300]",
        )),
    }
}

fn pane_environment() -> io::Result<BTreeMap<String, String>> {
    if std::env::var(crate::HERDR_ENV_VAR).ok().as_deref() != Some(crate::HERDR_ENV_VALUE) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "a worker context capability can only be issued by a Herdr-managed pane",
        ));
    }
    CONTEXT_KEYS
        .iter()
        .map(|key| {
            std::env::var(key)
                .map(|value| ((*key).to_string(), value))
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        format!("a managed pane is missing required {key}"),
                    )
                })
        })
        .collect()
}

fn handoff_dir() -> PathBuf {
    crate::config::state_dir().join(HANDOFF_DIR)
}

fn handoff_path(token: &str) -> io::Result<PathBuf> {
    let token = uuid::Uuid::parse_str(token)
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid worker context capability",
            )
        })?
        .simple()
        .to_string();
    Ok(handoff_dir().join(token))
}

fn write_handoff(token: &str, handoff: &Handoff) -> io::Result<()> {
    let dir = handoff_dir();
    fs::create_dir_all(&dir)?;
    restrict_dir(&dir)?;
    let bytes = serde_json::to_vec(handoff).map_err(io::Error::other)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(handoff_path(token)?)?;
    use std::io::Write;
    file.write_all(&bytes)?;
    file.sync_all()
}

fn consume_handoff(token: &str) -> io::Result<Handoff> {
    let path = handoff_path(token)?;
    let claimed = path.with_extension(format!("claimed-{}", uuid::Uuid::new_v4().simple()));
    fs::rename(&path, &claimed).map_err(|err| {
        if err.kind() == io::ErrorKind::NotFound {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "worker context capability is unknown or has already been consumed",
            )
        } else {
            err
        }
    })?;
    let result = (|| {
        let metadata = fs::symlink_metadata(&claimed)?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "invalid worker context capability",
            ));
        }
        let handoff: Handoff = serde_json::from_slice(&fs::read(&claimed)?).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid worker context capability",
            )
        })?;
        if handoff.version != FORMAT_VERSION || handoff.expires_at_ms < now_ms()? {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "worker context capability has expired",
            ));
        }
        Ok(handoff)
    })();
    let _ = fs::remove_file(claimed);
    result
}

fn ensure_live_pane(environment: &BTreeMap<String, String>) -> io::Result<()> {
    let pane_id = environment
        .get(crate::integration::HERDR_PANE_ID_ENV_VAR)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "worker context has no pane"))?;
    let socket_path = environment
        .get(crate::api::SOCKET_PATH_ENV_VAR)
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "worker context has no socket")
        })?;
    let previous = std::env::var_os(crate::api::SOCKET_PATH_ENV_VAR);
    std::env::set_var(crate::api::SOCKET_PATH_ENV_VAR, socket_path);
    let response = super::send_request(&Request {
        id: "cli:context:validate".into(),
        method: Method::PaneGet(PaneTarget {
            pane_id: pane_id.clone(),
        }),
    });
    match previous {
        Some(value) => std::env::set_var(crate::api::SOCKET_PATH_ENV_VAR, value),
        None => std::env::remove_var(crate::api::SOCKET_PATH_ENV_VAR),
    }
    if response?.get("error").is_some() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "worker context pane is no longer live",
        ));
    }
    Ok(())
}

fn now_ms() -> io::Result<u128> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis())
        .map_err(io::Error::other)
}

fn restrict_dir(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn print_help() {
    eprintln!("usage:\n  herdr context issue [--ttl-seconds 1..300]\n  herdr context exec <capability> -- <command> [args...]\n\nIssue from a managed pane, then pass the one-time capability only to the external worker launcher.");
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn capability_paths_reject_traversal_and_normalize_uuid() {
        assert!(handoff_path("../anything").is_err());
        assert_eq!(
            handoff_path("550e8400-e29b-41d4-a716-446655440000")
                .unwrap()
                .file_name()
                .unwrap(),
            "550e8400e29b41d4a716446655440000"
        );
    }
    #[test]
    fn issue_args_are_bounded() {
        assert_eq!(parse_issue_args(&[]).unwrap(), DEFAULT_TTL);
        assert_eq!(
            parse_issue_args(&["--ttl-seconds".into(), "300".into()]).unwrap(),
            MAX_TTL
        );
        assert!(parse_issue_args(&["--ttl-seconds".into(), "0".into()]).is_err());
        assert!(parse_issue_args(&["--ttl-seconds".into(), "301".into()]).is_err());
    }
}

use std::io::{self, Read};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::api::client::{ApiClient, ConnectionTarget};
use crate::api::schema::{
    Method, Request, ResponseResult, WorkerContextClaimParams, WorkerContextIssueParams,
    WorkerContextLeaseParams,
};

const PAYLOAD_VERSION: u32 = 1;
const MAX_STDIN_BYTES: u64 = 16 * 1024;

#[derive(Debug, Serialize, Deserialize)]
struct PairingPayload {
    version: u32,
    endpoint: String,
    token: String,
}

#[derive(Debug, Serialize)]
struct LeasePayload {
    version: u32,
    endpoint: String,
    lease: String,
    pane: crate::api::schema::WorkerContextPane,
}

pub(super) fn run_context_command(args: &[String]) -> io::Result<i32> {
    match args.first().map(String::as_str) {
        Some("issue") if args.len() == 1 => issue(),
        Some("claim") => claim(&args[1..]),
        Some("resolve") => resolve(&args[1..]),
        Some("revoke") => revoke(&args[1..]),
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

fn issue() -> io::Result<i32> {
    let pane_id = required_env(crate::integration::HERDR_PANE_ID_ENV_VAR)?;
    let context_capability = required_env(crate::worker_context::CAPABILITY_ENV_VAR)?;
    let client = ApiClient::local();
    let response = client
        .request(Request {
            id: "cli:worker-context:issue".into(),
            method: Method::WorkerContextIssue(WorkerContextIssueParams {
                pane_id,
                context_capability,
            }),
        })
        .map_err(api_error)?;
    let ResponseResult::WorkerContextPairing {
        pairing_token,
        pane: _,
    } = response.result
    else {
        return Err(unexpected_response());
    };
    println!(
        "{}",
        serde_json::to_string(&PairingPayload {
            version: PAYLOAD_VERSION,
            endpoint: client.socket_path().display().to_string(),
            token: pairing_token,
        })?
    );
    Ok(0)
}

fn claim(args: &[String]) -> io::Result<i32> {
    let consumer_id = parse_consumer_id(args)?;
    let payload: PairingPayload = serde_json::from_str(&read_stdin()?)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid pairing payload"))?;
    if payload.version != PAYLOAD_VERSION || payload.endpoint.is_empty() || payload.token.is_empty()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unsupported or incomplete pairing payload",
        ));
    }
    let client = ApiClient::for_target(ConnectionTarget::SocketPath(PathBuf::from(
        &payload.endpoint,
    )));
    let response = client
        .request(Request {
            id: "cli:worker-context:claim".into(),
            method: Method::WorkerContextClaim(WorkerContextClaimParams {
                pairing_token: payload.token,
                consumer_id,
            }),
        })
        .map_err(api_error)?;
    let ResponseResult::WorkerContextLease { lease, pane } = response.result else {
        return Err(unexpected_response());
    };
    println!(
        "{}",
        serde_json::to_string(&LeasePayload {
            version: PAYLOAD_VERSION,
            endpoint: payload.endpoint,
            lease,
            pane,
        })?
    );
    Ok(0)
}

fn resolve(args: &[String]) -> io::Result<i32> {
    let (endpoint, consumer_id) = parse_lease_args(args)?;
    let lease = read_secret_stdin()?;
    let response = client_for_endpoint(endpoint)
        .request(Request {
            id: "cli:worker-context:resolve".into(),
            method: Method::WorkerContextResolve(WorkerContextLeaseParams { lease, consumer_id }),
        })
        .map_err(api_error)?;
    let ResponseResult::WorkerContext { context } = response.result else {
        return Err(unexpected_response());
    };
    println!("{}", serde_json::to_string(&context)?);
    Ok(0)
}

fn revoke(args: &[String]) -> io::Result<i32> {
    let (endpoint, consumer_id) = parse_lease_args(args)?;
    let lease = read_secret_stdin()?;
    let response = client_for_endpoint(endpoint)
        .request(Request {
            id: "cli:worker-context:revoke".into(),
            method: Method::WorkerContextRevoke(WorkerContextLeaseParams { lease, consumer_id }),
        })
        .map_err(api_error)?;
    let ResponseResult::WorkerContextRevoked { revoked } = response.result else {
        return Err(unexpected_response());
    };
    println!("{}", serde_json::json!({ "revoked": revoked }));
    Ok(0)
}

fn parse_consumer_id(args: &[String]) -> io::Result<String> {
    match args {
        [flag, value] if flag == "--consumer-id" && !value.trim().is_empty() => Ok(value.clone()),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected --consumer-id <stable-id>",
        )),
    }
}

fn parse_lease_args(args: &[String]) -> io::Result<(PathBuf, String)> {
    let mut endpoint = None;
    let mut consumer_id = None;
    let mut index = 0;
    while index < args.len() {
        let value = args.get(index + 1).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "missing context option value")
        })?;
        match args[index].as_str() {
            "--endpoint" => endpoint = Some(PathBuf::from(value)),
            "--consumer-id" => consumer_id = Some(value.clone()),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "unknown context option",
                ));
            }
        }
        index += 2;
    }
    let endpoint = endpoint
        .filter(|path| !path.as_os_str().is_empty())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "--endpoint is required"))?;
    let consumer_id = consumer_id
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "--consumer-id is required"))?;
    Ok((endpoint, consumer_id))
}

fn client_for_endpoint(endpoint: PathBuf) -> ApiClient {
    ApiClient::for_target(ConnectionTarget::SocketPath(endpoint))
}

fn read_secret_stdin() -> io::Result<String> {
    let value = read_stdin()?;
    let value = value.trim();
    if value.is_empty() || value.chars().any(char::is_whitespace) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected one lease secret on stdin",
        ));
    }
    Ok(value.to_owned())
}

fn read_stdin() -> io::Result<String> {
    let mut value = String::new();
    io::stdin()
        .take(MAX_STDIN_BYTES + 1)
        .read_to_string(&mut value)?;
    if value.len() as u64 > MAX_STDIN_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "context input exceeds 16 KiB",
        ));
    }
    Ok(value)
}

fn required_env(name: &str) -> io::Result<String> {
    std::env::var(name).map_err(|_| {
        io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("context issuance requires a Herdr-managed pane with {name}"),
        )
    })
}

fn api_error(error: crate::api::client::ApiClientError) -> io::Error {
    io::Error::other(error.to_string())
}

fn unexpected_response() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "unexpected worker context response",
    )
}

fn print_help() {
    eprintln!(
        "usage:\n  herdr context issue\n  herdr context claim --consumer-id <id> < pairing.json\n  herdr context resolve --endpoint <path> --consumer-id <id> < lease\n  herdr context revoke --endpoint <path> --consumer-id <id> < lease"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lease_options_require_endpoint_and_consumer() {
        assert!(parse_lease_args(&[]).is_err());
        assert!(parse_lease_args(&["--endpoint".into(), "/tmp/herdr.sock".into()]).is_err());
        assert_eq!(
            parse_lease_args(&[
                "--endpoint".into(),
                "/tmp/herdr.sock".into(),
                "--consumer-id".into(),
                "window-a".into(),
            ])
            .expect("valid args"),
            (PathBuf::from("/tmp/herdr.sock"), "window-a".into())
        );
    }
}

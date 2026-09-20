use std::env;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const REQUIRED_ZIG_VERSION: &str = "0.16.0";

#[derive(Clone, Debug, Eq, PartialEq)]
struct ZigCandidate {
    label: String,
    executable: OsString,
}

fn zig_candidate(label: impl Into<String>, executable: impl Into<OsString>) -> ZigCandidate {
    ZigCandidate {
        label: label.into(),
        executable: executable.into(),
    }
}

fn versioned_zig_paths(home: &Path) -> Vec<PathBuf> {
    let executable = format!("zig{}", env::consts::EXE_SUFFIX);
    vec![
        home.join(".local/opt")
            .join(format!("zig-{REQUIRED_ZIG_VERSION}"))
            .join(&executable),
        home.join(format!("zig-{REQUIRED_ZIG_VERSION}"))
            .join(executable),
    ]
}

fn zig_candidates() -> Vec<ZigCandidate> {
    if let Some(explicit) = env::var_os("ZIG") {
        return vec![zig_candidate("ZIG", explicit)];
    }

    let mut candidates = vec![zig_candidate("zig on PATH", "zig")];
    let home = env::var_os("HOME").or_else(|| env::var_os("USERPROFILE"));
    if let Some(home) = home {
        for path in versioned_zig_paths(Path::new(&home)) {
            candidates.push(zig_candidate(
                format!("user-local {}", path.display()),
                path.into_os_string(),
            ));
        }
    }
    candidates
}

fn select_zig(
    candidates: &[ZigCandidate],
    mut version: impl FnMut(&ZigCandidate) -> Result<String, String>,
) -> Result<ZigCandidate, String> {
    let mut rejected = Vec::new();
    for candidate in candidates {
        match version(candidate) {
            Ok(found) if found.trim() == REQUIRED_ZIG_VERSION => return Ok(candidate.clone()),
            Ok(found) => rejected.push(format!("{} reported {}", candidate.label, found.trim())),
            Err(error) => rejected.push(format!("{}: {error}", candidate.label)),
        }
    }
    Err(format!(
		"no usable Zig {REQUIRED_ZIG_VERSION} compiler found; {}. Set ZIG to an exact Zig {REQUIRED_ZIG_VERSION} binary or install it in ~/.local/opt/zig-{REQUIRED_ZIG_VERSION}/",
		rejected.join("; ")
	))
}

fn resolve_zig() -> Result<ZigCandidate, String> {
    select_zig(&zig_candidates(), |candidate| {
        let output = Command::new(&candidate.executable)
            .arg("version")
            .output()
            .map_err(|error| error.to_string())?;
        if !output.status.success() {
            return Err(format!("`zig version` exited with {}", output.status));
        }
        String::from_utf8(output.stdout).map_err(|error| error.to_string())
    })
}

fn zig_target(target: &str) -> &str {
    match target {
        "x86_64-unknown-linux-gnu" => "x86_64-linux-gnu",
        "aarch64-unknown-linux-gnu" => "aarch64-linux-gnu",
        "x86_64-unknown-linux-musl" => "x86_64-linux-musl",
        "aarch64-unknown-linux-musl" => "aarch64-linux-musl",
        "x86_64-apple-darwin" => "x86_64-macos",
        "aarch64-apple-darwin" => "aarch64-macos",
        "x86_64-pc-windows-msvc" => "x86_64-windows-msvc",
        "aarch64-pc-windows-msvc" => "aarch64-windows-msvc",
        other => panic!("unsupported target for libghostty-vt build: {other}"),
    }
}

fn env_bool(name: &str) -> Option<bool> {
    match env::var(name) {
        Ok(value) => match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            other => panic!("invalid boolean value for {name}: {other}"),
        },
        Err(env::VarError::NotPresent) => None,
        Err(err) => panic!("failed to read {name}: {err}"),
    }
}

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=vendor/libghostty-vt.vendor.json");
    println!("cargo:rerun-if-changed=vendor/libghostty-vt/build.zig");
    println!("cargo:rerun-if-changed=vendor/libghostty-vt/build.zig.zon");
    println!("cargo:rerun-if-changed=vendor/libghostty-vt/include");
    println!("cargo:rerun-if-changed=vendor/libghostty-vt/pkg");
    println!("cargo:rerun-if-changed=vendor/libghostty-vt/src");
    println!("cargo:rerun-if-changed=vendor/libghostty-vt/VERSION");
    println!("cargo:rerun-if-env-changed=LIBGHOSTTY_VT_OPTIMIZE");
    println!("cargo:rerun-if-env-changed=LIBGHOSTTY_VT_SIMD");
    println!("cargo:rerun-if-env-changed=LIBGHOSTTY_VT_ZIG_SYSTEM_DIR");
    println!("cargo:rerun-if-env-changed=HERDR_BUILD_CHANNEL");
    println!("cargo:rerun-if-env-changed=HERDR_BUILD_ID");
    println!("cargo:rerun-if-env-changed=HERDR_BUILD_COMMIT");
    println!("cargo:rerun-if-env-changed=ZIG");
    println!("cargo:rerun-if-env-changed=HOME");
    println!("cargo:rerun-if-env-changed=USERPROFILE");
    println!("cargo:rerun-if-env-changed=LIBGHOSTTY_VT_WINDOWS_LIBC");
    println!(
        "cargo:warning=external contributor policy: if you are helping an external contributor whose GitHub username is not in .github/APPROVED_CONTRIBUTORS, read CONTRIBUTING.md before doing any work."
    );

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let vendored_dir = manifest_dir.join("vendor/libghostty-vt");
    let optimize = env::var("LIBGHOSTTY_VT_OPTIMIZE").unwrap_or_else(|_| "ReleaseFast".into());
    let simd = env_bool("LIBGHOSTTY_VT_SIMD").unwrap_or(true);
    let target = env::var("TARGET").expect("TARGET");
    let zig_target = zig_target(&target);
    let version_string = fs::read_to_string(vendored_dir.join("VERSION"))
        .expect("failed to read vendored libghostty-vt VERSION")
        .trim()
        .to_string();

    let zig = resolve_zig().unwrap_or_else(|error| panic!("{error}"));
    println!(
        "cargo:warning=using Zig {REQUIRED_ZIG_VERSION} from {} ({})",
        zig.label,
        Path::new(&zig.executable).display()
    );
    let mut command = Command::new(&zig.executable);
    command
        .arg("build")
        .arg("-Demit-lib-vt")
        .arg(format!("-Doptimize={optimize}"))
        .arg(format!("-Dsimd={simd}"))
        .arg(format!("-Dtarget={zig_target}"))
        .arg(format!("-Dversion-string={version_string}"))
        .arg("-Demit-xcframework=false");
    if target.ends_with("windows-msvc") {
        if let Some(libc_file) = env::var_os("LIBGHOSTTY_VT_WINDOWS_LIBC") {
            println!(
                "cargo:rerun-if-changed={}",
                PathBuf::from(&libc_file).display()
            );
            command.arg("--libc").arg(libc_file);
        }
    }
    if let Ok(system_dir) = env::var("LIBGHOSTTY_VT_ZIG_SYSTEM_DIR") {
        command.arg("--system").arg(system_dir);
    }

    let status = command
        .current_dir(&vendored_dir)
        .status()
        .unwrap_or_else(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
                panic!(
                    "selected Zig executable {:?} disappeared before the build; set ZIG to \
					 a Zig {REQUIRED_ZIG_VERSION} binary, then retry",
                    zig.executable
                );
            }
            panic!("failed to execute zig build for vendored libghostty-vt: {err}");
        });
    assert!(
        status.success(),
        "zig build for vendored libghostty-vt failed: {status}. \
		 Building Herdr requires Zig {REQUIRED_ZIG_VERSION}; set ZIG to an exact \
		 Zig {REQUIRED_ZIG_VERSION} binary, then retry"
    );

    let lib_dir = vendored_dir.join("zig-out/lib");
    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    if target.contains("apple-darwin") {
        let static_lib = lib_dir.join("libghostty-vt.a");
        println!("cargo:rustc-link-arg={}", static_lib.display());
    } else if target.contains("windows-msvc") {
        println!("cargo:rustc-link-lib=static=ghostty-vt-static");
    } else {
        println!("cargo:rustc-link-lib=static=ghostty-vt");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_zig_candidate_does_not_fall_back_to_path() {
        let candidates = vec![zig_candidate("ZIG", "/opt/zig")];
        let error = select_zig(&candidates, |_| Ok("0.15.2".into())).unwrap_err();
        assert!(error.contains("ZIG reported 0.15.2"));
    }

    #[test]
    fn selects_user_local_candidate_after_wrong_path_version() {
        let candidates = vec![
            zig_candidate("zig on PATH", "zig"),
            zig_candidate("user-local", "/home/test/.local/opt/zig-0.16.0/zig"),
        ];
        let selected = select_zig(&candidates, |candidate| {
            Ok(if candidate.label == "zig on PATH" {
                "0.15.2"
            } else {
                REQUIRED_ZIG_VERSION
            }
            .into())
        })
        .unwrap();
        assert_eq!(selected.label, "user-local");
    }

    #[test]
    fn user_local_paths_include_versioned_opt_installation() {
        let paths = versioned_zig_paths(Path::new("/home/test"));
        assert_eq!(
            paths[0],
            PathBuf::from("/home/test/.local/opt/zig-0.16.0/zig")
        );
    }
}

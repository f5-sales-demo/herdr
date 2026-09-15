#!/usr/bin/env python3
"""Render the f5-sales-demo Herdr formula from immutable release checksums."""

from __future__ import annotations

import argparse
import re
from pathlib import Path


REPOSITORY = "f5-sales-demo/herdr"
BINARY_TARGETS = (
    "linux-x86_64",
    "linux-aarch64",
    "macos-x86_64",
    "macos-aarch64",
)
PACKAGE_TARGETS = (
    "macos-x86_64.pkg",
    "macos-aarch64.pkg",
)
TARGETS = BINARY_TARGETS + PACKAGE_TARGETS
SHA256 = re.compile(r"^[0-9a-f]{64}$")
VERSION = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")


def read_checksum(path: Path, artifact: str) -> str:
    fields = path.read_text(encoding="utf-8").strip().split()
    if len(fields) != 2 or fields[1] != artifact or not SHA256.fullmatch(fields[0]):
        raise ValueError(f"invalid checksum file for {artifact}: {path}")
    return fields[0]


def load_checksums(directory: Path) -> dict[str, str]:
    checksums: dict[str, str] = {}
    for target in TARGETS:
        artifact = f"herdr-{target}"
        checksums[target] = read_checksum(directory / f"{artifact}.sha256", artifact)
    return checksums


def asset_block(target: str, checksum: str, indent: str = "      ") -> list[str]:
    artifact = f"herdr-{target}"
    return [
        f'{indent}url "https://github.com/{REPOSITORY}/releases/download/v#{{version}}/{artifact}", using: :nounzip',
        f'{indent}sha256 "{checksum}"',
    ]


def render_formula(version: str, checksums: dict[str, str]) -> str:
    if not VERSION.fullmatch(version):
        raise ValueError(f"expected a stable SemVer version, got {version!r}")
    if set(checksums) != set(TARGETS):
        raise ValueError(f"expected checksums for {', '.join(TARGETS)}")
    if any(not SHA256.fullmatch(value) for value in checksums.values()):
        raise ValueError("every checksum must be a lowercase SHA-256 digest")

    lines = [
        "class Herdr < Formula",
        '  desc "Agent multiplexer for your terminal (f5-sales-demo fork)"',
        f'  homepage "https://github.com/{REPOSITORY}"',
        f'  version "{version}"',
        '  license "Apache-2.0"',
        "",
        "  on_macos do",
        "    if Hardware::CPU.arm?",
        *asset_block("macos-aarch64", checksums["macos-aarch64"]),
        "    else",
        *asset_block("macos-x86_64", checksums["macos-x86_64"]),
        "    end",
        "  end",
        "",
        "  on_linux do",
        "    if Hardware::CPU.arm?",
        *asset_block("linux-aarch64", checksums["linux-aarch64"]),
        "    else",
        *asset_block("linux-x86_64", checksums["linux-x86_64"]),
        "    end",
        "  end",
        "",
        "  def install",
        '    bin.install Dir["herdr-*"].fetch(0) => "herdr"',
        '    (bin/"herdr").chmod 0755',
        '    generate_completions_from_executable(bin/"herdr", "completion")',
        "  end",
        "",
        "  service do",
        '    run [opt_bin/"herdr", "server"]',
        "    keep_alive true",
        '    log_path var/"log/herdr.log"',
        '    error_log_path var/"log/herdr.log"',
        "  end",
        "",
        "  test do",
        '    assert_match "herdr #{version}", shell_output("#{bin}/herdr --version")',
        "  end",
        "end",
        "",
    ]
    return "\n".join(lines)


def render_cask(version: str, checksums: dict[str, str]) -> str:
    """Render a managed-macOS installer cask from immutable package assets."""
    if not VERSION.fullmatch(version):
        raise ValueError(f"expected a stable SemVer version, got {version!r}")
    if set(checksums) != set(TARGETS):
        raise ValueError(f"expected checksums for {', '.join(TARGETS)}")
    if any(not SHA256.fullmatch(value) for value in checksums.values()):
        raise ValueError("every checksum must be a lowercase SHA-256 digest")

    return "\n".join(
        [
            'cask "herdr" do',
            f'  version "{version}"',
            '  arch arm: "aarch64", intel: "x86_64"',
            f'  sha256 arm: "{checksums["macos-aarch64.pkg"]}", intel: "{checksums["macos-x86_64.pkg"]}"',
            "",
            f'  url "https://github.com/{REPOSITORY}/releases/download/v#{{version}}/herdr-macos-#{{arch}}.pkg"',
            '  name "Herdr"',
            '  desc "Agent multiplexer for your terminal (f5-sales-demo fork)"',
            f'  homepage "https://github.com/{REPOSITORY}"',
            "",
            '  pkg "herdr-macos-#{arch}.pkg"',
            "",
            '  uninstall pkgutil: "com.f5.herdr"',
            "end",
            "",
        ]
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--checksums-dir", type=Path, required=True)
    parser.add_argument("--formula-output", type=Path, required=True)
    parser.add_argument("--cask-output", type=Path, required=True)
    args = parser.parse_args()

    checksums = load_checksums(args.checksums_dir)
    args.formula_output.write_text(render_formula(args.version, checksums), encoding="utf-8")
    args.cask_output.parent.mkdir(parents=True, exist_ok=True)
    args.cask_output.write_text(render_cask(args.version, checksums), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

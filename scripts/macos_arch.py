#!/usr/bin/env python3
"""Translate supported Rust macOS targets to canonical Mach-O architectures."""

from __future__ import annotations

import argparse


TARGET_ARCHITECTURES = {
    "aarch64-apple-darwin": "arm64",
    "x86_64-apple-darwin": "x86_64",
}


def macho_arch(target: str) -> str:
    """Return the lipo architecture name for a supported Rust target."""
    try:
        return TARGET_ARCHITECTURES[target]
    except KeyError as error:
        raise ValueError(f"unsupported macOS target: {target}") from error


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("target", help="Rust target triple")
    args = parser.parse_args()
    print(macho_arch(args.target))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

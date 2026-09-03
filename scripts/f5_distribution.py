#!/usr/bin/env python3
"""Build deterministic assets for the inactive F5 Herdr distribution channel."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import re
import shutil
import tarfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

PLATFORMS = ("linux-x86_64", "linux-aarch64", "macos-x86_64", "macos-aarch64")
TAG = re.compile(r"^v(?P<base>[0-9]+\.[0-9]+\.[0-9]+)-xcsh(?P<revision>[1-9][0-9]*)$")


@dataclass(frozen=True)
class TagInfo:
    tag: str
    base_version: str
    binary_version: str
    revision: int


@dataclass(frozen=True)
class Artifact:
    platform: str
    binary: Path
    archive: Path


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def parse_f5_tag(tag: str, cargo_version: str) -> TagInfo:
    match = TAG.fullmatch(tag)
    if match is None:
        raise ValueError("F5 release tag must match v<base>-xcsh<N>")
    base = match.group("base")
    if base != cargo_version:
        raise ValueError(f"tag base {base} does not match Cargo.toml {cargo_version}")
    return TagInfo(tag, base, cargo_version, int(match.group("revision")))


def package_binary(source: Path, platform: str, output: Path) -> Artifact:
    if platform not in PLATFORMS:
        raise ValueError(f"unsupported platform: {platform}")
    output.mkdir(parents=True, exist_ok=True)
    binary = output / f"herdr-{platform}"
    shutil.copyfile(source, binary)
    binary.chmod(0o755)
    archive = output / f"{binary.name}.tar.gz"
    info = tarfile.TarInfo(binary.name)
    payload = binary.read_bytes()
    info.size = len(payload)
    info.mode = 0o755
    info.mtime = 0
    info.uid = info.gid = 0
    info.uname = info.gname = "root"
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w", format=tarfile.USTAR_FORMAT) as tar:
        tar.addfile(info, io.BytesIO(payload))
    with archive.open("wb") as raw, gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
        compressed.write(buffer.getvalue())
    return Artifact(platform, binary, archive)


def build_manifest(tag: str, cargo_version: str, artifacts: Iterable[Artifact], release_base: str) -> dict:
    info = parse_f5_tag(tag, cargo_version)
    by_platform = {item.platform: item for item in artifacts}
    missing = sorted(set(PLATFORMS) - set(by_platform))
    if missing:
        raise ValueError(f"incomplete release artifacts: {', '.join(missing)}")
    release_root = f"{release_base.rstrip('/')}/{tag}"
    platforms = {}
    for platform in PLATFORMS:
        item = by_platform[platform]
        platforms[platform] = {
            "binary": {"url": f"{release_root}/{item.binary.name}", "sha256": digest(item.binary)},
            "archive": {"url": f"{release_root}/{item.archive.name}", "sha256": digest(item.archive)},
        }
    return {
        "schema_version": 1,
        "tag": info.tag,
        "base_version": info.base_version,
        "binary_version": info.binary_version,
        "revision": info.revision,
        "platforms": platforms,
    }


def render_formula(manifest: dict) -> str:
    def block(os_name: str, arm: str, intel: str) -> str:
        entries = manifest["platforms"]
        arm_item, intel_item = entries.get(arm), entries.get(intel)
        if not arm_item or not intel_item:
            return ""
        return f'''  on_{os_name} do
    if Hardware::CPU.arm?
      url "{arm_item['archive']['url']}"
      sha256 "{arm_item['archive']['sha256']}"
    else
      url "{intel_item['archive']['url']}"
      sha256 "{intel_item['archive']['sha256']}"
    end
  end
'''

    return f'''class Herdr < Formula
  desc "Terminal based agent runtime for coding agents (F5 distribution)"
  homepage "https://github.com/f5-sales-demo/herdr"
  version "{manifest['base_version']}"
  revision {manifest['revision']}
{block('macos', 'macos-aarch64', 'macos-x86_64')}{block('linux', 'linux-aarch64', 'linux-x86_64')}
  def install
    bin.install Dir["herdr-*"][0] => "herdr"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/herdr --version")
  end
end
'''


def cargo_version(path: Path) -> str:
    match = re.search(r'(?m)^version\s*=\s*"([^"]+)"', path.read_text(encoding="utf-8"))
    if match is None:
        raise ValueError("Cargo.toml package version is missing")
    return match.group(1)


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    validate = sub.add_parser("validate-tag")
    validate.add_argument("--tag", required=True)
    validate.add_argument("--cargo-toml", type=Path, default=Path("Cargo.toml"))
    package = sub.add_parser("package")
    package.add_argument("--binary", type=Path, required=True)
    package.add_argument("--platform", required=True)
    package.add_argument("--output", type=Path, required=True)
    manifest = sub.add_parser("manifest")
    manifest.add_argument("--tag", required=True)
    manifest.add_argument("--cargo-toml", type=Path, default=Path("Cargo.toml"))
    manifest.add_argument("--artifacts", type=Path, required=True)
    manifest.add_argument("--output", type=Path, required=True)
    manifest.add_argument("--release-base", default="https://github.com/f5-sales-demo/herdr/releases/download")
    formula = sub.add_parser("formula")
    formula.add_argument("--manifest", type=Path, required=True)
    formula.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "validate-tag":
        print(json.dumps(parse_f5_tag(args.tag, cargo_version(args.cargo_toml)).__dict__, sort_keys=True))
    elif args.command == "package":
        package_binary(args.binary, args.platform, args.output)
    elif args.command == "manifest":
        items = [Artifact(platform, args.artifacts / f"herdr-{platform}", args.artifacts / f"herdr-{platform}.tar.gz") for platform in PLATFORMS]
        payload = build_manifest(args.tag, cargo_version(args.cargo_toml), items, args.release_base)
        args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        sums = args.output.parent / "SHA256SUMS"
        files = [item.binary for item in items] + [item.archive for item in items]
        sums.write_text("".join(f"{digest(path)}  {path.name}\n" for path in sorted(files)), encoding="utf-8")
    else:
        args.output.write_text(render_formula(json.loads(args.manifest.read_text(encoding="utf-8"))), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

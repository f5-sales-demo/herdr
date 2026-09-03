# F5 distribution channel

> Status: inactive. No F5 Herdr release, installer endpoint, or Homebrew formula is published yet.

The `f5-sales-demo/herdr` fork stages a signed binary distribution independently of upstream
`herdr.dev`. Tags use `v<base>-xcsh<N>`: `<base>` must exactly equal the package version in
`Cargo.toml`, the binary continues to report that base version, and `N` becomes the Homebrew formula
revision.

## Release contract

The tag workflow builds Linux binaries and uses native Intel and Apple Silicon macOS runners. macOS
binaries must be Developer ID signed with hardened runtime and a secure timestamp, receive an
`Accepted` result from Apple's notary service, and pass both `codesign` and Gatekeeper checks before
artifact upload. Packaging runs twice and compares SHA-256 digests before the release job can create
raw binaries, deterministic archives, `SHA256SUMS`, `f5-latest.json`, `install.sh`, and `herdr.rb`.

The future installer reads the F5 release manifest, selects the exact OS/architecture asset, verifies
its SHA-256 digest, and atomically replaces the destination binary. It does not use or modify
upstream `website/install.sh`. The generated formula uses immutable architecture-specific release
URLs and checksums; it is an output for a later maintainer-controlled tap publication, not an
automatic tap update.

## Activation checklist

An authorized maintainer must configure `APPLE_CERTIFICATE_BASE64`,
`APPLE_CERTIFICATE_PASSWORD`, `APPLE_ID`, `APPLE_PASSWORD`, and `APPLE_TEAM_ID`; execute the first
`v<base>-xcsh<N>` release; verify the public installer on Intel and Apple Silicon macOS plus supported
Linux architectures; and publish the generated formula to the F5 tap. Until all steps succeed, keep
issue #3 open and do not advertise this channel to users.

# f5-sales-demo deployment

The `f5-sales-demo/herdr` fork has one deployment path for supported macOS and
Linux hosts. A successful CI run on `build-xcsh` triggers
`.github/workflows/downstream-release.yml`; maintainers do not build or copy
release binaries by hand.

## Release contract

Every fork release publishes the same version and source commit for these four
assets:

| Host | GitHub release asset | Required verification |
| --- | --- | --- |
| Linux x86_64 | `herdr-linux-x86_64` | static ELF, native `--version` smoke test |
| Linux aarch64 | `herdr-linux-aarch64` | static ELF with no dynamic interpreter |
| macOS Intel | `herdr-macos-x86_64` | Developer ID signature and accepted notarization |
| macOS Apple silicon | `herdr-macos-aarch64` | Developer ID signature and accepted notarization |

Each binary has a matching `.sha256` file. New releases also contain one
`SHA256SUMS` file covering all four binaries. The GitHub Release is made
immutable before package-manager publication begins.

The workflow then renders `f5-sales-demo/homebrew-tap/herdr.rb` from the
published checksums, installs and tests that formula on macOS, and pushes it to
the tap. The formula installs the prebuilt release binary on macOS and Linux;
it never recompiles a different payload from source.

## Required GitHub Actions secrets

Configure these repository secrets before merging a releasable change:

| Secret | Purpose |
| --- | --- |
| `APPLE_CERTIFICATE_BASE64` | Base64-encoded Developer ID Application certificate and private key |
| `APPLE_CERTIFICATE_PASSWORD` | Password protecting the PKCS#12 export |
| `APPLE_TEAM_ID` | Team ID in the Developer ID Application identity |
| `APPLE_ID` | Apple account used by `notarytool` |
| `APPLE_PASSWORD` | App-specific password used by `notarytool` |
| `HOMEBREW_TAP_TOKEN` | Fine-grained token with Contents write access to `f5-sales-demo/homebrew-tap` |

The credential gate runs before the release commit and tag are created. A
missing credential therefore cannot leave behind a new tag with incomplete or
unsigned assets.

The certificate must be a `Developer ID Application` identity. The workflow
imports it into an ephemeral keychain, selects only the identity matching
`APPLE_TEAM_ID`, signs with the hardened runtime and a trusted timestamp, and
deletes the temporary keychain and decoded credentials when the job exits.
These Apple secret names match the existing `f5-sales-demo/xcsh` signing
workflow so the two repositories use the same credential contract.

## Release flow

1. Merge a conventional `feat`, `fix`, `perf`, or `refactor` commit to
   `build-xcsh`.
2. Wait for `CI` to succeed. The downstream workflow derives the next SemVer,
   commits it, and creates the matching annotated tag.
3. Wait for both architecture builds on both operating systems.
4. Confirm the GitHub Release is immutable and contains all binaries and
   checksums.
5. Confirm `f5-sales-demo/tap/herdr` reports the same version.

An existing tag whose release workflow failed can be retried through the
`Downstream release` workflow's `tag` input. If the immutable GitHub Release
already exists, the workflow preserves it and republishes the tap formula from
the checksums attached to that release.

## Consumer installation and verification

Use the same tap command on macOS or a Homebrew-enabled Linux host:

```bash
brew trust --formula f5-sales-demo/tap/herdr
brew install f5-sales-demo/tap/herdr
herdr --version
```

The explicit trust is required by current Homebrew releases because this
third-party formula installs a prebuilt executable rather than compiling local
source. It must be set before Homebrew evaluates an untapped formula.

On macOS, verify the installed payload:

```bash
codesign --verify --strict --verbose=4 "$(brew --prefix herdr)/bin/herdr"
spctl --assess --type execute --verbose=4 "$(brew --prefix herdr)/bin/herdr"
```

The signing details must contain `Authority=Developer ID Application:` and the
expected `TeamIdentifier`; they must not contain `Signature=adhoc`.

On Linux, verify that the installed payload is static:

```bash
file "$(brew --prefix herdr)/bin/herdr"
readelf -l "$(brew --prefix herdr)/bin/herdr" | grep INTERP
```

`file` must report a static or static-PIE executable. The `readelf` command is
expected to print nothing and exit nonzero because the binary has no dynamic
interpreter.

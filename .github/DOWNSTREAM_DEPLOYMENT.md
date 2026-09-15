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
| macOS Intel | `herdr-macos-x86_64`, `herdr-macos-x86_64.pkg` | Developer ID Application signature, accepted notarization, and stapled Developer ID Installer package |
| macOS Apple silicon | `herdr-macos-aarch64`, `herdr-macos-aarch64.pkg` | Developer ID Application signature, accepted notarization, and stapled Developer ID Installer package |

Every binary and macOS package has a matching `.sha256` file. New releases
also contain one `SHA256SUMS` file covering the four binaries and two
installer packages. The GitHub Release is made immutable before
package-manager publication begins.

The workflow then renders `f5-sales-demo/homebrew-tap/herdr.rb` and
`f5-sales-demo/homebrew-tap/Casks/herdr.rb` from published checksums. It
tests the prebuilt formula, publishes both tap entries, and installs those
published entries on a clean macOS runner. The formula installs the prebuilt
release binary on macOS and Linux; the cask installs the stapled signed macOS
package. Neither recompiles a different payload from source.

## Required GitHub Actions secrets

Configure these repository secrets before merging a releasable change:

| Secret | Purpose |
| --- | --- |
| `APPLE_CERTIFICATE_BASE64` | Base64-encoded Developer ID Application certificate and private key |
| `APPLE_CERTIFICATE_PASSWORD` | Password protecting the PKCS#12 export |
| `APPLE_INSTALLER_CERTIFICATE_BASE64` | Base64-encoded Developer ID Installer certificate and private key |
| `APPLE_INSTALLER_CERTIFICATE_PASSWORD` | Password protecting the Installer PKCS#12 export |
| `APPLE_TEAM_ID` | Team ID in the Developer ID Application identity |
| `APPLE_ID` | Apple account used by `notarytool` |
| `APPLE_PASSWORD` | App-specific password used by `notarytool` |
| `HOMEBREW_TAP_TOKEN` | Fine-grained token with Contents write access to `f5-sales-demo/homebrew-tap` |

The credential gate runs before the release commit and tag are created. A
missing credential therefore cannot leave behind a new tag with incomplete or
unsigned assets.

The exports must contain a `Developer ID Application` identity and a separate
`Developer ID Installer` identity for the same team. The workflow imports both
Apple Developer ID intermediate chains into an ephemeral keychain, selects only
identities matching `APPLE_TEAM_ID`, signs binaries with hardened runtime and a
trusted timestamp, signs/staples packages with the Installer identity, and
deletes temporary keychain and decoded credentials when the job exits. These
Apple secret names match the existing `f5-sales-demo/xcsh` signing workflow so
the two repositories use the same credential contract.

## Release flow

1. Merge conventional `feat`, `fix`, `perf`, or `refactor` commits to
   `build-xcsh` and require the push-triggered `CI` run to pass. Push CI is a
   verification gate only and never creates a release.
2. When the branch is ready to release, manually dispatch `CI` on
   `build-xcsh`. Its successful completion is the explicit release-intent
   signal. The downstream workflow derives the next SemVer, commits it, and
   creates the matching annotated tag. This mirrors the `f5-sales-demo/xcsh`
   separation between ordinary CI and an explicit version-release decision.
3. Wait for both architecture builds on both operating systems. macOS binary
   notarization, package notarization, stapling, and Gatekeeper assessment must
   all pass.
4. Confirm the GitHub Release is immutable and contains all binaries,
   packages, and checksums.
5. Confirm the published formula and cask install the same version and pass
   code-signing/Gatekeeper checks.

An existing tag whose release workflow failed can be retried through the
`Downstream release` workflow's `tag` input. If the immutable GitHub Release
already exists, the workflow preserves it and republishes the tap formula from
the checksums attached to that release. Recovery keeps all build and package
inputs pinned to the tag commit, but pins formula/cask rendering and release
verification to the `build-xcsh` commit from which the recovery was manually
dispatched. Both identities are recorded by the workflow. This permits a
reviewed release-tooling correction to recover publication without changing,
reusing, or rewriting the immutable tag and its payloads.

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
```

The signing details must contain `Authority=Developer ID Application:` and the
expected `TeamIdentifier`; they must not contain `Signature=adhoc`. Do not use
`spctl --type execute` as a Gatekeeper verdict for the bare CLI: macOS can
classify a valid, notarized Mach-O command-line tool as `not an app`. The
signed, stapled installer package is the Gatekeeper assessment boundary.

For managed macOS installation, use the cask rather than the formula:

```bash
brew install --cask f5-sales-demo/tap/herdr
pkgutil --pkg-info com.f5.herdr
codesign --verify --strict /usr/local/bin/herdr
```

Before installation, the downloaded release package must pass all three
package-level checks:

```bash
pkgutil --check-signature herdr-macos-aarch64.pkg
xcrun stapler validate herdr-macos-aarch64.pkg
spctl --assess --type install --verbose=4 herdr-macos-aarch64.pkg
```

On Linux, verify that the installed payload is static:

```bash
file "$(brew --prefix herdr)/bin/herdr"
readelf -l "$(brew --prefix herdr)/bin/herdr" | grep INTERP
```

`file` must report a static or static-PIE executable. The `readelf` command is
expected to print nothing and exit nonzero because the binary has no dynamic
interpreter.

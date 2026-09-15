function Invoke-HerdrSmokeCommand {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Command,

        [Parameter(Mandatory = $true)]
        [string[]] $Arguments
    )

    $savedErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $output = & $Command @Arguments 2>&1
        $exitCode = $LASTEXITCODE
    } catch {
        $output = @($_.Exception.Message)
        $exitCode = 1
    } finally {
        $ErrorActionPreference = $savedErrorActionPreference
    }

    [pscustomobject]@{
        ExitCode = $exitCode
        Output = @($output | ForEach-Object { $_.ToString() })
    }
}

function Wait-HerdrSmokeOutput {
    param(
        [Parameter(Mandatory = $true)]
        [string] $ExePath,

        [Parameter(Mandatory = $true)]
        [string] $PaneId,

        [Parameter(Mandatory = $true)]
        [string] $Marker,

        [ValidateRange(1, 300)]
        [int] $TimeoutSeconds = 45,

        [scriptblock] $CommandRunner = ${function:Invoke-HerdrSmokeCommand}
    )

    $timeoutMilliseconds = $TimeoutSeconds * 1000
    $wait = & $CommandRunner $ExePath @(
        "pane",
        "wait-output",
        $PaneId,
        "--match",
        $Marker,
        "--source",
        "recent-unwrapped",
        "--lines",
        "40",
        "--timeout",
        $timeoutMilliseconds.ToString()
    )
    if ($wait.ExitCode -eq 0) {
        return
    }

    $pane = & $CommandRunner $ExePath @(
        "pane",
        "read",
        $PaneId,
        "--source",
        "recent-unwrapped",
        "--lines",
        "80",
        "--format",
        "text"
    )
    $status = & $CommandRunner $ExePath @("status", "server")

    $waitText = $wait.Output -join "`n"
    $paneText = $pane.Output -join "`n"
    $statusText = $status.Output -join "`n"
    $failureKind = if ($waitText -match '"code"\s*:\s*"timeout"') {
        "timed out after $TimeoutSeconds seconds"
    } else {
        "failed with exit code $($wait.ExitCode)"
    }

    throw @"
pane wait-output $failureKind while waiting for '$Marker'
wait-output (exit $($wait.ExitCode)):
$waitText
pane read (exit $($pane.ExitCode)):
$paneText
server status (exit $($status.ExitCode)):
$statusText
"@
}

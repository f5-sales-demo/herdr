$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "windows_smoke_wait_output.ps1")

function Assert-Equal {
    param(
        [Parameter(Mandatory = $true)]
        $Actual,

        [Parameter(Mandatory = $true)]
        $Expected,

        [Parameter(Mandatory = $true)]
        [string] $Message
    )

    if ($Actual -ne $Expected) {
        throw "$Message`: expected '$Expected', got '$Actual'"
    }
}

function Assert-Containment {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Actual,

        [Parameter(Mandatory = $true)]
        [string] $Expected,

        [Parameter(Mandatory = $true)]
        [string] $Message
    )

    if (-not $Actual.Contains($Expected)) {
        throw "$Message`: expected '$Expected' in '$Actual'"
    }
}

function Test-SuccessUsesEventAwareWait {
    $calls = [System.Collections.Generic.List[object]]::new()
    $runner = {
        param([string] $Command, [string[]] $Arguments)
        $calls.Add([pscustomobject]@{ Command = $Command; Arguments = $Arguments })
        [pscustomobject]@{ ExitCode = 0; Output = @('{"result":{"matched":true}}') }
    }.GetNewClosure()

    Wait-HerdrSmokeOutput -ExePath "herdr.exe" -PaneId "w1:p1" `
        -Marker "READY" -CommandRunner $runner

    Assert-Equal $calls.Count 1 "success should invoke one command"
    Assert-Equal ($calls[0].Arguments -join " ") `
        "pane wait-output w1:p1 --match READY --source recent-unwrapped --lines 40 --timeout 45000" `
        "success should use the event-aware wait with the default timeout"
}

function Test-TimeoutFailureDiagnostic {
    $calls = [System.Collections.Generic.List[object]]::new()
    $runner = {
        param([string] $Command, [string[]] $Arguments)
        $calls.Add([pscustomobject]@{ Command = $Command; Arguments = $Arguments })
        switch ($calls.Count) {
            1 { return [pscustomobject]@{ ExitCode = 1; Output = @('{"error":{"code":"timeout"}}') } }
            2 { return [pscustomobject]@{ ExitCode = 0; Output = @("pane diagnostic") } }
            3 { return [pscustomobject]@{ ExitCode = 0; Output = @("status: running") } }
        }
    }.GetNewClosure()

    try {
        Wait-HerdrSmokeOutput -ExePath "herdr.exe" -PaneId "w1:p1" `
            -Marker "READY" -TimeoutSeconds 12 -CommandRunner $runner
        throw "expected timeout failure"
    } catch {
        $message = $_.Exception.Message
    }

    Assert-Containment $message "timed out after 12 seconds" "timeout should stay a failure"
    Assert-Containment $message "pane diagnostic" "timeout should include pane output"
    Assert-Containment $message "status: running" "timeout should include server status"
    Assert-Equal $calls.Count 3 "timeout should collect both diagnostics"
}

function Test-NonzeroExitFailureDiagnostic {
    $calls = [System.Collections.Generic.List[object]]::new()
    $runner = {
        param([string] $Command, [string[]] $Arguments)
        $calls.Add([pscustomobject]@{ Command = $Command; Arguments = $Arguments })
        switch ($calls.Count) {
            1 { return [pscustomobject]@{ ExitCode = 7; Output = @("transport failed") } }
            2 { return [pscustomobject]@{ ExitCode = 0; Output = @("pane output") } }
            3 { return [pscustomobject]@{ ExitCode = 0; Output = @("status output") } }
        }
    }.GetNewClosure()

    try {
        Wait-HerdrSmokeOutput -ExePath "herdr.exe" -PaneId "w1:p1" `
            -Marker "READY" -CommandRunner $runner
        throw "expected nonzero exit failure"
    } catch {
        $message = $_.Exception.Message
    }

    Assert-Containment $message "failed with exit code 7" "nonzero exit should stay a failure"
    Assert-Containment $message "transport failed" "failure should include wait-output stderr"
}

function Test-DiagnosticFailureReporting {
    $calls = [System.Collections.Generic.List[object]]::new()
    $runner = {
        param([string] $Command, [string[]] $Arguments)
        $calls.Add([pscustomobject]@{ Command = $Command; Arguments = $Arguments })
        switch ($calls.Count) {
            1 { return [pscustomobject]@{ ExitCode = 1; Output = @('{"error":{"code":"timeout"}}') } }
            2 { return [pscustomobject]@{ ExitCode = 2; Output = @("pane read failed") } }
            3 { return [pscustomobject]@{ ExitCode = 3; Output = @("status failed") } }
        }
    }.GetNewClosure()

    try {
        Wait-HerdrSmokeOutput -ExePath "herdr.exe" -PaneId "w1:p1" `
            -Marker "READY" -CommandRunner $runner
        throw "expected diagnostic failure"
    } catch {
        $message = $_.Exception.Message
    }

    Assert-Containment $message "pane read (exit 2)" "pane diagnostic exit should be reported"
    Assert-Containment $message "pane read failed" "pane diagnostic output should be reported"
    Assert-Containment $message "server status (exit 3)" "status diagnostic exit should be reported"
    Assert-Containment $message "status failed" "status diagnostic output should be reported"
}

Test-SuccessUsesEventAwareWait
Test-TimeoutFailureDiagnostic
Test-NonzeroExitFailureDiagnostic
Test-DiagnosticFailureReporting

Write-Output "windows smoke wait-output tests passed"

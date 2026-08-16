<#
.SYNOPSIS
  distributed-runtime local 3-node scenario acceptance script (Task 11.2).

.DESCRIPTION
  Black-box acceptance over real runtime-server processes + real PostgreSQL:
    (1) 3-node registration & ACTIVE membership
    (2) MOCK capability only on the 2 nodes with mock-desired=true
    (3) scale-out: enable node-3, minimal-move rebalance ratio in [20%, 45%]
    (4) drain: disable node-1 -> migrate -> cutover -> actual=DISABLED
    (5) lease takeover after killing the leader
    (6) DEAD detection + placement recompute after killing a node
    (7) stale-epoch fencing - covered by unit test MockCapabilityTest (printed reference)

  Docker preflight: prefers `docker` on PATH, falls back to `wsl -e docker`
  (this machine runs dockerd inside WSL Ubuntu-24.04). If Docker is unavailable
  the script prints SKIPPED for every item and exits 0 (not a failure, per Task 11).

.NOTES
  PowerShell 5.1 compatible. Output strings are ASCII on purpose (PS 5.1 reads
  BOM-less UTF-8 scripts as ANSI). Addresses use 127.0.0.1 explicitly because
  `localhost` may resolve to ::1 while listeners are IPv4-only.
#>
param(
    [int]$PgPort = 55432,
    [switch]$SkipCleanup
)

$ErrorActionPreference = 'Stop'
$RepoRoot = Split-Path -Parent $PSScriptRoot
$JarPath = Join-Path $RepoRoot 'runtime\server\target\runtime-server-1.0-SNAPSHOT.jar'
$LogDir = Join-Path $PSScriptRoot 'build\runtime-scenario'
$PgContainer = 'rt-scenario-pg'

$NodeIds = @('node-1', 'node-2', 'node-3')
$HttpPorts = @{ 'node-1' = 8091; 'node-2' = 8092; 'node-3' = 8093 }
$GrpcPorts = @{ 'node-1' = 9101; 'node-2' = 9102; 'node-3' = 9103 }

$script:DockerDirect = $null   # $true = `docker` on PATH; $false = via wsl.exe
$script:Results = New-Object System.Collections.Generic.List[object]
$script:Processes = @{}       # nodeId -> System.Diagnostics.Process

# ---------------------------------------------------------------- helpers --

function Write-Info { param([string]$Message) Write-Host ("[INFO] " + $Message) }

function Add-Result {
    param([string]$Name, [bool]$Passed, [string]$Detail)
    $tag = 'FAIL'
    if ($Passed) { $tag = 'PASS' }
    $script:Results.Add([pscustomobject]@{ Name = $Name; Passed = $Passed; Detail = $Detail })
    Write-Host ("[" + $tag + "] " + $Name)
    Write-Host ("       " + $Detail)
    Write-Host ''
}

function Invoke-Native {
    # run a native command, swallow its output (kept in $LASTEXITCODE only);
    # $ErrorActionPreference is relaxed so redirected stderr cannot throw.
    param([string]$FilePath, [string[]]$Arguments)
    $prevEap = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $null = & $FilePath @Arguments 2>&1
    } catch { }
    finally { $ErrorActionPreference = $prevEap }
    return $LASTEXITCODE
}

function Resolve-DockerMode {
    if (Get-Command docker -ErrorAction SilentlyContinue) {
        $rc = Invoke-Native -FilePath 'docker' -Arguments @('version')
        if ($rc -eq 0) { $script:DockerDirect = $true; Write-Info 'docker CLI on PATH'; return $true }
    }
    if (Get-Command wsl.exe -ErrorAction SilentlyContinue) {
        $rc = Invoke-Native -FilePath 'wsl.exe' -Arguments @('-e', 'docker', 'version')
        if ($rc -eq 0) { $script:DockerDirect = $false; Write-Info 'docker via WSL (wsl -e docker)'; return $true }
    }
    return $false
}

function Invoke-Docker {
    param([Parameter(Mandatory = $true)][string[]]$DockerArgs)
    if ($script:DockerDirect) {
        return Invoke-Native -FilePath 'docker' -Arguments $DockerArgs
    }
    $wslArgs = @('-e', 'docker') + $DockerArgs
    return Invoke-Native -FilePath 'wsl.exe' -Arguments $wslArgs
}

function Stop-ProcessesOnPorts {
    param([int[]]$Ports)
    foreach ($port in $Ports) {
        $conns = @(Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue)
        foreach ($conn in $conns) {
            try {
                $proc = Get-Process -Id $conn.OwningProcess -ErrorAction SilentlyContinue
                if ($proc -and $proc.ProcessName -like '*java*') {
                    Write-Info ("killing stale process {0} (pid {1}) on port {2}" -f $proc.ProcessName, $proc.Id, $port)
                    Stop-Process -Id $proc.Id -Force
                }
            } catch { }
        }
    }
}

function Get-NodeStatus {
    param([string]$NodeId)
    $port = $HttpPorts[$NodeId]
    return Invoke-RestMethod -Uri ("http://127.0.0.1:{0}/rt/status" -f $port) -TimeoutSec 5
}

function Wait-Until {
    param([scriptblock]$Condition, [int]$TimeoutSec = 60, [string]$Description = 'condition')
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        try {
            if (& $Condition) { return $true }
        } catch { }
        Start-Sleep -Milliseconds 500
    }
    Write-Warning ("timeout waiting: " + $Description)
    return $false
}

function Start-RuntimeNode {
    param([string]$NodeId, [int]$HttpPort, [int]$GrpcPort, [bool]$MockDesired)
    $env:RUNTIME_NODE_ID = $NodeId
    $env:SERVER_PORT = "$HttpPort"
    $env:RUNTIME_NODE_GRPC_PORT = "$GrpcPort"
    $env:RUNTIME_CAPABILITIES_MOCK_DESIRED = 'false'
    if ($MockDesired) { $env:RUNTIME_CAPABILITIES_MOCK_DESIRED = 'true' }

    $outLog = Join-Path $LogDir ($NodeId + '.log')
    $errLog = Join-Path $LogDir ($NodeId + '.err.log')
    Write-Info ("starting {0} (http={1}, grpc={2}, mockDesired={3})" -f $NodeId, $HttpPort, $GrpcPort, $MockDesired)
    $proc = Start-Process -FilePath $script:JavaExe `
        -ArgumentList @('-jar', $JarPath) `
        -RedirectStandardOutput $outLog -RedirectStandardError $errLog `
        -PassThru -WindowStyle Hidden
    $script:Processes[$NodeId] = $proc
    return $proc
}

function Get-PrimaryOwners {
    # returns hashtable owner -> count for the MOCK primaries in a status snapshot
    param([object]$Status)
    $counts = @{}
    $map = $Status.primariesByCapability.MOCK
    if ($null -eq $map) { return $counts }
    foreach ($prop in $map.PSObject.Properties) {
        $owner = [string]$prop.Value
        if (-not $counts.ContainsKey($owner)) { $counts[$owner] = 0 }
        $counts[$owner] = $counts[$owner] + 1
    }
    return $counts
}

function Get-CapabilityRecord {
    param([object]$Status, [string]$NodeId, [string]$Type)
    foreach ($rec in @($Status.capabilities)) {
        if ($null -eq $rec -or $null -eq $rec.node) { continue }
        if (([string]$rec.node.value -eq $NodeId) -and ([string]$rec.type -eq $Type)) { return $rec }
    }
    return $null
}

function Find-AliveNodes {
    $alive = @()
    foreach ($id in $NodeIds) {
        $proc = $script:Processes[$id]
        if ($null -ne $proc -and -not $proc.HasExited) { $alive += $id }
    }
    return $alive
}

# ------------------------------------------------------------------ preflight

Write-Host '=== runtime-scenario: local 3-node acceptance ==='
Write-Info ("repo root: " + $RepoRoot)
if (-not (Test-Path $JarPath)) {
    Write-Info 'executable jar missing, building (mvnw -f runtime -pl server -am -DskipTests package)...'
    $rc = Invoke-Native -FilePath (Join-Path $RepoRoot 'mvnw.cmd') `
        -Arguments @('-f', 'runtime', '-pl', 'server', '-am', '-DskipTests', 'package')
    if ($rc -ne 0 -or -not (Test-Path $JarPath)) {
        Write-Host '[FAIL] runtime-server jar build failed'
        exit 1
    }
}

$script:JavaExe = $null
if ($env:JAVA_HOME -and (Test-Path (Join-Path $env:JAVA_HOME 'bin\java.exe'))) {
    $script:JavaExe = Join-Path $env:JAVA_HOME 'bin\java.exe'
} elseif (Get-Command java -ErrorAction SilentlyContinue) {
    $script:JavaExe = (Get-Command java).Source
}
if (-not $script:JavaExe) {
    Write-Host '[FAIL] no java executable found (JAVA_HOME / PATH)'
    exit 1
}
Write-Info ("java: " + $script:JavaExe)

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$portCleanup = @($HttpPorts.Values) + @($GrpcPorts.Values)
Stop-ProcessesOnPorts -Ports $portCleanup

# -------------------------------------------------------------- scenario body

$script:AllSkipped = $false
try {
    # 0. temp PostgreSQL
    $dockerOk = Resolve-DockerMode
    if (-not $dockerOk) {
        Write-Host '[SKIPPED] Docker unavailable (neither `docker` on PATH nor `wsl -e docker` works).'
        Write-Host '          All 7 scenario items are SKIPPED - not a failure per Task 11 preface.'
        $script:AllSkipped = $true
    } else {
        Write-Info ("starting temp PG container {0} (postgres:16, port {1})..." -f $PgContainer, $PgPort)
        Invoke-Docker @('rm', '-f', $PgContainer) | Out-Null
        $rc = Invoke-Docker @('run', '-d', '--name', $PgContainer,
            '-e', 'POSTGRES_PASSWORD=rt', '-e', 'POSTGRES_USER=rt', '-e', 'POSTGRES_DB=rt',
            '-p', ("{0}:5432" -f $PgPort), 'postgres:16')
        if ($rc -ne 0) { throw 'docker run postgres:16 failed' }

        $pgReady = Wait-Until -TimeoutSec 60 -Description 'postgres pg_isready' -Condition {
            (Invoke-Docker @('exec', $PgContainer, 'pg_isready', '-U', 'rt', '-d', 'rt')) -eq 0
        }
        if (-not $pgReady) { throw 'postgres did not become ready in 60s' }
        $pgTcp = Wait-Until -TimeoutSec 20 -Description "PG reachable from Windows on 127.0.0.1:$PgPort" -Condition {
            (Test-NetConnection 127.0.0.1 -Port $PgPort -WarningAction SilentlyContinue).TcpTestSucceeded
        }
        if (-not $pgTcp) { throw "PG not reachable on 127.0.0.1:$PgPort (WSL mirrored port forwarding issue?)" }
        Write-Info ("postgres ready at 127.0.0.1:{0}" -f $PgPort)

        # common env for all 3 nodes (child processes inherit at Start-Process time)
        $env:SPRING_DATASOURCE_URL = ("jdbc:postgresql://127.0.0.1:{0}/rt" -f $PgPort)
        $env:SPRING_DATASOURCE_USERNAME = 'rt'
        $env:SPRING_DATASOURCE_PASSWORD = 'rt'
        $env:RUNTIME_STORE = 'jdbc'
        $env:RUNTIME_NODE_HOST = '127.0.0.1'
        $env:RUNTIME_NODE_WEIGHT = '1.0'
        # tuned timing: lease ttl 5s / renew 1s / reconcile 500ms; heartbeat 500ms, suspect 2, dead 4; cooldown 3s
        $env:RUNTIME_HEARTBEAT_INTERVAL_MS = '500'
        $env:RUNTIME_SUSPECT_AFTER_MISSES = '2'
        $env:RUNTIME_DEAD_AFTER_MISSES = '4'
        $env:RUNTIME_SCHEDULER_TTL_MS = '5000'
        $env:RUNTIME_SCHEDULER_RENEW_INTERVAL_MS = '1000'
        $env:RUNTIME_SCHEDULER_RECONCILE_INTERVAL_MS = '500'
        $env:RUNTIME_REBALANCE_COOLDOWN_MS = '3000'
        $env:RUNTIME_MIGRATION_MAX_CONCURRENT = '8'

        Start-RuntimeNode -NodeId 'node-1' -HttpPort 8091 -GrpcPort 9101 -MockDesired $true | Out-Null
        Start-RuntimeNode -NodeId 'node-2' -HttpPort 8092 -GrpcPort 9102 -MockDesired $true | Out-Null
        Start-RuntimeNode -NodeId 'node-3' -HttpPort 8093 -GrpcPort 9103 -MockDesired $false | Out-Null

        $bootOk = $true
        foreach ($id in $NodeIds) {
            $ok = Wait-Until -TimeoutSec 90 -Description ("$id /rt/status reachable") -Condition {
                $s = Get-NodeStatus $id
                return ($null -ne $s -and $null -ne $s.nodeId)
            }
            if (-not $ok) {
                $bootOk = $false
                Write-Warning ("node did not answer /rt/status: " + $id)
            }
        }
        if (-not $bootOk) { throw 'one or more nodes failed to boot (see logs under scripts\build\runtime-scenario)' }
        Write-Info 'all 3 nodes up (Spring started, DDL bootstrapped, scheduler running)'

        # ---------------- assertion (1): 3-node registration & ACTIVE
        $ok1 = Wait-Until -TimeoutSec 60 -Description 'every node sees 3 ACTIVE nodes in /rt/nodes' -Condition {
            foreach ($id in $NodeIds) {
                $nodes = Invoke-RestMethod -Uri ("http://127.0.0.1:{0}/rt/nodes" -f $HttpPorts[$id]) -TimeoutSec 5
                $active = @($nodes | Where-Object { [string]$_.state -eq 'ACTIVE' })
                if ($active.Count -lt 3) { return $false }
            }
            return $true
        }
        $detail1 = 'each of the 3 nodes /rt/nodes lists all 3 nodes with state=ACTIVE'
        if (-not $ok1) {
            $nodes = Invoke-RestMethod -Uri ("http://127.0.0.1:{0}/rt/nodes" -f $HttpPorts['node-1']) -TimeoutSec 5
            $states = (@($nodes) | ForEach-Object { "{0}={1}" -f $_.id.value, $_.state }) -join ','
            $detail1 = "node-1 view: $states"
        }
        Add-Result '(1) 3-node registration ACTIVE' $ok1 $detail1

        # ---------------- assertion (2): MOCK primaries only over node-1/node-2
        $script:LeaderSnapshot = $null
        $script:LeaderNodeId = $null
        $ok2 = Wait-Until -TimeoutSec 60 -Description 'leader elected & MOCK primaries subset of node-1,node-2' -Condition {
            foreach ($id in $NodeIds) {
                try { $s = Get-NodeStatus $id } catch { continue }
                if ($s.leader -eq $true) {
                    $owners = @((Get-PrimaryOwners $s).Keys)
                    if ($owners.Count -ge 1) {
                        $bad = @($owners | Where-Object { $_ -ne 'node-1' -and $_ -ne 'node-2' })
                        if ($bad.Count -eq 0) {
                            $script:LeaderSnapshot = $s
                            $script:LeaderNodeId = $id
                            return $true
                        }
                    }
                }
            }
            return $false
        }
        $detail2 = 'no leader snapshot captured'
        if ($ok2) {
            $counts = Get-PrimaryOwners $script:LeaderSnapshot
            $summary2 = (@($counts.Keys) | Sort-Object | ForEach-Object { "{0}={1}" -f $_, $counts[$_] }) -join ','
            $detail2 = "leader={0}, MOCK primaries: {1} (owners subset of node-1,node-2)" -f $script:LeaderNodeId, $summary2
        }
        Add-Result '(2) MOCK only on 2 nodes (desired=ACTIVE ones)' $ok2 $detail2

        # ---------------- assertion (3): scale-out minimal move
        Invoke-RestMethod -Method Put `
            -Uri ("http://127.0.0.1:{0}/rt/nodes/node-3/capabilities/MOCK/desired" -f $HttpPorts['node-2']) `
            -ContentType 'application/json' -Body '{"desired":"ACTIVE"}' -TimeoutSec 5 | Out-Null
        Write-Info 'PUT node-3 MOCK desired=ACTIVE'

        $okActual = Wait-Until -TimeoutSec 60 -Description 'node-3 MOCK actual=ACTIVE' -Condition {
            $s = Get-NodeStatus 'node-3'
            $rec = Get-CapabilityRecord $s 'node-3' 'MOCK'
            return ($null -ne $rec -and [string]$rec.actual -eq 'ACTIVE')
        }
        # The scale-out plan (~1/3 of 4096 partitions) is applied as one migrateAndWait
        # batch, but per-partition cutover lands in the placement table progressively.
        # Waiting only for node-3's FIRST partition snapshots the ring mid-cutover;
        # wait for convergence instead (terminal share = 1/3, inside [20%, 45%]).
        $okPrimary = Wait-Until -TimeoutSec 120 -Description 'MOCK primaries converge: node-3 share in [20%,45%]' -Condition {
            $s = Get-NodeStatus 'node-2'
            $counts = Get-PrimaryOwners $s
            $t = 0; $n = 0
            foreach ($k in $counts.Keys) { $t += $counts[$k] }
            if ($counts.ContainsKey('node-3')) { $n = $counts['node-3'] }
            if ($t -le 0) { return $false }
            $r = [double]$n / [double]$t
            return ($r -ge 0.20 -and $r -le 0.45)
        }
        $total = 0; $n3 = 0; $ratio = 0.0
        $s2 = Get-NodeStatus 'node-2'
        $counts3 = Get-PrimaryOwners $s2
        foreach ($k in $counts3.Keys) { $total += $counts3[$k] }
        if ($counts3.ContainsKey('node-3')) { $n3 = $counts3['node-3'] }
        if ($total -gt 0) { $ratio = [double]$n3 / [double]$total }
        $okRatio = ($okActual -and $okPrimary -and $ratio -ge 0.20 -and $ratio -le 0.45)
        $has1 = ($counts3.ContainsKey('node-1') -and $counts3['node-1'] -gt 0)
        $has2 = ($counts3.ContainsKey('node-2') -and $counts3['node-2'] -gt 0)
        $ok3 = ($okRatio -and $has1 -and $has2)
        $dist = (@($counts3.Keys) | Sort-Object | ForEach-Object { "{0}={1}" -f $_, $counts3[$_] }) -join ','
        $detail3 = "node-3 share={0:P1} (expected in [20%,45%]); node-3 actual=ACTIVE={1}; node-1/node-2 still holding partitions={2}/{3}; dist: {4}. NOTE: exact minimal-move is guaranteed by unit tests (RebalancePlannerTest / migration module tests); this is a black-box approximation." -f $ratio, $okActual, $has1, $has2, $dist
        Add-Result '(3) scale-out rebalance ratio 20%-45%' $ok3 $detail3

        # ---------------- assertion (4): drain node-1 (migrate + cutover + disable)
        Invoke-RestMethod -Method Put `
            -Uri ("http://127.0.0.1:{0}/rt/nodes/node-1/capabilities/MOCK/desired" -f $HttpPorts['node-2']) `
            -ContentType 'application/json' -Body '{"desired":"DISABLED"}' -TimeoutSec 5 | Out-Null
        Write-Info 'PUT node-1 MOCK desired=DISABLED (drain)'

        $okDisabled = Wait-Until -TimeoutSec 60 -Description 'node-1 MOCK actual=DISABLED' -Condition {
            $s = Get-NodeStatus 'node-2'
            $rec = Get-CapabilityRecord $s 'node-1' 'MOCK'
            return ($null -ne $rec -and [string]$rec.actual -eq 'DISABLED')
        }
        $okNoOwner1 = Wait-Until -TimeoutSec 60 -Description 'primaries exclude node-1' -Condition {
            $s = Get-NodeStatus 'node-2'
            $counts = Get-PrimaryOwners $s
            return (-not $counts.ContainsKey('node-1') -or $counts['node-1'] -eq 0)
        }
        $ok4 = ($okDisabled -and $okNoOwner1)
        $detail4 = "node-1 actual=DISABLED={0}; primaries exclude node-1={1} (freeze -> migrate -> cutover CAS -> stop)" -f $okDisabled, $okNoOwner1
        Add-Result '(4) drain cutover (node-1 -> DISABLED, no primaries)' $ok4 $detail4

        # ---------------- assertion (5): lease takeover after leader kill
        $leaderId = $null
        foreach ($id in (Find-AliveNodes)) {
            try {
                $s = Get-NodeStatus $id
                if ($s.leader -eq $true) { $leaderId = $id; break }
            } catch { }
        }
        if ($null -eq $leaderId) {
            Add-Result '(5) lease takeover' $false 'no leader found among alive nodes'
        } else {
            $victim = $script:Processes[$leaderId]
            Write-Info ("killing current leader {0} (pid {1})" -f $leaderId, $victim.Id)
            Stop-Process -Id $victim.Id -Force
            $script:Processes[$leaderId] = $null
            $script:NewLeaderId = $null
            $okTakeover = Wait-Until -TimeoutSec 15 -Description 'survivor takes over the lease' -Condition {
                foreach ($id in (Find-AliveNodes)) {
                    try {
                        $s = Get-NodeStatus $id
                        if ($s.leader -eq $true) { $script:NewLeaderId = $id; return $true }
                    } catch { }
                }
                return $false
            }
            $okServe = $false
            if ($okTakeover) {
                try {
                    $sNew = Get-NodeStatus $script:NewLeaderId
                    $nNew = Invoke-RestMethod -Uri ("http://127.0.0.1:{0}/rt/nodes" -f $HttpPorts[$script:NewLeaderId]) -TimeoutSec 5
                    $okServe = ($null -ne $sNew -and $null -ne $nNew)
                } catch { }
            }
            $ok5 = ($okTakeover -and $okServe)
            $detail5 = "killed leader={0}; new leader={1} within 15s (2xTTL+buffer, TTL=5s); new leader still serving /rt/status + /rt/nodes = {2}" -f $leaderId, $script:NewLeaderId, $okServe
            Add-Result '(5) lease takeover after leader killed' $ok5 $detail5
        }

        # ---------------- assertion (6): DEAD detection + recompute
        # A dead node's partitions can only be recomputed onto a surviving
        # desired-ACTIVE node. After (4) drained node-1 and (5) killed one of
        # node-2/node-3, re-enable node-1 first (also exercises the
        # DISABLED -> ACTIVE re-enable path), then kill the remaining victim.
        # admin endpoint works on any live node (desired state goes to the shared DB);
        # node-2 may already be dead if it was the leader killed in (5).
        $adminNode = $null
        foreach ($id in @('node-2', 'node-3', 'node-1')) {
            $proc = $script:Processes[$id]
            if ($null -ne $proc -and -not $proc.HasExited) { $adminNode = $id; break }
        }
        if ($null -eq $adminNode) { throw 'no alive node left to accept the desired-state PUT' }
        Invoke-RestMethod -Method Put `
            -Uri ("http://127.0.0.1:{0}/rt/nodes/node-1/capabilities/MOCK/desired" -f $HttpPorts[$adminNode]) `
            -ContentType 'application/json' -Body '{"desired":"ACTIVE"}' -TimeoutSec 5 | Out-Null
        Write-Info ("PUT node-1 MOCK desired=ACTIVE via {0} (re-enable as relocation target for (6))" -f $adminNode)
        $okReenable = Wait-Until -TimeoutSec 60 -Description 'node-1 MOCK actual=ACTIVE again' -Condition {
            $s = Get-NodeStatus 'node-1'
            $rec = Get-CapabilityRecord $s 'node-1' 'MOCK'
            return ($null -ne $rec -and [string]$rec.actual -eq 'ACTIVE')
        }
        $target = $null
        foreach ($id in @('node-2', 'node-3')) {
            $proc = $script:Processes[$id]
            if ($null -ne $proc -and -not $proc.HasExited) { $target = $id; break }
        }
        if (-not $okReenable -or $null -eq $target) {
            Add-Result '(6) DEAD detection + placement recompute' $false `
                ("preconditions failed: node-1 re-enabled={0}, killable victim found={1}" -f $okReenable, ($null -ne $target))
        } else {
            $targetProc = $script:Processes[$target]
            Write-Info ("killing {0} (pid {1}) to trigger DEAD detection" -f $target, $targetProc.Id)
            Stop-Process -Id $targetProc.Id -Force
            $script:Processes[$target] = $null
            $script:DeadViewState = '<none>'
            $okView = Wait-Until -TimeoutSec 15 -Description ("survivor membership view marks {0} SUSPECTED/DEAD" -f $target) -Condition {
                foreach ($id in (Find-AliveNodes)) {
                    try {
                        $s = Get-NodeStatus $id
                        $m = $s.members.PSObject.Properties[$target]
                        if ($null -ne $m) {
                            $st = [string]$m.Value.state
                            if ($st -eq 'SUSPECTED' -or $st -eq 'DEAD') { $script:DeadViewState = $st; return $true }
                        }
                    } catch { }
                }
                return $false
            }
            $okRecompute = Wait-Until -TimeoutSec 60 -Description ("primaries recomputed without {0}" -f $target) -Condition {
                foreach ($id in (Find-AliveNodes)) {
                    try {
                        $s = Get-NodeStatus $id
                        $counts = Get-PrimaryOwners $s
                        if (-not $counts.ContainsKey($target) -or $counts[$target] -eq 0) { return $true }
                    } catch { }
                }
                return $false
            }
            $ok6 = ($okView -and $okRecompute)
            $detail6 = "node-1 re-enabled as target={0}; survivor membership view marks {1} as {2} (<=15s, suspect=2 misses/dead=4 misses @500ms heartbeat); primaries exclude {1} after recompute (<=60s) = {3}" -f $okReenable, $target, $script:DeadViewState, $okRecompute
            Add-Result '(6) DEAD detection + placement recompute' $ok6 $detail6
        }

        # ---------------- assertion (7): stale-epoch fencing reference
        # Old-epoch writes are fenced inside MockCapability (StaleEpochException); there is
        # deliberately no REST write endpoint (management plane only writes desired state),
        # so this item prints the unit-test reference and re-runs the class as evidence.
        $fencingCases = @('putRejectsWhenPartitionHasNoOwner', 'putRejectsWhenOwnerIsAnotherNode',
            'putRejectsStaleEpoch', 'conveniencePutUsesCurrentOwnerEpoch')
        Write-Info 're-running MockCapabilityTest as fencing evidence...'
        $fencingGreen = $false
        $rc7 = Invoke-Native -FilePath (Join-Path $RepoRoot 'mvnw.cmd') `
            -Arguments @('-f', 'runtime/capability', '-Dtest=MockCapabilityTest', 'test')
        if ($rc7 -eq 0) { $fencingGreen = $true }
        $detail7 = "stale-epoch reject-write covered by runtime\capability MockCapabilityTest: {0}; class re-run result green={1}; no REST write endpoint by design (admin writes desired only)" -f ($fencingCases -join ', '), $fencingGreen
        Add-Result '(7) stale-epoch fencing (unit-test reference)' $fencingGreen $detail7
    }
} catch {
    Write-Warning ("scenario aborted: " + $_.Exception.Message)
} finally {
    if (-not $SkipCleanup) {
        Write-Info 'cleanup: stopping node processes...'
        foreach ($id in $NodeIds) {
            $proc = $script:Processes[$id]
            if ($null -ne $proc -and -not $proc.HasExited) {
                try { Stop-Process -Id $proc.Id -Force } catch { }
            }
        }
        if ($null -ne $script:DockerDirect) {
            Write-Info ("cleanup: docker rm -f {0}" -f $PgContainer)
            Invoke-Docker @('rm', '-f', $PgContainer) | Out-Null
        }
    } else {
        Write-Info 'SkipCleanup set: node processes / PG container left running'
    }
}

# ------------------------------------------------------------------ summary
Write-Host '==================== scenario summary ===================='
if ($script:AllSkipped) {
    Write-Host 'ALL ITEMS SKIPPED (Docker unavailable) - exit 0 per Task 11 preface.'
    exit 0
}
$passCount = 0
foreach ($r in $script:Results) {
    $tag = 'FAIL'
    if ($r.Passed) { $tag = 'PASS'; $passCount++ }
    Write-Host ("  [{0}] {1}" -f $tag, $r.Name)
}
$total = $script:Results.Count
Write-Host ("=========================================================== {0}/{1} PASS" -f $passCount, $total)
if ($total -eq 7 -and $passCount -eq 7) { exit 0 } else { exit 1 }

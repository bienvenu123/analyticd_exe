param(
    [string]$DataPath = 'data_assignment',
    [string]$UserId = 'user_000042'
)
$ErrorActionPreference = 'Stop'
$projectRoot = (Resolve-Path -LiteralPath $PSScriptRoot).Path
$hbaseHome = if ($env:HBASE_HOME) { $env:HBASE_HOME } else { Join-Path $env:USERPROFILE 'Downloads\hbase-2.6.6-bin\hbase-2.6.6' }
$hadoopHome = $env:HADOOP_HOME
$javaHome = $env:JAVA_HOME
$hbaseCmd = Join-Path $hbaseHome 'bin\hbase.cmd'
$runtime = Join-Path $projectRoot 'hbase_runtime'
$logs = Join-Path $runtime 'logs'
$confDir = Join-Path $runtime 'conf'
$store = Join-Path $env:TEMP 'auca_hbase_analytics'
New-Item -ItemType Directory -Force -Path $logs,$confDir,(Join-Path $store 'tmp'),(Join-Path $store 'data'),(Join-Path $store 'zookeeper') | Out-Null

if (-not (Test-Path -LiteralPath $hbaseCmd)) { throw "HBase not found at $hbaseHome. Set HBASE_HOME to the HBase installation folder." }
if (-not $javaHome -or -not (Test-Path -LiteralPath (Join-Path $javaHome 'bin\java.exe'))) {
    throw 'Set JAVA_HOME to a supported JDK installation before starting HBase.'
}

# Keep the checked-in configuration portable; only the runtime overlay contains local paths.
[xml]$site = Get-Content -LiteralPath (Join-Path $projectRoot 'hbase_conf\hbase-site.xml') -Raw
function Set-HBaseProperty([string]$Name, [string]$Value) {
    $property = @($site.configuration.property | Where-Object { $_.name -eq $Name }) | Select-Object -First 1
    if (-not $property) {
        $property = $site.CreateElement('property')
        $nameNode = $site.CreateElement('name'); $nameNode.InnerText = $Name
        $valueNode = $site.CreateElement('value'); $valueNode.InnerText = $Value
        [void]$property.AppendChild($nameNode); [void]$property.AppendChild($valueNode)
        [void]$site.configuration.AppendChild($property)
    } else { $property.value = $Value }
}
$storePath = $store.Replace('\','/')
Set-HBaseProperty 'hbase.tmp.dir' "$storePath/tmp"
Set-HBaseProperty 'hbase.rootdir' "file:///$storePath/data"
Set-HBaseProperty 'hbase.zookeeper.property.dataDir' "$storePath/zookeeper"
$site.Save((Join-Path $confDir 'hbase-site.xml'))

$env:HBASE_HOME = $hbaseHome
$env:HBASE_CONF_DIR = $confDir
$env:HBASE_LOG_DIR = $logs
$env:HBASE_PID_DIR = $runtime
$env:JAVA_HOME = $javaHome
if ($hadoopHome) { $env:HADOOP_HOME = $hadoopHome; $env:PATH = "$hadoopHome\bin;$env:PATH" }
$env:PATH = "$javaHome\bin;$env:PATH"

function Test-LocalPort([int]$Port) {
    $client = [System.Net.Sockets.TcpClient]::new()
    try { $client.Connect('127.0.0.1', $Port); return $true }
    catch { return $false }
    finally { $client.Dispose() }
}

if (-not (Test-LocalPort 16010)) {
    $masterOut = Join-Path $logs 'hbase-master.stdout.log'
    $masterErr = Join-Path $logs 'hbase-master.stderr.log'
    Start-Process -FilePath $hbaseCmd -ArgumentList @('master', 'start') -WorkingDirectory $projectRoot -WindowStyle Hidden -RedirectStandardOutput $masterOut -RedirectStandardError $masterErr | Out-Null
}
$deadline = (Get-Date).AddMinutes(3)
while (-not (Test-LocalPort 16010) -and (Get-Date) -lt $deadline) { Start-Sleep -Seconds 3 }
if (-not (Test-LocalPort 16010)) { throw "HBase master did not bind port 16010. Check $logs" }

if (-not (Test-LocalPort 9090)) {
    $thriftOut = Join-Path $logs 'hbase-thrift.stdout.log'
    $thriftErr = Join-Path $logs 'hbase-thrift.stderr.log'
    Start-Process -FilePath $hbaseCmd -ArgumentList @('thrift', 'start', '-p', '9090') -WorkingDirectory $projectRoot -WindowStyle Hidden -RedirectStandardOutput $thriftOut -RedirectStandardError $thriftErr | Out-Null
}
$deadline = (Get-Date).AddMinutes(2)
while (-not (Test-LocalPort 9090) -and (Get-Date) -lt $deadline) { Start-Sleep -Seconds 3 }
if (-not (Test-LocalPort 9090)) { throw "HBase Thrift did not bind port 9090. Check $logs" }

python (Join-Path $projectRoot 'hbase_setup.py') --host localhost --port 9090
if ($LASTEXITCODE -ne 0) { throw 'HBase table creation failed.' }

$data = (Resolve-Path -LiteralPath (Join-Path $projectRoot $DataPath)).Path
python (Join-Path $projectRoot 'hbase_loader.py') --data $data --host localhost --port 9090 --results (Join-Path $projectRoot 'output\hbase_results.json')
if ($LASTEXITCODE -ne 0) { throw 'HBase session and product metrics loader failed.' }

python (Join-Path $projectRoot 'hbase_query.py') --user-id $UserId --host localhost --port 9090 --out (Join-Path $projectRoot 'output\hbase_user_query.json')
if ($LASTEXITCODE -ne 0) { throw 'HBase user-prefix query failed.' }
Write-Output "HBase is available at localhost:9090; loaded data and verified the user scan for $UserId."

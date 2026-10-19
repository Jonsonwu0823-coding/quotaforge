[CmdletBinding()]
param(
    [switch]$Check,
    [switch]$Stream,
    [switch]$Stop,
    [string]$UpstreamBaseUrl,
    [string]$Reconcile,
    [switch]$NoCache,
    [long]$AccountLimit = 1000000
)

# 本机用（Windows PowerShell 5.1 起）：以真实上游（百炼）起 QF 和它的账本数据库，接入外部网络 qs-qf，
# 用 qfctl 给 QS 建账户、发一把 QF Key。嵌入缓存默认开着（见下面的 -NoCache）。
#
#   .\scripts\compose-real.ps1           输入百炼地址和 Key，起 QF，QF Key 放进这个窗口的环境变量 QUERYSHIELD_MODEL_API_KEY
#   .\scripts\compose-real.ps1 -Check    同上，再经 QF 发一次聊天、一次嵌入（花费很少），只显示状态码、模型名、usage、x-request-id，
#                                        出错时显示状态码和错误码；聊天带一个新生成的 Idempotency-Key，再用同一个键发一遍，
#                                        显示第二次是重放、已消费没有增加（不多花钱）；同一个嵌入再发一遍，显示第二次命中嵌入缓存
#                                        （Cache-Status: quotaforge; hit、正文和第一次相同、调用状态 CACHED、已消费没有增加，不调上游）；
#                                        带 -NoCache 时不发第二次，改成核对嵌入的响应没有 Cache-Status 头（缓存确实关着）。
#                                        最后显示这个账户的额度（上限、已消费、剩余）和缓存命中的次数，以及这把 Key 和账户的限流限额
#                                        （每分钟的请求数和 token 数）。任何一次请求不是 2xx，或者重放、缓存的核对不成立，
#                                        就在额度那一行之后报错退出（throw）；QF 还开着，QUERYSHIELD_MODEL_API_KEY 仍然设着
#   .\scripts\compose-real.ps1 -Stream   同上，再经 QF 发三次流式聊天（花费很少），只显示流的结构，不显示内容：
#                                        1. 不带 stream_options：事件数、data 行数、最后一个是不是 [DONE]、choices 为空的事件数（QF 去掉了
#                                           usage 事件，应为 0）、行尾是 \n 还是 \r\n；调用状态是 SETTLED、有用量；
#                                        2. 带 include_usage: true：usage 事件在倒数第几个、它的 choices 是不是空数组；调用的用量等于它的数；
#                                        3. 要一个长一点的回答，读 2 秒就断开：等一会儿查调用状态，应是 SETTLED（QF 读完了上游的流）。
#                                        流的原文写进临时目录（-Stop 时删掉），不显示。可以和 -Check 一起用；任何一项不成立就报错退出
#   .\scripts\compose-real.ps1 -Stop     停掉 QF，连数据库的数据一起删掉，删掉临时目录，清掉 QUERYSHIELD_MODEL_API_KEY
#   .\scripts\compose-real.ps1 -Reconcile <QS 的 demo-summary.json>
#                                        经 QS 跑完真实演示之后：把 QS 的摘要和正在运行的 QF 的账本逐个 run 对账
#                                        （check_qs_runs.py，真实模式），再跑一次 qfctl verify。不启动、不清理任何东西；
#                                        退出码：0 全部对上；1 有对不上的 run、账本校验失败，或本脚本报错（QF 没在运行、找不到
#                                        Python 等）；2 核对脚本跑不了（摘要不合法、读不了账本）。不能和 -Check、-Stop 一起用
#
# -UpstreamBaseUrl 显式指定上游地址，不再询问。用来把“真实”上游指向别的 OpenAI 兼容服务
# （例如 QF 自己的假上游），在不花钱的情况下检查脚本流程。
#
# -AccountLimit <tokens> 是给 QS 建的账户的额度上限，默认 1,000,000；长评测（例如几十段对话的多轮评测）建议给 2000000。
#
# 嵌入缓存默认开着：只影响不带 Idempotency-Key 的嵌入请求（QS 建知识索引的那批），带键的请求（run 里的）不查缓存。
# 缓存放在单独的 qf-redis-cache 里（不持久化），和账本的库一样每次启动都是新的，停止时一起删掉。
# -NoCache 关掉它：QF 用的配置是 deploy\config.real.json 去掉 embedding_cache 那一行（写进临时目录），和开缓存之前的配置
# 逐字节相同，也不起 qf-redis-cache。容器里仍有 QF_REDIS_CACHE_URL 这个环境变量，但没有配置指向它，QF 不读。
#
# 百炼 Key 只放在本进程的环境变量里，经 Compose secrets 挂成容器里的文件；不打印、不写文件，
# 脚本结束时恢复它设过的环境变量（$envNames 里的几个）。QF Key 只放进当前 PowerShell 进程的环境变量 QUERYSHIELD_MODEL_API_KEY（不显示，不写文件，
# 库里只有它的哈希），脚本结束后留着，给同一个窗口里随后运行的 QS 的 compose-real-demo.ps1 读取（它在网关模式下先读这个变量，
# 有值就不提示）；-Stop 清掉。再次启动时新 Key 直接覆盖旧值；-Reconcile 不碰它。
#
# 数据库每次都是新的：每次启动随机生成数据库密码，写进系统临时目录下 quotaforge-real\db 的三个文件，
# 经 Compose secrets 挂进容器（不进环境变量、docker inspect 和任何输出）。-Stop 时连数据和这个目录一起删掉。
# 上一次没有 -Stop 就再次启动时，先自动清掉上一次的容器、数据卷和临时文件，否则旧库里的密码和新生成的对不上。
#
# docker compose 都带 --progress plain --ansi never：让 docker 的输出是逐行的纯文本，不带进度动画和颜色。

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue' # 5.1 的 Invoke-WebRequest 进度条写回窗口顶部几行时会弄乱中文（-Check 用到）；只在本脚本的作用域里生效
$summaryPath = $null
if ($AccountLimit -lt 1) { throw "-AccountLimit 要是正整数（tokens），现在是 $AccountLimit。" }
if ($PSBoundParameters.ContainsKey('Reconcile')) {
    if ($Check -or $Stream -or $Stop) { throw '-Reconcile 不能和 -Check、-Stream、-Stop 一起用。' }
    if (-not $Reconcile -or -not (Test-Path -LiteralPath $Reconcile -PathType Leaf)) { throw "找不到 QS 的摘要文件：$Reconcile" }
    # 下面会切到 QF 目录，所以先把相对路径变成完整路径。
    $summaryPath = (Resolve-Path -LiteralPath $Reconcile).ProviderPath
}
$qfDir = Split-Path -Parent $PSScriptRoot
$tempDir = Join-Path ([IO.Path]::GetTempPath()) 'quotaforge-real'
$dbDir = Join-Path $tempDir 'db'
$envNames = @('QF_UPSTREAM_API_KEY', 'QF_UPSTREAM_BASE_URL', 'QF_DB_SECRETS_DIR', 'QF_REAL_CONFIG', 'COMPOSE_IGNORE_ORPHANS')
$account = 'queryshield'
$project = 'quotaforge' # compose.yaml 里的 name:，所有 docker compose 命令靠它找到这套栈
# 限流限额（每分钟）：远高于演示的用量（3 分钟 54 次请求，单次聊天的估算最多约 17,300 tokens）。
$accountRate = @('-rpm', '600', '-tpm', '2000000')
$keyRate = @('-rpm', '300', '-tpm', '1000000')

# 原生命令的失败看退出码；不把它的 stderr 转成 PowerShell 错误（5.1 下 2>&1 会这样）。
function Invoke-Native {
    param([string]$What, [scriptblock]$Command)
    & $Command
    if ($LASTEXITCODE -ne 0) { throw "$What 失败（退出码 $LASTEXITCODE）" }
}

function Read-HiddenSecret {
    param([string]$Prompt)
    $secure = Read-Host -Prompt $Prompt -AsSecureString
    $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secure)
    try { return [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr) }
    finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr) }
}

# 所有 docker compose 调用都经这里，带同一组全局参数。不带 -f 时只用主文件：qfctl、ps、down 不需要真实覆盖文件。
function Invoke-Compose {
    param([string[]]$Arguments)
    docker compose --progress plain --ansi never @Arguments
}

# 32 个十六进制字符的随机密码。
function New-HexSecret {
    $bytes = New-Object byte[] 16
    $rng = [Security.Cryptography.RandomNumberGenerator]::Create()
    try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
    return (($bytes | ForEach-Object { $_.ToString('x2') }) -join '')
}

# 写三个数据库 secret 文件（UTF-8 无 BOM），返回给 Compose 用的目录（正斜杠）。
function New-DatabaseSecrets {
    New-Item -ItemType Directory -Force -Path $dbDir | Out-Null
    $utf8 = New-Object Text.UTF8Encoding $false
    $files = @{
        'root_password' = New-HexSecret
        'migrate_dsn'   = "qf_migrate:$(New-HexSecret)@tcp(qf-mysql:3306)/quotaforge"
        'app_dsn'       = "qf_app:$(New-HexSecret)@tcp(qf-mysql:3306)/quotaforge"
    }
    foreach ($name in $files.Keys) {
        [IO.File]::WriteAllText((Join-Path $dbDir $name), $files[$name] + "`n", $utf8)
    }
    return $dbDir.Replace('\', '/')
}

# -NoCache 用的配置：deploy\config.real.json 去掉 embedding_cache 那一行（连同它前面的逗号），其余逐字节不变，
# 也就是开缓存之前的配置。核对正好删掉一处、一行，否则报错：配置被改过时，不悄悄生成一份半开半关的。返回给 Compose 用的路径（正斜杠）。
function New-NoCacheConfig {
    $text = [IO.File]::ReadAllText((Join-Path (Join-Path $qfDir 'deploy') 'config.real.json'))
    $pattern = ',\s*"embedding_cache":\s*\{[^}]*\}'
    $found = [regex]::Matches($text, $pattern).Count
    $result = [regex]::Replace($text, $pattern, '')
    $removedLines = ($text -split "`n").Count - ($result -split "`n").Count
    if ($found -ne 1 -or $removedLines -ne 1 -or $result.Contains('embedding_cache')) {
        throw "deploy\config.real.json 里 embedding_cache 应当正好是一行（找到 $found 处，删掉 $removedLines 行），没有生成关掉缓存的配置。"
    }
    $path = Join-Path $tempDir 'config.real.json'
    [IO.File]::WriteAllText($path, $result, (New-Object Text.UTF8Encoding $false))
    return $path.Replace('\', '/')
}

# 删掉容器、数据卷（数据库的数据）和临时目录。-Stop 和发现上一次的残留时都用它。
# 只带主文件：qf-redis-cache 只在 compose.real.yaml 里，这里算孤儿，由 --remove-orphans 删掉（它的 /data 是 tmpfs，没有卷）。
function Remove-Previous {
    Invoke-Native 'docker compose down' { Invoke-Compose @('down', '-v', '--remove-orphans') }
    Remove-Item -Recurse -Force $tempDir -ErrorAction SilentlyContinue
}

function Invoke-Qfctl {
    param([string[]]$Arguments)
    $output = Invoke-Compose (@('run', '--rm', '-T', '--no-deps', 'qfctl') + $Arguments)
    if ($LASTEXITCODE -ne 0) { throw "qfctl $($Arguments[0]) $($Arguments[1]) 失败（退出码 $LASTEXITCODE）" }
    return $output
}

# 只显示代理变量设没设：值里可能有代理的账号密码。
function Show-ProxyVariables {
    $containerId = Invoke-Compose @('ps', '-q', 'quotaforge')
    $containerEnv = docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' $containerId
    foreach ($name in 'HTTP_PROXY', 'HTTPS_PROXY', 'NO_PROXY', 'http_proxy', 'https_proxy', 'no_proxy') {
        $state = '未设置'
        if ($containerEnv -cmatch "^$name=") { $state = '已设置' }
        Write-Host "  ${name}：$state"
    }
}

# 发一次请求，显示结果；成功时返回响应（给重放演示比对），失败时返回 $null。
function Invoke-QF {
    param([string]$Path, [string]$Key, [string]$Body, [string]$IdempotencyKey)
    $headers = @{ Authorization = "Bearer $Key" }
    if ($IdempotencyKey) { $headers['Idempotency-Key'] = $IdempotencyKey }
    $request = @{
        Uri             = "http://127.0.0.1:8080$Path"
        Method          = 'Post'
        Headers         = $headers
        ContentType     = 'application/json'
        Body            = [Text.Encoding]::UTF8.GetBytes($Body)
        UseBasicParsing = $true
    }
    try {
        $response = Invoke-WebRequest @request
    }
    catch {
        # 非 2xx 会抛异常（5.1 和 7 的异常类型不同，都带 Response，正文在 ErrorDetails.Message）。
        # 显示状态码和 QF 的错误码，例如 upstream_auth_failed（Key 错）和 upstream_unavailable（地址错）；
        # 正文不是 QF 的错误 JSON 时只显示状态码。对应的调用号在 QF 日志里。
        $err = $_
        $failed = $err.Exception.Response
        if ($null -eq $failed) { throw }
        $code = $null
        try { $code = ($err.ErrorDetails.Message | ConvertFrom-Json).error.code } catch { }
        if ($code) {
            Write-Host "  ${Path}：状态 $([int]$failed.StatusCode)，错误码 $code"
        }
        else {
            Write-Host "  ${Path}：状态 $([int]$failed.StatusCode)"
        }
        return $null
    }
    $json = $response.Content | ConvertFrom-Json
    $replayed = ''
    if ($response.Headers['Idempotent-Replayed']) { $replayed = "，重放（Idempotent-Replayed: $($response.Headers['Idempotent-Replayed'])）" }
    $cache = ''
    if ($response.Headers['Cache-Status']) { $cache = "，Cache-Status: $($response.Headers['Cache-Status'])" }
    Write-Host "  ${Path}：状态 $([int]$response.StatusCode)，模型 $($json.model)，usage $($json.usage | ConvertTo-Json -Compress)，x-request-id $($response.Headers['x-request-id'])$replayed$cache"
    return $response
}

# 这个账户已消费的 token 数：qfctl usage 第二行的第 4 列（制表符分隔）。
function Get-Consumed {
    $row = (Invoke-Qfctl @('usage', '-account', $account) | Select-Object -Skip 1 -First 1) -split "`t"
    return $row[3]
}

# Python 3.10 起能跑核对脚本。Windows 上的 python 可能只是应用商店的占位程序，所以要真的运行一下。
function Find-Python {
    foreach ($name in 'python', 'python3', 'py') {
        $command = Get-Command $name -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
        if (-not $command) { continue }
        $null = & $command.Source -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)'
        if ($LASTEXITCODE -eq 0) { return $command.Source }
    }
    throw '找不到 Python 3.10 或更新的版本（python、python3、py 都不能用）。请先安装 Python；核对脚本只用标准库，不需要装包。'
}

# 发一次流式聊天：curl.exe 边收边写进 $tempDir 下的文件（Invoke-WebRequest 会攒到最后），返回 x-request-id 和流的原文。
function Invoke-QFStream {
    param([string]$Key, [string]$Name, [string]$Body, [string[]]$Extra = @())
    $bodyFile = Join-Path $tempDir "$Name.json"
    $outFile = Join-Path $tempDir "$Name.sse"
    $headerFile = Join-Path $tempDir "$Name.headers"
    [IO.File]::WriteAllText($bodyFile, $Body)
    # 只用 -s（不带 -S）：读 2 秒就断开那一次 curl 会以 28 退出，不往 stderr 写（5.1 下原生命令的 stderr 会变成 PowerShell 错误）。
    & curl.exe -s -N -o $outFile -D $headerFile -H "Authorization: Bearer $Key" -H 'Content-Type: application/json' `
        --data-binary "@$bodyFile" @Extra 'http://127.0.0.1:8080/v1/chat/completions'
    $id = (Get-Content $headerFile | Where-Object { $_ -match '^x-request-id:' } | Select-Object -First 1) -replace '^x-request-id:\s*', ''
    $raw = if (Test-Path $outFile) { [IO.File]::ReadAllText($outFile) } else { '' }
    return @{ Id = $id.Trim(); Raw = $raw }
}

# 把流的原文切成事件里的 data（多个 data 行用 \n 连起来）。
function Get-StreamData {
    param([string]$Raw)
    $events = [regex]::Split($Raw, "\r?\n\r?\n") | Where-Object { $_ -ne '' }
    return @($events | ForEach-Object {
            (($_ -split "\r?\n") | Where-Object { $_ -like 'data:*' } | ForEach-Object { $_.Substring(5).TrimStart(' ') }) -join "`n"
        } | Where-Object { $_ -ne '' })
}

function Get-QFCall {
    param([string]$Key, [string]$Id)
    return Invoke-RestMethod -Uri "http://127.0.0.1:8080/v1/calls/$Id" -Headers @{ Authorization = "Bearer $Key" } -UseBasicParsing
}

# -Stream 的三次检查，见文件开头。返回是否全部成立。
function Invoke-StreamCheck {
    param([string]$Key)
    $passed = $true
    $plain = Invoke-QFStream -Key $Key -Name 'stream-plain' -Body '{"model":"qwen-plus","stream":true,"messages":[{"role":"user","content":"Reply with one word: ok"}],"max_tokens":8}'
    $data = Get-StreamData $plain.Raw
    $emptyChoices = @($data | Where-Object { $_ -ne '[DONE]' -and @(($_ | ConvertFrom-Json).choices).Count -eq 0 }).Count
    $crlf = $plain.Raw.Contains("`r`n")
    Start-Sleep -Seconds 1 # 收尾在流结束之后
    $call = Get-QFCall -Key $Key -Id $plain.Id
    Write-Host "  不带 stream_options：$($data.Count) 个 data，最后一个是 [DONE] $($data[-1] -eq '[DONE]')，choices 为空的 $emptyChoices 个，行尾 \r\n $crlf；调用 $($call.status)，用量 $($call.usage | ConvertTo-Json -Compress)"
    $passed = $passed -and ($data[-1] -eq '[DONE]') -and ($emptyChoices -eq 0) -and ($call.status -eq 'SETTLED')

    $wants = Invoke-QFStream -Key $Key -Name 'stream-usage' -Body '{"model":"qwen-plus","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"Reply with one word: ok"}],"max_tokens":8}'
    $data = Get-StreamData $wants.Raw
    $index = -1
    for ($i = 0; $i -lt $data.Count; $i++) { if ($data[$i] -ne '[DONE]' -and $null -ne ($data[$i] | ConvertFrom-Json).usage) { $index = $i } }
    $usage = if ($index -ge 0) { ($data[$index] | ConvertFrom-Json) } else { $null }
    Start-Sleep -Seconds 1
    $call = Get-QFCall -Key $Key -Id $wants.Id
    $fromEnd = if ($index -ge 0) { $data.Count - $index } else { 0 }
    Write-Host "  带 include_usage：$($data.Count) 个 data，usage 在倒数第 $fromEnd 个，它的 choices 是空数组 $($null -ne $usage -and @($usage.choices).Count -eq 0)；调用 $($call.status)，用量和 usage 事件相同 $($null -ne $usage -and $call.usage.total_tokens -eq $usage.usage.total_tokens)"
    $passed = $passed -and ($null -ne $usage) -and ($call.status -eq 'SETTLED') -and ($call.usage.total_tokens -eq $usage.usage.total_tokens)

    $gone = Invoke-QFStream -Key $Key -Name 'stream-gone' -Extra @('--max-time', '2') -Body '{"model":"qwen-plus","stream":true,"messages":[{"role":"user","content":"Count from 1 to 300, one number per line."}],"max_tokens":1200}'
    $data = Get-StreamData $gone.Raw
    $status = ''
    for ($i = 0; $i -lt 30 -and $status -ne 'SETTLED' -and $status -ne 'USAGE_UNKNOWN'; $i++) {
        Start-Sleep -Seconds 1
        $status = (Get-QFCall -Key $Key -Id $gone.Id).status
    }
    $call = Get-QFCall -Key $Key -Id $gone.Id
    Write-Host "  读 2 秒就断开：断开前收到 $($data.Count) 个 data，最后一个是 [DONE] $($data.Count -gt 0 -and $data[-1] -eq '[DONE]')；调用 $($call.status)，用量 $($call.usage | ConvertTo-Json -Compress)"
    return $passed -and ($call.status -eq 'SETTLED')
}

# 逐个 run 对账，再校验账本。返回退出码：1 表示对不上或校验失败，2 表示核对跑不了。
function Invoke-Reconcile {
    param([string]$SummaryPath)
    $containerId = Invoke-Compose @('ps', '--status', 'running', '-q', 'quotaforge') | Select-Object -First 1
    if (-not $containerId -or -not (Test-Path $dbDir)) {
        throw '真实栈没有在运行：请先用 .\scripts\compose-real.ps1 起 QF，再跑 QS 的真实演示。'
    }
    $python = Find-Python
    Write-Host "== QS 摘要对 QF 账本，逐个 run（项目 $project，账户 $account）"
    & $python (Join-Path $PSScriptRoot 'check_qs_runs.py') --project $project --account $account $SummaryPath | Out-Host
    $exitCode = $LASTEXITCODE
    $env:QF_DB_SECRETS_DIR = $dbDir.Replace('\', '/')
    Write-Host '== qfctl verify'
    $verify = Invoke-Compose @('run', '--rm', '-T', '--no-deps', 'qfctl', 'verify')
    $verifyExit = $LASTEXITCODE
    $verify | ForEach-Object { Write-Host "  $_" }
    if ($exitCode -eq 0 -and $verifyExit -ne 0) { $exitCode = 1 }
    return $exitCode
}

$previousLocation = Get-Location
$previousEnv = @{}
foreach ($name in $envNames) { $previousEnv[$name] = [Environment]::GetEnvironmentVariable($name, 'Process') }
Set-Location $qfDir
# 只带主文件的命令（qfctl、ps、down）会把 compose.real.yaml 里的 qf-redis-cache 当成孤儿，每次都警告一行；关掉这个警告。
# 不影响清理：down 带着 --remove-orphans，照样删掉它。
$env:COMPOSE_IGNORE_ORPHANS = 'true'
try {
    docker info --format '{{.ServerVersion}}' | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Docker 没有运行：请先启动 Docker Desktop。' }

    if ($summaryPath) {
        exit (Invoke-Reconcile $summaryPath)
    }
    if ($Stop) {
        Remove-Previous
        Remove-Item Env:QUERYSHIELD_MODEL_API_KEY -ErrorAction SilentlyContinue # 不管原来有没有值
        Write-Host '已停止 QF，数据库的数据和临时目录已删除，QUERYSHIELD_MODEL_API_KEY 已清掉。'
        return
    }
    if ((Invoke-Compose @('ps', '-a', '-q')) -or (Test-Path $tempDir)) {
        Write-Host '发现上一次没有 -Stop 的残留，先清理（容器、数据库的数据、临时目录），再重新开始。'
        Remove-Previous
    }
    if (-not (docker network ls --filter 'name=^qs-qf$' --format '{{.Name}}')) {
        Invoke-Native '创建网络 qs-qf' { docker network create qs-qf | Out-Null }
    }

    if (-not $UpstreamBaseUrl) {
        $UpstreamBaseUrl = Read-Host -Prompt '百炼的 base URL（例如 https://dashscope.aliyuncs.com/compatible-mode/v1）'
    }
    if (-not $UpstreamBaseUrl) { throw '上游地址为空。' }
    $env:QF_UPSTREAM_BASE_URL = $UpstreamBaseUrl
    $env:QF_UPSTREAM_API_KEY = Read-HiddenSecret -Prompt '百炼 Key（输入不显示）'
    if (-not $env:QF_UPSTREAM_API_KEY) { throw '百炼 Key 为空。' }

    $env:QF_DB_SECRETS_DIR = New-DatabaseSecrets
    $services = @('quotaforge', 'qf-redis-cache')
    if ($NoCache) {
        $env:QF_REAL_CONFIG = New-NoCacheConfig
        $services = @('quotaforge')
    }

    Invoke-Native '构建 QF 镜像' { Invoke-Compose @('build', 'quotaforge', 'qfctl') }
    Invoke-Native '启动 QF' { Invoke-Compose (@('-f', 'compose.yaml', '-f', 'compose.real.yaml', 'up', '-d', '--wait') + $services) }
    Invoke-Qfctl (@('account', 'create', '-name', $account, '-limit', "$AccountLimit") + $accountRate) | Out-Null
    $issued = Invoke-Qfctl (@('key', 'issue', '-account', $account, '-models', 'qwen-plus,text-embedding-v4') + $keyRate)
    $qfKey = ($issued | Where-Object { $_ -like 'key: *' }) -replace '^key: ', ''
    if (-not $qfKey) { throw 'qfctl key issue 的输出格式不对' }
    $env:QUERYSHIELD_MODEL_API_KEY = $qfKey # 不在 $envNames 里：脚本结束时不恢复，留给同一个窗口里的 QS 脚本
    Write-Host 'QF 已启动：本机 http://127.0.0.1:8080/v1，qs-qf 网络上 http://quotaforge:8080/v1。'
    Write-Host "账户 $account 的额度上限 $AccountLimit tokens（-AccountLimit 可改）。"
    if ($NoCache) {
        Write-Host '嵌入缓存关着（-NoCache）：QF 用的配置去掉了 embedding_cache，和开缓存之前的配置逐字节相同；没有起 qf-redis-cache。'
    }
    else {
        Write-Host '嵌入缓存开着（qf-redis-cache，每次启动都是新的）：只影响不带 Idempotency-Key 的嵌入请求；-NoCache 关掉。'
    }
    Write-Host 'QF Key 已放进这个窗口的环境变量 QUERYSHIELD_MODEL_API_KEY（不显示，只在这个窗口里有效）。在同一个窗口里运行 QS 的 compose-real-demo.ps1，它会直接读取，不用粘贴；-Stop 时清掉。'
    Write-Host 'quotaforge 容器里的代理变量（只显示设没设）：'
    Show-ProxyVariables

    if ($Check) {
        Write-Host '经 QF 各发一次聊天和嵌入（聊天带一个新的 Idempotency-Key）：'
        $chatBody = '{"model":"qwen-plus","messages":[{"role":"user","content":"Reply with one word: ok"}],"max_tokens":8}'
        $embeddingBody = '{"model":"text-embedding-v4","input":["ok"]}'
        $idempotencyKey = "real-check-$([guid]::NewGuid().ToString('N'))"
        $first = Invoke-QF -Path '/v1/chat/completions' -Key $qfKey -Body $chatBody -IdempotencyKey $idempotencyKey
        $embedding = Invoke-QF -Path '/v1/embeddings' -Key $qfKey -Body $embeddingBody
        $checkPassed = [bool]$first -and [bool]$embedding # 任何一次请求不是 2xx 就没通过（Invoke-QF 返回 $null）
        if ($first) {
            # 同一个键、同样的请求体再发一遍：QF 重放第一次的响应，不调上游、不扣额度。
            $consumedBefore = Get-Consumed
            Write-Host '用同一个 Idempotency-Key 再发一遍聊天：'
            $second = Invoke-QF -Path '/v1/chat/completions' -Key $qfKey -Body $chatBody -IdempotencyKey $idempotencyKey
            $consumedAfter = Get-Consumed
            $checkPassed = $checkPassed -and [bool]$second -and ($consumedAfter -eq $consumedBefore)
            if ($second) {
                $sameId = "$($second.Headers['x-request-id'])" -eq "$($first.Headers['x-request-id'])"
                $sameBody = $second.Content -eq $first.Content
                Write-Host "  重放：Idempotent-Replayed $($second.Headers['Idempotent-Replayed'])，x-request-id 和第一次相同 $sameId，正文相同 $sameBody；已消费 $consumedBefore → $consumedAfter"
                $checkPassed = $checkPassed -and $sameId -and $sameBody
            }
        }
        if ($embedding -and $NoCache) {
            # Cache-Status 只在开着缓存时才加：没有它，说明这次运行确实没查缓存。
            $noHeader = -not $embedding.Headers['Cache-Status']
            Write-Host "嵌入缓存关着（-NoCache）：嵌入的响应没有 Cache-Status 头 $noHeader"
            $checkPassed = $checkPassed -and $noHeader
        }
        elseif ($embedding) {
            # 同样的嵌入（不带 Idempotency-Key）再发一遍：命中嵌入缓存，不调上游、不扣额度，记一条 CACHED。
            $consumedBefore = Get-Consumed
            Write-Host '同一个嵌入再发一遍（应当命中嵌入缓存）：'
            $again = Invoke-QF -Path '/v1/embeddings' -Key $qfKey -Body $embeddingBody
            $consumedAfter = Get-Consumed
            $checkPassed = $checkPassed -and [bool]$again -and ($consumedAfter -eq $consumedBefore)
            if ($again) {
                $hit = "$($again.Headers['Cache-Status'])" -eq 'quotaforge; hit'
                $sameBody = $again.Content -eq $embedding.Content
                $status = (Get-QFCall -Key $qfKey -Id "$($again.Headers['x-request-id'])").status
                Write-Host "  命中：Cache-Status 是 quotaforge; hit $hit，正文和第一次相同 $sameBody，调用状态 $status；已消费 $consumedBefore → $consumedAfter"
                $checkPassed = $checkPassed -and $hit -and $sameBody -and ($status -eq 'CACHED')
            }
        }
        # qfctl usage 的第二行：account limit reserved consumed remaining、各状态的调用数、限流限额，最后一列是 CACHED，以制表符分隔
        $row = (Invoke-Qfctl @('usage', '-account', $account) | Select-Object -Skip 1 -First 1) -split "`t"
        Write-Host "账户 $account 的额度：上限 $($row[1])，已消费 $($row[3])，剩余 $($row[4]) tokens；嵌入缓存命中（CACHED）$($row[12]) 次"
        # 限流限额（0 表示不限）：qfctl key list 的第 5、6 列，usage 的第 11、12 列（rpm_limit、tpm_limit）
        $keyRow = (Invoke-Qfctl @('key', 'list', '-account', $account) | Select-Object -Skip 1 -First 1) -split "`t"
        Write-Host "限流（每分钟）：这把 Key 最多 $($keyRow[4]) 个请求、$($keyRow[5]) tokens；账户最多 $($row[10]) 个请求、$($row[11]) tokens"
        if (-not $checkPassed) {
            throw '-Check 没有全部通过，见上面的输出。QF 还开着，看完日志再运行 -Stop。'
        }
    }
    if ($Stream) {
        Write-Host '经 QF 发三次流式聊天（只显示流的结构）：'
        if (-not (Invoke-StreamCheck -Key $qfKey)) {
            throw '-Stream 没有全部通过，见上面的输出。QF 还开着，看完日志再运行 -Stop。'
        }
    }
}
finally {
    # 原来没设的要真正删除：PowerShell 7.6 上 SetEnvironmentVariable(名字, $null) 只把值设成空串，变量还在。
    foreach ($name in $envNames) {
        if ($null -eq $previousEnv[$name]) { Remove-Item "Env:$name" -ErrorAction SilentlyContinue }
        else { [Environment]::SetEnvironmentVariable($name, $previousEnv[$name], 'Process') }
    }
    Set-Location $previousLocation
}

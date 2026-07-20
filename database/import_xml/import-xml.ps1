param(
    [string]$ApiBaseUrl = "",
    [string]$XmlPath = "/data/xml"
)

if ([string]::IsNullOrWhiteSpace($ApiBaseUrl)) {
    $apiPort = $env:OPENPLOT_API_PORT
    if ([string]::IsNullOrWhiteSpace($apiPort)) {
        $apiPort = "7011"
    }
    $ApiBaseUrl = "http://localhost:$apiPort"
}

$ErrorActionPreference = "Stop"

$LoginBody = @{
    username = "renan.dev"
    password = "Renan@1234"
} | ConvertTo-Json

Write-Host "Autenticando em $ApiBaseUrl/api/v1/auth/login..."

$LoginResponse = Invoke-RestMethod `
    -Uri "$ApiBaseUrl/api/v1/auth/login" `
    -Method POST `
    -ContentType "application/json" `
    -Body $LoginBody `
    -SessionVariable OpenPlotSession

Write-Host "Login realizado."

# Tenta localizar token em formatos comuns de resposta
$Token = $null

if ($LoginResponse.token) {
    $Token = $LoginResponse.token
}
elseif ($LoginResponse.accessToken) {
    $Token = $LoginResponse.accessToken
}
elseif ($LoginResponse.jwt) {
    $Token = $LoginResponse.jwt
}
elseif ($LoginResponse.data.token) {
    $Token = $LoginResponse.data.token
}
elseif ($LoginResponse.data.accessToken) {
    $Token = $LoginResponse.data.accessToken
}

$Headers = @{
    accept = "*/*"
}

if ($Token) {
    Write-Host "JWT encontrado. Enviando Authorization Bearer."
    $Headers["Authorization"] = "Bearer $Token"
}
else {
    Write-Host "JWT não encontrado na resposta. Usando sessão/cookie."
}

$ImportBody = @{
    path = $XmlPath
} | ConvertTo-Json

Write-Host "Chamando importação XML para path=$XmlPath..."

$ImportResponse = Invoke-RestMethod `
    -Uri "$ApiBaseUrl/api/v1/xml/import" `
    -Method POST `
    -ContentType "application/json" `
    -Headers $Headers `
    -Body $ImportBody `
    -WebSession $OpenPlotSession

Write-Host "Resposta da importação:"
$ImportResponse | ConvertTo-Json -Depth 20
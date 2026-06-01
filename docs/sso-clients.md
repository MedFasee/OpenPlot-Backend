# SSO Clients no OpenPlot

## Objetivo

Manter o login padrão do OpenPlot em `/api/v1/auth/login` e adicionar um segundo fluxo de autenticação para clientes SSO server-to-server, começando pelo `multiinfeed`.

## Fluxo implementado

1. O cliente externo chama `POST /api/v1/sso/{clientId}/link`.
2. O OpenPlot valida `X-Client-Id`, `X-Timestamp`, `X-Nonce` e `X-Signature`.
3. O nonce é persistido para proteção anti-replay.
4. O OpenPlot cria um token temporário de uso único em `openplot.sso_login_token`.
5. A resposta devolve uma URL do front do MedPlot/OpenPlot com o token temporário.
6. O front chama `POST /api/v1/sso/consumir-token`.
7. O OpenPlot consome o token, resolve o usuário técnico fixo configurado para o cliente SSO e emite o JWT padrão da aplicação.

## Configuração

### appsettings

```json
{
  "Sso": {
	"FrontendBaseUrl": "https://medplot.local",
	"ConsumePath": "/sso/consumir",
	"RedirectPathTemplate": "/processar-busca?consultaId={consultaId}",
	"RequestTtlSeconds": 300,
	"LoginTokenTtlSeconds": 120
  },
  "SsoClients": [
	{
	  "ClientId": "multiinfeed",
	  "Secret": "secret_compartilhado",
	  "TechnicalUsername": "MIF_robo"
	}
  ]
}
```

## Assinatura HMAC

A assinatura usa HMAC SHA256 sobre a mensagem canônica:

```txt
{HTTP_METHOD}\n{REQUEST_PATH}\n{TIMESTAMP_ISO_8601_UTC}\n{NONCE}\n{SHA256_HEX_DO_BODY}
```

Headers obrigatórios:

- `X-Client-Id`
- `X-Timestamp`
- `X-Nonce`
- `X-Signature`

A assinatura recebida aceita Base64 ou hexadecimal.

## Usuário técnico por cliente

Cada cliente SSO aponta para um usuário técnico local do OpenPlot via `TechnicalUsername`.

No consumo do token, o JWT emitido pelo OpenPlot recebe claims adicionais:

- `auth_flow = sso`
- `origin_client = {clientId}`
- `consulta_id = {consultaId}`

## Persistência

As tabelas usadas pelo fluxo estão documentadas em `sso_tables.sql` e também são criadas automaticamente pela API caso ainda não existam.

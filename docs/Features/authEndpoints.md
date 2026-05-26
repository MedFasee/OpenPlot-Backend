# Auth Endpoints - Documentação Técnica

## Visão Geral

A feature de autenticação continua responsável pelo login interativo padrão, emissão do JWT de acesso e encerramento da sessão atual.

Com a entrada do fluxo por `SSO_client`, a feature `Auth` deixou de ser o único ponto de entrada de autenticação: o login tradicional continua em `/api/v1/auth/login`, enquanto o login SSO é concluído em `/api/v1/sso/consumir-token` após a validação prévia do cliente SSO.

## Responsabilidade da Feature

A feature `Auth` concentra:

- validação de credenciais no login padrão;
- criação da identidade autenticada;
- emissão de JWT;
- projeção de dados do usuário para consumo do front;
- encerramento de sessão e limpeza do cookie/token.

No cenário com `SSO_client`, a emissão final do JWT e a abertura da sessão reutilizam os mesmos contratos de login (`LoginResponse`, `LoginEnvelope` e `ISessionUserService`), mas a resolução do usuário passa a ser feita pelo fluxo SSO.

## Componentes Principais

- **`AuthEndpoints`**: expõe as rotas HTTP de login padrão e logout.
- **`IAuthService`**: autentica usuário e senha no fluxo tradicional.
- **`SsoEndpoints`**: expõe o fluxo de autenticação por cliente SSO.
- **`ISsoIdentityService`**: resolve o usuário técnico associado ao `SSO_client`.
- **`ISessionUserService`**: persiste o usuário autenticado em sessão.
- **`IOpenPlotLoginTokenService`**: gera o JWT e o envelope de resposta usados tanto no login padrão quanto no login SSO.
- **`JwtOptions`**: define emissor, audiência, chave e expiração do token.

---

## Endpoints

## `POST /api/v1/auth/login`

Autentica o usuário com credenciais locais e retorna um envelope com token JWT e dados básicos do usuário.

### Entrada
- Body: `LoginRequest`
  - `username`
  - `password`

### Fluxo técnico
1. Chama `IAuthService.AuthenticateAsync(...)`.
2. Armazena o usuário na sessão via `ISessionUserService`.
3. Gera o JWT por `IOpenPlotLoginTokenService`.
4. Retorna `ApiResponse<LoginEnvelope>`.

### Retorno
- `200` com token e dados do usuário.
- `401` em caso de falha de autenticação.

### Observações
- A expiração do token é configurada por `Jwt:ExpirationHours`.
- O mapeamento de role é simplificado para `admin`, `editor` ou `reader`.
- Este endpoint permanece dedicado ao login por `username` e `password`.

---

## `POST /api/v1/sso/consumir-token`

Conclui a autenticação quando a origem é um `SSO_client` já validado pelo fluxo `/api/v1/sso/{clientId}/link`.

### Entrada
- Body: `ConsumeSsoTokenRequest`
  - `token`

### Fluxo técnico
1. Consome o token temporário de uso único persistido para o cliente SSO.
2. Resolve o usuário técnico configurado para o cliente via `ISsoIdentityService`.
3. Acrescenta claims de contexto do SSO, como `auth_flow`, `origin_client` e `consulta_id`.
4. Armazena o usuário resolvido na sessão via `ISessionUserService`.
5. Gera o JWT por `IOpenPlotLoginTokenService`.
6. Retorna `ApiResponse<ConsumeSsoTokenResponse>` com dados de login e redirecionamento.

### Retorno
- `200` com `consultaId`, `originClient`, `redirectPath` e `login`.
- `400` quando o token temporário não é informado.
- `401` quando o token temporário é inválido, expirou, já foi consumido ou o usuário técnico do cliente não pode ser resolvido.

### Observações
- O endpoint não recebe `username` e `password`.
- O usuário autenticado é determinado pelo `SSO_client` configurado no backend.
- O JWT final mantém o mesmo formato do login padrão, com claims adicionais específicas do fluxo SSO.

---

## `POST /api/v1/auth/logout`

Encerra a sessão atual do usuário autenticado, independentemente de a sessão ter sido aberta pelo login padrão ou pelo fluxo `SSO_client`.

### Entrada
- Não requer body.

### Fluxo técnico
1. Obtém o usuário atual da sessão.
2. Se não houver usuário em sessão, retorna `401`.
3. Limpa a sessão.
4. Remove o cookie configurado para autenticação.

### Retorno
- `200` com mensagem de sessão encerrada.
- `401` se não houver sessão autenticada.

---

## Considerações de Arquitetura

- A feature não contém regra de domínio analítico; sua função é exclusivamente de autenticação/autorização de acesso.
- O JWT pode ser emitido tanto pelo fluxo padrão em `AuthEndpoints` quanto pelo fluxo SSO em `SsoEndpoints`.
- A sessão HTTP e o token coexistem como mecanismos de contexto/autenticação.
- O fluxo com `SSO_client` separa a validação do cliente externo da resolução do usuário final, preservando o contrato de login consumido pelo front.
- Para detalhes do handshake server-to-server e da assinatura HMAC, consultar `docs/sso-clients.md`.

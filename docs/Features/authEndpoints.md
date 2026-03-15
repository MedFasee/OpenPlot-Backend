# Auth Endpoints - Documentação Técnica

## Visão Geral

A feature de autenticação é responsável por autenticar o usuário, emitir o token JWT de acesso e encerrar a sessão atual.

## Responsabilidade da Feature

A feature `Auth` concentra:

- validação de credenciais;
- criação de identidade autenticada;
- emissão de JWT;
- projeção de dados do usuário para consumo do front;
- encerramento de sessão e limpeza do cookie/token.

## Componentes Principais

- **`AuthEndpoints`**: expõe as rotas HTTP da feature.
- **`IAuthService` / `AuthService`**: executa autenticação do usuário.
- **`ISessionUserService`**: persiste o usuário autenticado em sessão.
- **`JwtOptions`**: define emissor, audiência, chave e expiração do token.

---

## Endpoints

## `POST /api/v1/auth/login`

Autentica o usuário e retorna um envelope com token JWT e dados básicos do usuário.

### Entrada
- Body: `LoginRequest`
  - `username`
  - `password`

### Fluxo técnico
1. Chama `IAuthService.AuthenticateAsync(...)`.
2. Armazena o usuário na sessão via `ISessionUserService`.
3. Monta claims do token JWT.
4. Assina o token com `SigningKey` configurada.
5. Retorna `ApiResponse<LoginEnvelope>`.

### Retorno
- `200` com token e dados do usuário.
- `401` em caso de falha de autenticação.

### Observações
- A expiração do token é configurada por `Jwt:ExpirationHours`.
- O mapeamento de role é simplificado para `admin`, `editor` ou `reader`.

---

## `POST /api/v1/auth/logout`

Encerra a sessão atual do usuário autenticado.

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
- O JWT é emitido na borda HTTP (`AuthEndpoints`) a partir dos dados retornados pelo serviço de autenticação.
- A sessão HTTP e o token coexistem como mecanismos de contexto/autenticação.

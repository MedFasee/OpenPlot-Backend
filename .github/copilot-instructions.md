# Copilot Instructions

## Diretrizes de projeto
- O usuário prefere que o docker compose suba tudo que for necessário com mínima execução de scripts externos, mesmo que isso exija ajustes no código da API.
- O ambiente Docker do projeto será executado em uma máquina Linux; ao revisar o compose e a configuração do banco, priorizar compatibilidade com Linux.
- O banco no ambiente Docker Compose deve continuar usando o nome `postgres`; ao ajustar compose e conexões do ambiente, não trocar o nome do database para `openplot`.
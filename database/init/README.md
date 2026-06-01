# Bootstrap do banco OpenPlot

## Estado atual
- O restore inicial do banco é feito por `01-restore.sh`.
- Após o restore, o script já garante de forma idempotente o índice único `ux_signal_pdc_pmu_name_phase_component` em `openplot.signal`.
- A API também garante esse índice na inicialização para complementar o bootstrap do ambiente.
- O sistema não depende mais de `search_path` para executar o upsert de sinais.

## Impacto operacional
- Não é mais necessário executar manualmente um script de `search_path` após criar o banco.
- Os fluxos de importação (`OpenPlot.XmlImporter` e importação XML da API) também garantem o índice antes de usar `ON CONFLICT` em `openplot.signal`.

## Observação
- Se o banco for recriado pelo fluxo padrão do projeto, o índice será provisionado automaticamente durante o restore.

# Marco: ajuste de bootstrap do GSF e eliminação de warnings

## Contexto
A solução apresentava uma falha de inicialização no projeto `OpenPlot.Ingestor.Gsf` causada por incompatibilidade entre dependências da família Gemstone/SnapDB, além de warnings de compilação distribuídos entre `OpenPlot.Api` e `OpenPlot.Ingestor.Gsf`.

## O que mudou

### OpenPlot.Ingestor.Gsf
- Alinhamento explícito dos pacotes `Gemstone.*` para a versão `1.0.173`.
- Promoção da dependência transitiva `System.Security.Cryptography.Xml` para `8.0.3` para remover avisos de vulnerabilidade.
- Remoção de referências de pacote desnecessárias que geravam `NU1510`.
- Ativação de `Nullable` no projeto.
- Correções de nulabilidade e contratos de comparação em:
  - `Data/TimeSeries.cs`
  - `DbSystemDataFactory.cs`
  - `Hosting/IngestorJobProcessor.cs`
  - `Repository/HistorianDataFetcher.cs`
  - `Repository/MeasurementHistorian.cs`
  - `Repository/MeasurementHistorian2.cs`
  - `Repository/MeasurementMedFasee.cs`
  - `Channel.cs`
  - `Snap/HistorianKey.cs`
  - `Snap/HistorianValue.cs`

### OpenPlot.Api
- Correções de nulabilidade em `Data/TimeSeries.cs`.
- Ajuste de fallback obrigatório em `Features/Auth/AuthEndpoints.cs`.
- Remoção de `catch` com variável não utilizada em handlers.
- Tratamento seguro de `SelectRate` anulável nos handlers de séries.

## Motivo das mudanças
- Eliminar a exceção de bootstrap no acesso ao SnapDB/Gemstone no início do worker.
- Deixar os projetos ajustados compilando sem warnings.
- Melhorar a consistência de nulabilidade e reduzir risco de falhas em runtime.
- Remover dependências redundantes e corrigir aviso de segurança em pacote transitivo.

## Resultado esperado
- `OpenPlot.Ingestor.Gsf` inicializa sem a exceção `RuntimeBinderException` observada anteriormente.
- `OpenPlot.Api` compila em Release sem warnings.
- `OpenPlot.Ingestor.Gsf` compila em Release sem warnings.
- A solução fica pronta para validação final consolidada.

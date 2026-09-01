namespace OpenPlot.Features.Runs.Repositories;

// Chaves fortemente tipadas por familia de consulta, consumidas pelo
// IQueryExecutionCoordinator. Cada uma inclui os parametros que alteram a
// projecao/SQL: quantity/component/phaseMode/phase (quando aplicavel), PMUs
// normalizadas, janela temporal e a decisao de sampling (UseRaw/BucketWidth/
// SourceRelation), para nunca compartilhar RAW com preview.
public readonly record struct SimpleQueryKey(
    int PdcId,
    string Quantity,
    string Component,
    string PhaseMode,
    string Phase,
    string PmuKey,
    long FromTicks,
    long ToTicks,
    bool UseRaw,
    long BucketTicks,
    string SourceRelation);

public readonly record struct PhasorQueryKey(
    int PdcId,
    string Quantity,
    string Component,
    string PhaseMode,
    string Phase,
    string PmuKey,
    long FromTicks,
    long ToTicks,
    bool UseRaw,
    long BucketTicks,
    string SourceRelation);

public readonly record struct AngleFramesQueryKey(
    int PdcId,
    string Kind,
    string Phase,
    long FromTicks,
    long ToTicks,
    string PmuKey,
    bool UseRaw,
    long BucketTicks,
    string SourceRelation);

public readonly record struct PowerFramesQueryKey(
    int PdcId,
    string PmuKey,
    long FromTicks,
    long ToTicks,
    bool UseRaw,
    long BucketTicks,
    string SourceRelation);

// QueryAbcMagAngAsync nao possui nenhum handler ativo chamando-o hoje
// (confirmado por busca no codigo-fonte: so aparece na interface e na propria
// implementacao). Definimos a chave por documentacao/prontidao, mas o metodo
// permanece fora do QueryExecutionCoordinator ate que surja um consumidor real,
// conforme instrucao explicita de nao alterar codigo comprovadamente morto.
public readonly record struct AbcQueryKey(
    int PdcId,
    string Kind,
    string PmuKey,
    long FromTicks,
    long ToTicks,
    bool UseRaw,
    long BucketTicks,
    string SourceRelation);

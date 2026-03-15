# Import Endpoints - Documentação Técnica

## Visão Geral

A feature `Import` disponibiliza a carga de arquivos XML de configuração para popular o banco com PDCs, PMUs e sinais.

## Responsabilidade da Feature

A feature concentra:

- recebimento do caminho de um XML ou diretório;
- invocação do importador XML;
- retorno do resumo técnico da importação.

## Componentes Principais

- **`ImportEndpoints`**: rotas HTTP da feature.
- **`OpenPlot.XmlImporter.XmlImporter`**: componente responsável pela leitura e persistência do XML.
- **`ImportXmlRequest`**: contrato de entrada com o caminho a importar.

---

## Endpoint

## `POST /api/v1/xml/import`

Executa a importação de um arquivo XML ou de uma pasta contendo XMLs.

### Entrada
- Body: `ImportXmlRequest`
  - `path`

### Fluxo técnico
1. Valida se `path` foi informado.
2. Resolve a connection string do banco.
3. Instancia `XmlImporter`.
4. Executa `RunAsync(path, ct)`.
5. Retorna o resumo da importação.

### Retorno
- `200` com `{ status, data }`, onde `data` contém a lista de resumos por arquivo.
- `400` se `path` não for informado.

---

## Considerações de Arquitetura

- A lógica pesada de parsing e persistência foi mantida fora da feature HTTP, no projeto `OpenPlot.XmlImporter`.
- A feature atua como adaptador de API para o importador.
- É adequada para bootstrap e atualização de metadados do ambiente.

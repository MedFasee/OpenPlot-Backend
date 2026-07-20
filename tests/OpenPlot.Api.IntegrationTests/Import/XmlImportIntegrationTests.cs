using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace OpenPlot.Api.IntegrationTests.Import;

public sealed class XmlImportIntegrationTests
{
  private const string ApiBaseUrl = "http://localhost:7011/api/v1/";
  private const string PdcName = "it-kind-pdc-dev-http";
  private const string PmuIdName = "it-kind-pmu-dev-http";

    [Fact]
  public async Task ImportAsync_WhenSignalsMapToFvih_ExposesKindOnCatalogEndpoint()
    {
    using var http = new HttpClient { BaseAddress = new Uri(ApiBaseUrl) };
    var configDirectory = GetConfigDirectory();
    var fileName = $"{PdcName}.xml";
    var hostXmlPath = Path.Combine(configDirectory, fileName);
    var containerXmlPath = $"/data/xml/{fileName}";

        try
        {
      await File.WriteAllTextAsync(hostXmlPath, CreateXml(PdcName, PmuIdName));

      var token = await LoginAsync(http);
      http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);

      var importResponse = await http.PostAsync(
        "xml/import",
        new StringContent($"{{\"path\":\"{containerXmlPath}\"}}", Encoding.UTF8, "application/json"));

      importResponse.EnsureSuccessStatusCode();

            var terminalsResponse = await http.GetAsync($"configs/{PdcName}/terminals");
      terminalsResponse.EnsureSuccessStatusCode();

      using var json = JsonDocument.Parse(await terminalsResponse.Content.ReadAsStringAsync());
      var tipo = json.RootElement
        .GetProperty("data")
        .GetProperty("terminais")[0]
        .GetProperty("estados")[0]
        .GetProperty("tensoes")[0]
        .GetProperty("estacoes")[0]
        .GetProperty("terminais")[0]
        .GetProperty("tipo")
        .GetString();

      Assert.Equal("FVIH", tipo);
        }
        finally
        {
      if (File.Exists(hostXmlPath))
        File.Delete(hostXmlPath);
        }
    }

  private static async Task<string> LoginAsync(HttpClient http)
    {
    var body = new StringContent(
      "{\"username\":\"renan.dev\",\"password\":\"Renan@1234\"}",
      Encoding.UTF8,
      "application/json");

    var response = await http.PostAsync("auth/login", body);
    response.EnsureSuccessStatusCode();

    using var json = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
    return json.RootElement.GetProperty("data").GetProperty("token").GetString()
      ?? throw new InvalidOperationException("Token JWT não retornado pelo login do ambiente .dev.");
    }

  private static string GetConfigDirectory()
    => Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", "Config"));

    private static string CreateXml(string pdcName, string pmuIdName) => $"""
<openplot>
  <pdc>
    <name>{pdcName}</name>
    <type>integration-test</type>
    <address>127.0.0.1</address>
    <fps>60</fps>
  </pdc>
  <pmu>
    <idName>{pmuIdName}</idName>
    <fullName>PMU Integration Test</fullName>
    <voltLevel>230000</voltLevel>
    <idNumber>1</idNumber>
    <local>
      <area>AREA-TESTE</area>
      <state>RJ</state>
      <station>EST-TESTE</station>
      <lat>-22.9</lat>
      <lon>-43.2</lon>
    </local>
    <measurements>
      <phasor>
        <pName>VA</pName>
        <pType>Voltage</pType>
        <pPhase>A</pPhase>
        <chId>101</chId>
      </phasor>
      <phasor>
        <pName>IA</pName>
        <pType>Current</pType>
        <pPhase>A</pPhase>
        <chId>102</chId>
      </phasor>
      <freq>
        <fName>FREQ</fName>
        <fId>103</fId>
      </freq>
      <analog>
        <aType>VTHD</aType>
        <aName>VTHD-A</aName>
        <aPhase>A</aPhase>
        <aId>104</aId>
      </analog>
    </measurements>
  </pmu>
</openplot>
""";
}
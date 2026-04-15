using Microsoft.AspNetCore.Http;

namespace OpenPlot.UnitTests.Infrastructure;

internal static class ResultAssertions
{
    public static void HasStatusCode(IResult result, int expectedStatusCode)
    {
        var statusCodeResult = Assert.IsAssignableFrom<IStatusCodeHttpResult>(result);
        Assert.Equal(expectedStatusCode, statusCodeResult.StatusCode);
    }
}

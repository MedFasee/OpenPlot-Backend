using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using OpenPlot.Services.BackgroundCache;

namespace OpenPlot.UnitTests.Services.BackgroundCache;

public sealed class BackgroundCacheCoordinatorTests
{
    [Fact]
    public async Task GetOrCreateAsync_SameKeyConcurrently_ReturnsOneOwnerAndCacheId()
    {
        var coordinator = CreateCoordinator();
        var key = CreateSeqKey();

        var reservations = await Task.WhenAll(
            Enumerable.Range(0, 2)
                .Select(_ => coordinator.ReserveOrGetAsync(key, CancellationToken.None)));

        Assert.Single(reservations.Select(reservation => reservation.CacheId).Distinct());
        Assert.Single(reservations, reservation => reservation.IsOwner);
    }

    [Fact]
    public async Task ReserveOrGetAsync_DifferentKeys_ReturnsDifferentOwnerReservations()
    {
        var coordinator = CreateCoordinator();
        var positive = await coordinator.ReserveOrGetAsync(CreateSeqKey(seq: "pos"), CancellationToken.None);
        var negative = await coordinator.ReserveOrGetAsync(CreateSeqKey(seq: "neg"), CancellationToken.None);

        Assert.True(positive.IsOwner);
        Assert.True(negative.IsOwner);
        Assert.NotEqual(positive.CacheId, negative.CacheId);
    }

    [Fact]
    public async Task Complete_RemovesReservation()
    {
        var coordinator = CreateCoordinator();
        var key = CreateSeqKey();
        var reservation = await coordinator.ReserveOrGetAsync(key, CancellationToken.None);

        coordinator.Complete(key, reservation.CacheId);

        var next = await coordinator.ReserveOrGetAsync(key, CancellationToken.None);
        Assert.True(next.IsOwner);
        Assert.NotEqual(reservation.CacheId, next.CacheId);
    }

    [Fact]
    public async Task Fail_RemovesReservation()
    {
        var coordinator = CreateCoordinator();
        var key = CreateSeqKey();
        var reservation = await coordinator.ReserveOrGetAsync(key, CancellationToken.None);

        await coordinator.FailAsync(key, reservation.CacheId);

        var next = await coordinator.ReserveOrGetAsync(key, CancellationToken.None);
        Assert.True(next.IsOwner);
        Assert.NotEqual(reservation.CacheId, next.CacheId);
    }

    [Fact]
    public async Task ReserveOrGetAsync_WhenPersistedCacheExists_ReturnsHistoricalCacheIdWithoutOwner()
    {
        var repository = CreateRepository();
        var cacheId = Guid.NewGuid();
        repository.Setup(x => x.FindByCacheKeyAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ExistingCache(cacheId, CreateSeqKey().RunId));
        var coordinator = CreateCoordinator(repository);

        var reservation = await coordinator.ReserveOrGetAsync(CreateSeqKey(), CancellationToken.None);

        Assert.Equal(cacheId, reservation.CacheId);
        Assert.False(reservation.IsOwner);
        repository.Verify(x => x.ReserveAsync(It.IsAny<Guid>(), It.IsAny<Guid>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ReserveOrGetAsync_WhenRemoteInstanceWinsRace_ReusesWinnerWithoutOwner()
    {
        var repository = CreateRepository();
        var winner = Guid.NewGuid();
        repository.Setup(x => x.ReserveAsync(It.IsAny<Guid>(), It.IsAny<Guid>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(winner);
        var coordinator = CreateCoordinator(repository);

        var reservation = await coordinator.ReserveOrGetAsync(CreateSeqKey(), CancellationToken.None);

        Assert.Equal(winner, reservation.CacheId);
        Assert.False(reservation.IsOwner);
    }

    [Fact]
    public void Create_SamePmusInDifferentOrder_ReturnsEqualKeys()
    {
        var first = CreateSeqKey(pmus: new[] { " Pmu-B ", "pmu-a" });
        var second = CreateSeqKey(pmus: new[] { "PMU-A", "pmu-b" });

        Assert.Equal(first, second);
    }

    [Fact]
    public void Create_DifferentMaxPoints_DoesNotChangeIntegralCacheKey()
    {
        var maxPoints1000 = CreateSeqKey();
        var maxPoints5000 = CreateSeqKey();

        Assert.Equal(maxPoints1000, maxPoints5000);
    }

    [Fact]
    public void Create_SeqPositiveAndNegative_ReturnsDifferentKeys()
    {
        Assert.NotEqual(CreateSeqKey(seq: "pos"), CreateSeqKey(seq: "neg"));
    }

    [Fact]
    public void Create_SeqVoltageAndCurrent_ReturnsDifferentKeys()
    {
        Assert.NotEqual(CreateSeqKey(kind: "voltage"), CreateSeqKey(kind: "current"));
    }

    [Fact]
    public void Create_SeqRawAndPu_ReturnsDifferentKeys()
    {
        Assert.NotEqual(CreateSeqKey(unit: "raw"), CreateSeqKey(unit: "pu"));
    }

    [Fact]
    public async Task GetOrCreateAsync_HighConcurrency_ReturnsOneOwnerAndCacheId()
    {
        var coordinator = CreateCoordinator();
        var key = CreateSeqKey();

        var reservations = await Task.WhenAll(
            Enumerable.Range(0, 50)
                .Select(_ => coordinator.ReserveOrGetAsync(key, CancellationToken.None)));

        Assert.Single(reservations.Select(reservation => reservation.CacheId).Distinct());
        Assert.Single(reservations, reservation => reservation.IsOwner);
    }

    private static BackgroundCacheCoordinator CreateCoordinator()
    {
        return CreateCoordinator(CreateRepository());
    }

    private static BackgroundCacheCoordinator CreateCoordinator(Mock<IAnalysisCacheRepository> repository)
        => new(() => repository.Object, NullLogger<BackgroundCacheCoordinator>.Instance);

    private static Mock<IAnalysisCacheRepository> CreateRepository()
    {
        var repository = new Mock<IAnalysisCacheRepository>();
        repository.Setup(x => x.FindByCacheKeyAsync(It.IsAny<string>(), It.IsAny<CancellationToken>())).ReturnsAsync((ExistingCache?)null);
        repository.Setup(x => x.ReserveAsync(It.IsAny<Guid>(), It.IsAny<Guid>(), It.IsAny<string>(), It.IsAny<CancellationToken>())).ReturnsAsync((Guid cacheId, Guid _, string _, CancellationToken _) => cacheId);
        repository.Setup(x => x.ReleaseReservationAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
        return repository;
    }

    private static CacheWorkKey CreateSeqKey(
        string kind = "voltage",
        string seq = "pos",
        string unit = "raw",
        IEnumerable<string>? pmus = null)
        => CacheWorkKey.Create(
            "Seq",
            Guid.Parse("6c9f9b41-3dc6-4ef2-8c31-62d03dc6acd0"),
            new DateTime(2026, 8, 31, 12, 0, 0, DateTimeKind.Utc),
            new DateTime(2026, 8, 31, 12, 5, 0, DateTimeKind.Utc),
            ("kind", kind),
            ("seq", seq),
            ("unit", unit),
            ("pmus", CacheWorkKey.NormalizeCollection(pmus ?? new[] { "pmu-a", "pmu-b" })));
}
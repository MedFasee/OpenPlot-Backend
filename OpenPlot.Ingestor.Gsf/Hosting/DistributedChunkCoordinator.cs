using System;
using System.Threading;
using Npgsql;

namespace OpenPlot.Ingestor.Gsf.Hosting;

internal interface IChunkExecutionCoordinator
{
    IDisposable Acquire(CancellationToken cancellationToken);
}

internal sealed class PostgresAdvisoryLockChunkExecutionCoordinator : IChunkExecutionCoordinator
{
    private const int LockFamily = 24813;
    private static readonly TimeSpan RetryDelay = TimeSpan.FromMilliseconds(200);
    private int _nextStartSlot = 1;

    private readonly IngestorRuntimeContext _runtimeContext;

    public PostgresAdvisoryLockChunkExecutionCoordinator(IngestorRuntimeContext runtimeContext)
    {
        _runtimeContext = runtimeContext;
    }

    public IDisposable Acquire(CancellationToken cancellationToken)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            using var connection = new NpgsqlConnection(_runtimeContext.Options.PgConnString);
            connection.Open();

            var totalSlots = Math.Max(1, _runtimeContext.Options.GlobalMaxParallelChunks);
            var startSlot = GetNextStartSlot(totalSlots);

            for (var offset = 0; offset < totalSlots; offset++)
            {
                var slot = ((startSlot - 1 + offset) % totalSlots) + 1;
                if (TryAcquireSlot(connection, slot, out var lease))
                    return lease;
            }

            cancellationToken.WaitHandle.WaitOne(RetryDelay);
        }
    }

    private int GetNextStartSlot(int totalSlots)
    {
        var next = Interlocked.Increment(ref _nextStartSlot);
        return ((next - 1) % totalSlots) + 1;
    }

    private static bool TryAcquireSlot(NpgsqlConnection connection, int slot, out IDisposable lease)
    {
        using var command = new NpgsqlCommand("SELECT pg_try_advisory_lock(@family, @slot);", connection);
        command.Parameters.AddWithValue("family", LockFamily);
        command.Parameters.AddWithValue("slot", slot);

        var acquired = command.ExecuteScalar() is bool value && value;
        if (acquired)
        {
            lease = new AdvisoryLockLease(connection, slot);
            return true;
        }

        lease = null!;
        return false;
    }

    private sealed class AdvisoryLockLease : IDisposable
    {
        private readonly NpgsqlConnection _connection;
        private readonly int _slot;
        private bool _disposed;

        public AdvisoryLockLease(NpgsqlConnection connection, int slot)
        {
            _connection = connection;
            _slot = slot;
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            try
            {
                if (_connection.State == System.Data.ConnectionState.Open)
                {
                    using var command = new NpgsqlCommand("SELECT pg_advisory_unlock(@family, @slot);", _connection);
                    command.Parameters.AddWithValue("family", LockFamily);
                    command.Parameters.AddWithValue("slot", _slot);
                    command.ExecuteNonQuery();
                }
            }
            finally
            {
                _disposed = true;
                _connection.Dispose();
            }
        }
    }
}

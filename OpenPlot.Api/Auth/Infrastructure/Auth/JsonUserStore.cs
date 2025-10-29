using System.Text.Json;
using OpenPlot.Auth.Infrastructure.Auth.Models;

namespace OpenPlot.Auth.Infrastructure.Auth;

using System.Text.Json;
using Microsoft.Extensions.Options;

public sealed class JsonUserStore : IUserStore, IDisposable
{
    private readonly string _fullPath;
    private readonly object _lock = new();
    private volatile Dictionary<string, UserRecord> _byUser = new(StringComparer.OrdinalIgnoreCase);
    private volatile Dictionary<string, UserRecord> _bySub = new();
    private FileSystemWatcher? _fsw;

    public JsonUserStore(
        IOptions<UserStoreOptions> opt,
        IWebHostEnvironment env)
    {
        var path = opt.Value.UsersPath;
        _fullPath = Path.IsPathRooted(path) ? path : Path.Combine(env.ContentRootPath, path);
        Load();
        TryWatch();
    }

    public Task<UserRecord?> FindByUsernameAsync(string username, CancellationToken ct = default) =>
        Task.FromResult(_byUser.TryGetValue(username, out var u) ? u : null);

    public Task<UserRecord?> FindBySubAsync(string sub, CancellationToken ct = default) =>
        Task.FromResult(_bySub.TryGetValue(sub, out var u) ? u : null);

    private void Load()
    {
        lock (_lock)
        {
            if (!File.Exists(_fullPath))
                throw new FileNotFoundException("users.json não encontrado", _fullPath);

            var json = File.ReadAllText(_fullPath);
            var list = JsonSerializer.Deserialize<List<UserRecord>>(json, new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            }) ?? new List<UserRecord>();

            _byUser = list.GroupBy(u => u.username, StringComparer.OrdinalIgnoreCase)
                          .ToDictionary(g => g.Key, g => g.First(), StringComparer.OrdinalIgnoreCase);

            _bySub = list.GroupBy(u => u.sub)
                          .ToDictionary(g => g.Key, g => g.First());
        }
    }

    private void TryWatch()
    {
        var dir = Path.GetDirectoryName(_fullPath)!;
        var file = Path.GetFileName(_fullPath);
        if (!Directory.Exists(dir)) return;

        _fsw = new FileSystemWatcher(dir, file)
        {
            NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.Size | NotifyFilters.FileName
        };
        _fsw.Changed += (_, __) => SafeReload();
        _fsw.Created += (_, __) => SafeReload();
        _fsw.Renamed += (_, __) => SafeReload();
        _fsw.EnableRaisingEvents = true;
    }

    private void SafeReload()
    {
        try { Load(); } catch { /* log se quiser */ }
    }

    public void Dispose() => _fsw?.Dispose();
}


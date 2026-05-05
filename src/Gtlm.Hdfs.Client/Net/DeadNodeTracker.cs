namespace Gtlm.Hdfs.Client.Net;

using System.Collections.Concurrent;
using Gtlm.Hdfs.Client.Models;

/// <summary>
/// Tracks DataNodes that recently failed. Deprioritizes them when choosing replicas.
/// Dead nodes expire after a configurable duration.
/// </summary>
public sealed class DeadNodeTracker
{
    private readonly ConcurrentDictionary<string, long> _deadNodes = new();
    private readonly TimeSpan _expiry;

    public DeadNodeTracker(TimeSpan expiry)
    {
        _expiry = expiry;
    }

    public void MarkDead(DatanodeInfo dataNode)
    {
        _deadNodes[dataNode.DatanodeUuid] = Environment.TickCount64;
    }

    public bool IsDead(DatanodeInfo dataNode)
    {
        if (!_deadNodes.TryGetValue(dataNode.DatanodeUuid, out var deadAt))
            return false;

        if (Environment.TickCount64 - deadAt > (long)_expiry.TotalMilliseconds)
        {
            _deadNodes.TryRemove(dataNode.DatanodeUuid, out _);
            return false;
        }

        return true;
    }

    /// <summary>
    /// Sort locations with dead nodes moved to the end, preserving
    /// relative order of live nodes and dead nodes.
    /// </summary>
    public IReadOnlyList<DatanodeInfo> PrioritizeLocations(IReadOnlyList<DatanodeInfo> locations)
    {
        if (_deadNodes.IsEmpty || locations.Count <= 1)
            return locations;

        var live = new List<DatanodeInfo>();
        var dead = new List<DatanodeInfo>();

        foreach (var dn in locations)
        {
            if (IsDead(dn))
                dead.Add(dn);
            else
                live.Add(dn);
        }

        if (dead.Count == 0)
            return locations;

        live.AddRange(dead);
        return live;
    }

    public int DeadCount => _deadNodes.Count;

    public void Clear() => _deadNodes.Clear();
}

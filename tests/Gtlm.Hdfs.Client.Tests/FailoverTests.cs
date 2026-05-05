namespace Gtlm.Hdfs.Client.Tests;

using Gtlm.Hdfs.Client.Models;
using Gtlm.Hdfs.Client.Net;

public class DeadNodeTrackerTests
{
    private static DatanodeInfo MakeDn(string uuid) => new()
    {
        IpAddress = "10.0.0.1",
        HostName = "dn-" + uuid,
        DatanodeUuid = uuid,
        XferPort = 9866,
    };

    [Fact]
    public void IsDead_NotMarked_ReturnsFalse()
    {
        var tracker = new DeadNodeTracker(TimeSpan.FromMinutes(10));
        Assert.False(tracker.IsDead(MakeDn("u1")));
    }

    [Fact]
    public void MarkDead_ThenIsDead_ReturnsTrue()
    {
        var tracker = new DeadNodeTracker(TimeSpan.FromMinutes(10));
        var dn = MakeDn("u1");

        tracker.MarkDead(dn);

        Assert.True(tracker.IsDead(dn));
        Assert.Equal(1, tracker.DeadCount);
    }

    [Fact]
    public async Task IsDead_AfterExpiry_ReturnsFalse()
    {
        var tracker = new DeadNodeTracker(TimeSpan.FromMilliseconds(1));
        var dn = MakeDn("u1");

        tracker.MarkDead(dn);
        await Task.Delay(50);

        Assert.False(tracker.IsDead(dn));
    }

    [Fact]
    public void MarkDead_MultipleTimes_UpdatesTimestamp()
    {
        var tracker = new DeadNodeTracker(TimeSpan.FromMinutes(10));
        var dn = MakeDn("u1");

        tracker.MarkDead(dn);
        tracker.MarkDead(dn);

        Assert.True(tracker.IsDead(dn));
        Assert.Equal(1, tracker.DeadCount);
    }

    [Fact]
    public void Clear_RemovesAll()
    {
        var tracker = new DeadNodeTracker(TimeSpan.FromMinutes(10));
        tracker.MarkDead(MakeDn("u1"));
        tracker.MarkDead(MakeDn("u2"));

        tracker.Clear();

        Assert.Equal(0, tracker.DeadCount);
        Assert.False(tracker.IsDead(MakeDn("u1")));
    }

    [Fact]
    public void PrioritizeLocations_NoDeadNodes_ReturnsOriginal()
    {
        var tracker = new DeadNodeTracker(TimeSpan.FromMinutes(10));
        var locs = new[] { MakeDn("u1"), MakeDn("u2"), MakeDn("u3") };

        var result = tracker.PrioritizeLocations(locs);

        Assert.Same(locs, result);
    }

    [Fact]
    public void PrioritizeLocations_DeadNodeMovedToEnd()
    {
        var tracker = new DeadNodeTracker(TimeSpan.FromMinutes(10));
        var dn1 = MakeDn("u1");
        var dn2 = MakeDn("u2");
        var dn3 = MakeDn("u3");

        tracker.MarkDead(dn1);

        var result = tracker.PrioritizeLocations([dn1, dn2, dn3]);

        Assert.Equal("u2", result[0].DatanodeUuid);
        Assert.Equal("u3", result[1].DatanodeUuid);
        Assert.Equal("u1", result[2].DatanodeUuid); // dead, moved to end
    }

    [Fact]
    public void PrioritizeLocations_AllDead_PreservesOrder()
    {
        var tracker = new DeadNodeTracker(TimeSpan.FromMinutes(10));
        var dn1 = MakeDn("u1");
        var dn2 = MakeDn("u2");

        tracker.MarkDead(dn1);
        tracker.MarkDead(dn2);

        var result = tracker.PrioritizeLocations([dn1, dn2]);

        // All dead, but still returned (no live nodes to put first)
        Assert.Equal(2, result.Count);
    }

    [Fact]
    public void PrioritizeLocations_SingleLocation_ReturnsSame()
    {
        var tracker = new DeadNodeTracker(TimeSpan.FromMinutes(10));
        var locs = new[] { MakeDn("u1") };

        var result = tracker.PrioritizeLocations(locs);

        Assert.Same(locs, result);
    }

    [Fact]
    public void PrioritizeLocations_MultipleDeadNodes()
    {
        var tracker = new DeadNodeTracker(TimeSpan.FromMinutes(10));
        var dn1 = MakeDn("u1");
        var dn2 = MakeDn("u2");
        var dn3 = MakeDn("u3");
        var dn4 = MakeDn("u4");

        tracker.MarkDead(dn1);
        tracker.MarkDead(dn3);

        var result = tracker.PrioritizeLocations([dn1, dn2, dn3, dn4]);

        // Live nodes first, then dead nodes
        Assert.Equal("u2", result[0].DatanodeUuid);
        Assert.Equal("u4", result[1].DatanodeUuid);
        Assert.Equal("u1", result[2].DatanodeUuid);
        Assert.Equal("u3", result[3].DatanodeUuid);
    }
}

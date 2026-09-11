using Xunit;
using static Ra.SedaBus.EnvelopeHelpers;

namespace Ra.SedaBus.Tests;

public class EnvelopeTests
{
    [Fact]
    public void MakeEnvelopeGetsAUniqueIdAndACurrentRouteOfTo()
    {
        var e = MakeEnvelope("work", 42);
        Assert.False(string.IsNullOrEmpty(e.Id));
        Assert.Equal("work", TargetService(e));
        Assert.Equal(42, (int?)EnvelopePayload(e));

        var e2 = MakeEnvelope("work", 1);
        Assert.NotEqual(e.Id, e2.Id);
    }

    [Fact]
    public void PayloadAccessorsRoundTripArbitraryJson()
    {
        var e = MakeEnvelope("ingest", "hello");
        Assert.Equal("hello", (string?)EnvelopePayload(e));
        SetPayload(e, 7);
        Assert.Equal(7, (int?)EnvelopePayload(e));
    }

    [Fact]
    public void SenderAndHeadersAreSetOnTheEnvelope()
    {
        var e = MakeEnvelope("ingest", sender: "producer-1", headers: new Dictionary<string, string> { ["k"] = "v" });
        Assert.Equal("producer-1", e.Client);
        Assert.Equal("v", (string?)e.Header("k"));
    }

    [Fact]
    public void SlipIsVisitedToThenEachHopInOrderViaTargetServiceAndRatchet()
    {
        var e = MakeEnvelope("one", slip: new[] { "two", "three" });

        Assert.Equal("one", TargetService(e));

        Assert.NotNull(e.DynamicRoutingSlip.PeekAtNextRoute());
        e.Ratchet();
        Assert.Equal("two", TargetService(e));

        Assert.NotNull(e.DynamicRoutingSlip.PeekAtNextRoute());
        e.Ratchet();
        Assert.Equal("three", TargetService(e));

        Assert.Null(e.DynamicRoutingSlip.PeekAtNextRoute());
    }
}

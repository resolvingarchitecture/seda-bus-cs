using System.Collections.Concurrent;
using System.Threading;
using Xunit;
using static Ra.SedaBus.EnvelopeHelpers;

namespace Ra.SedaBus.Tests;

public class BusTests
{
    private static ChannelConfig Cfg() => new();

    [Fact]
    public void PointToPointRoundRobinsAcrossConsumers()
    {
        var bus = new Bus(4);
        var a = 0;
        var b = 0;
        bus.Channel("work", Cfg().WithCapacity(100));
        bus.Subscribe("work", _ => { Interlocked.Increment(ref a); return true; });
        bus.Subscribe("work", _ => { Interlocked.Increment(ref b); return true; });

        for (var i = 0; i < 20; i++)
        {
            Assert.True(bus.Publish(MakeEnvelope("work", i), TimeSpan.FromSeconds(1)));
        }
        Assert.True(bus.Shutdown(TimeSpan.FromSeconds(5)));
        Assert.Equal(10, a);
        Assert.Equal(10, b);
    }

    [Fact]
    public void PubSubFansOutToEveryConsumer()
    {
        var bus = new Bus(4);
        var results = new BlockingCollection<(string Tag, int Value)>();
        bus.Channel("events", Cfg().WithCapacity(100).WithDelivery(Delivery.PubSub));
        foreach (var tag in new[] { "a", "b", "c" })
        {
            bus.Subscribe("events", e =>
            {
                results.Add((tag, EnvelopePayload(e)!.GetValue<int>()));
                return true;
            });
        }

        for (var i = 0; i < 4; i++)
        {
            bus.Publish(MakeEnvelope("events", i), TimeSpan.FromSeconds(1));
        }

        var got = new List<(string Tag, int Value)>();
        for (var i = 0; i < 12; i++)
        {
            Assert.True(results.TryTake(out var item, TimeSpan.FromSeconds(5)), "expected a fan-out message");
            got.Add(item);
        }
        bus.Shutdown(TimeSpan.FromSeconds(5));

        Assert.Equal(12, got.Count);
        foreach (var tag in new[] { "a", "b", "c" })
        {
            Assert.Equal(4, got.Count(p => p.Tag == tag));
        }
    }

    [Fact]
    public void RoutingSlipVisitsEveryStageInOrder()
    {
        var bus = new Bus(4);
        var trailLock = new object();
        var trail = new List<string>();
        foreach (var name in new[] { "one", "two", "three" })
        {
            bus.Channel(name, Cfg().WithCapacity(50));
            bus.Subscribe(name, _ =>
            {
                lock (trailLock) trail.Add(name);
                return true;
            });
        }

        var done = new BlockingCollection<bool>();
        bus.PublishWithCallback(
            MakeEnvelope("one", "x", new[] { "two", "three" }),
            TimeSpan.FromSeconds(1),
            _ => done.Add(true));

        Assert.True(done.TryTake(out _, TimeSpan.FromSeconds(5)), "expected completion callback");
        bus.Shutdown(TimeSpan.FromSeconds(5));

        lock (trailLock)
        {
            Assert.Equal(new[] { "one", "two", "three" }, trail);
        }
    }

    [Fact]
    public void BackpressureRejectsWhenTheQueueIsFull()
    {
        var bus = new Bus(4);
        using var gate = new ManualResetEventSlim(false);

        bus.Channel("slow", Cfg().WithCapacity(2).WithConcurrency(1).WithBackpressure(Backpressure.Reject));
        bus.Subscribe("slow", _ =>
        {
            gate.Wait(TimeSpan.FromSeconds(5));
            return true;
        });

        var accepted = 0;
        for (var i = 0; i < 10; i++)
        {
            if (bus.Publish(MakeEnvelope("slow", i), TimeSpan.FromMilliseconds(50))) accepted++;
        }
        gate.Set();

        Assert.True(accepted <= 3, $"accepted {accepted}");
        bus.Shutdown(TimeSpan.FromSeconds(5));
        Assert.True(bus.GetStats()["slow"].Dropped >= 7);
    }

    [Fact]
    public void NackRetriesThenDeadLetters()
    {
        var bus = new Bus(4);
        var attempts = 0;
        bus.Channel("flaky", Cfg().WithCapacity(10).WithMaxAttempts(3));
        bus.Channel("dead", Cfg().WithCapacity(10));
        bus.SetDeadLetterChannel("flaky", "dead");

        var done = new BlockingCollection<bool>();
        bus.Subscribe("dead", _ => { done.Add(true); return true; });
        bus.Subscribe("flaky", _ => { Interlocked.Increment(ref attempts); return false; });

        bus.Publish(MakeEnvelope("flaky", "boom"), TimeSpan.FromSeconds(1));
        Assert.True(done.TryTake(out _, TimeSpan.FromSeconds(5)), "expected dead-lettered message");
        bus.Shutdown(TimeSpan.FromSeconds(5));

        Assert.Equal(3, attempts);
        Assert.Equal(1, bus.GetStats()["flaky"].DeadLettered);
    }

    [Fact]
    public void ShutdownDrainsQueuedWork()
    {
        var bus = new Bus(4);
        var done = 0;
        bus.Channel("drain", Cfg().WithCapacity(500).WithConcurrency(4));
        bus.Subscribe("drain", _ =>
        {
            Thread.Sleep(10);
            Interlocked.Increment(ref done);
            return true;
        });
        for (var i = 0; i < 50; i++)
        {
            bus.Publish(MakeEnvelope("drain", i), TimeSpan.FromSeconds(1));
        }
        Assert.True(bus.Shutdown(TimeSpan.FromSeconds(10)));
        Assert.Equal(50, done);
    }

    [Fact]
    public void PublishAfterPauseIsRejected()
    {
        var bus = new Bus(2);
        bus.Channel("p", Cfg().WithCapacity(10));
        bus.Subscribe("p", _ => true);
        bus.Pause();
        Assert.False(bus.Publish(MakeEnvelope("p", 1), TimeSpan.FromMilliseconds(10)));
        bus.Resume();
        Assert.True(bus.Publish(MakeEnvelope("p", 2), TimeSpan.FromMilliseconds(10)));
        bus.Shutdown(TimeSpan.FromSeconds(2));
    }

    [Fact]
    public void UnknownChannelReturnsFalse()
    {
        var bus = new Bus(2);
        Assert.False(bus.Publish(MakeEnvelope("nope", 1)));
        bus.Shutdown(TimeSpan.FromSeconds(2));
    }

    [Fact]
    public void ConcurrentProducersDeliverExactlyOnce()
    {
        var bus = new Bus(8);
        var seenLock = new object();
        var seen = new List<int>();
        bus.Channel("fan", Cfg().WithCapacity(5000).WithConcurrency(8));
        bus.Subscribe("fan", e =>
        {
            var n = EnvelopePayload(e)!.GetValue<int>();
            lock (seenLock) seen.Add(n);
            return true;
        });

        var threads = new List<Thread>();
        for (var b = 0; b < 6; b++)
        {
            var basis = b;
            var t = new Thread(() =>
            {
                for (var i = 0; i < 500; i++)
                {
                    var n = basis * 1000 + i;
                    while (!bus.Publish(MakeEnvelope("fan", n), TimeSpan.FromSeconds(1)))
                    {
                    }
                }
            });
            threads.Add(t);
            t.Start();
        }
        foreach (var t in threads) t.Join();
        Assert.True(bus.Shutdown(TimeSpan.FromSeconds(15)));

        lock (seenLock)
        {
            Assert.Equal(3000, seen.Count);
            Assert.Equal(3000, seen.Distinct().Count());
        }
    }

    [Fact]
    public void BlockBackpressureNoLostWakeupUnderSaturation()
    {
        // Deliberately tiny capacity + untimed Block: forces every producer
        // to wait on almost every publish, hammering the exact class of
        // race a queue redesign risks reintroducing - a producer
        // registering as a waiter a moment too late to see a slot a
        // concurrent Poll() already freed, with that Poll() having already
        // decided (no waiters registered yet) not to pulse anyone. Bounded
        // by an explicit deadline rather than a bare Join(), so a
        // regression hangs this test loudly instead of the whole suite
        // silently.
        var bus = new Bus(4);
        var seenLock = new object();
        var seen = new List<int>();
        bus.Channel("tight", Cfg().WithCapacity(2).WithConcurrency(2).WithBackpressure(Backpressure.Block));
        bus.Subscribe("tight", e =>
        {
            var n = EnvelopePayload(e)!.GetValue<int>();
            lock (seenLock) seen.Add(n);
            return true;
        });

        var done = 0;
        var threads = new List<Thread>();
        for (var b = 0; b < 6; b++)
        {
            var basis = b;
            var t = new Thread(() =>
            {
                for (var i = 0; i < 300; i++)
                {
                    var n = basis * 1000 + i;
                    // Untimed Block (no timeout) - exactly the path being tested.
                    Assert.True(bus.Publish(MakeEnvelope("tight", n)));
                }
                Interlocked.Increment(ref done);
            });
            threads.Add(t);
            t.Start();
        }

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while (Volatile.Read(ref done) < 6 && DateTime.UtcNow < deadline)
        {
            Thread.Sleep(20);
        }
        Assert.True(Volatile.Read(ref done) == 6,
            "a producer never returned from an untimed Block publish - lost wakeup");
        foreach (var t in threads) t.Join();
        Assert.True(bus.Shutdown(TimeSpan.FromSeconds(15)));

        lock (seenLock)
        {
            Assert.Equal(1800, seen.Count);
            Assert.Equal(1800, seen.Distinct().Count());
        }
    }

    [Fact]
    public void ShutdownReleasesThreadPoolFloorBackDown()
    {
        // Found by an independent production-readiness audit: the
        // constructor used to raise ThreadPool's process-wide min-thread
        // floor and nothing ever lowered it again, even after this same
        // Bus's own Shutdown - a real leak across repeated create/dispose
        // cycles. Proven here by round-tripping a large, unambiguous raise.
        ThreadPool.GetMinThreads(out var before, out _);
        var bus = new Bus(before + 50);
        ThreadPool.GetMinThreads(out var raised, out _);
        Assert.True(raised >= before + 50);

        bus.Shutdown(TimeSpan.FromSeconds(2));

        ThreadPool.GetMinThreads(out var after, out _);
        Assert.True(after <= before,
            $"expected the pool floor to return to at most {before} after Shutdown, was {after}");
    }
}

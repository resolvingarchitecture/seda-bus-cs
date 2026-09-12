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
    public void DropNewestBehavesLikeReject()
    {
        // DropNewest's observable contract from the caller's side is
        // identical to Reject - discard the incoming envelope, don't admit
        // it - matching every other seda-bus port's own choice to treat the
        // two the same. This exists to prove the policy is actually wired
        // to distinguishable, intentional behaviour, not silently ignored.
        var bus = new Bus(4);
        using var gate = new ManualResetEventSlim(false);
        bus.Channel("dn", Cfg().WithCapacity(2).WithConcurrency(1).WithBackpressure(Backpressure.DropNewest));
        bus.Subscribe("dn", _ =>
        {
            gate.Wait(TimeSpan.FromSeconds(5));
            return true;
        });

        var accepted = 0;
        for (var i = 0; i < 10; i++)
        {
            if (bus.Publish(MakeEnvelope("dn", i), TimeSpan.FromMilliseconds(50))) accepted++;
        }
        gate.Set();

        Assert.True(accepted <= 3, $"accepted {accepted}");
        bus.Shutdown(TimeSpan.FromSeconds(5));
        Assert.True(bus.GetStats()["dn"].Dropped >= 7);
    }

    [Fact]
    public void DropOldestEvictsInsteadOfRejecting()
    {
        var bus = new Bus(4);
        using var gate = new ManualResetEventSlim(false);
        bus.Channel("do", Cfg().WithCapacity(2).WithConcurrency(1).WithBackpressure(Backpressure.DropOldest));
        bus.Subscribe("do", _ =>
        {
            gate.Wait(TimeSpan.FromSeconds(5));
            return true;
        });

        var accepted = 0;
        for (var i = 0; i < 10; i++)
        {
            if (bus.Publish(MakeEnvelope("do", i), TimeSpan.FromMilliseconds(50))) accepted++;
            Assert.True(bus.GetStats()["do"].Depth <= 2, "depth must never exceed capacity under DropOldest");
        }
        gate.Set();
        bus.Shutdown(TimeSpan.FromSeconds(5));

        // DropOldest must always admit the newest envelope (unlike Reject,
        // which sheds some) - evicting older *queued* entries to make room
        // instead of ever refusing the incoming one. The exact identity of
        // which envelopes survive depends on scheduling timing (how fast
        // the one in-flight slot is claimed), so this asserts the
        // timing-independent invariants instead: every publish succeeds,
        // and at most 3 (capacity 2 + at most 1 concurrently in-flight)
        // ever avoid eviction.
        Assert.Equal(10, accepted);
        Assert.True(bus.GetStats()["do"].Dropped >= 7, $"dropped={bus.GetStats()["do"].Dropped}");
    }

    [Fact]
    public void InvalidChannelConfigFailsFastAtConstruction()
    {
        // The fluent With* builder clamps to >=1 - exercised implicitly by
        // every other test in this file via Cfg().With*(...). This test
        // targets the bypass: a ChannelConfig built via raw
        // object-initializer syntax skips that clamping entirely, since its
        // properties are public init-only. Channel's own constructor closes
        // it - fail-fast, not silent misbehaviour.
        var bus = new Bus(2);
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            bus.Channel("bad-capacity", new ChannelConfig { Capacity = 0 }));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            bus.Channel("bad-concurrency", new ChannelConfig { Concurrency = 0 }));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            bus.Channel("bad-max-attempts", new ChannelConfig { MaxAttempts = 0 }));
        bus.ShutdownNow();
    }

    [Fact]
    public void ThrowingConsumerNacksInsteadOfCrashingTheBus()
    {
        var bus = new Bus(4);
        var delivered = new ConcurrentBag<int>();
        bus.Channel("flaky3", Cfg().WithCapacity(50).WithMaxAttempts(1));
        bus.Subscribe("flaky3", e =>
        {
            var n = EnvelopePayload(e)!.GetValue<int>();
            if (n % 3 == 0) throw new InvalidOperationException("boom");
            delivered.Add(n);
            return true;
        });

        for (var i = 0; i < 30; i++)
        {
            Assert.True(bus.Publish(MakeEnvelope("flaky3", i), TimeSpan.FromSeconds(1)));
        }
        Assert.True(bus.Shutdown(TimeSpan.FromSeconds(5)));

        // Every non-multiple-of-3 envelope, both before and after a throw
        // elsewhere in the batch, still got delivered - the bus and its
        // worker survive a throwing consumer instead of losing subsequent
        // work.
        var expected = Enumerable.Range(0, 30).Where(n => n % 3 != 0).ToList();
        Assert.Equal(expected.Count, delivered.Count);
        foreach (var n in expected) Assert.Contains(n, delivered);
        Assert.Equal(10, bus.GetStats()["flaky3"].DeadLettered); // maxAttempts=1: a nack dead-letters immediately
    }

    [Fact]
    public void ShutdownAccountingHoldsUnderConcurrentDelayedProcessing()
    {
        // "drained: true" must mean every published envelope was actually
        // Delivered or DeadLettered - not merely that a queue looked empty
        // while something was still popped-but-not-yet-acked on a worker.
        // Bus tracks this via _inFlight, incremented before a Drain task is
        // queued and decremented only after its whole batch (every Process
        // call in it) finishes - not via queue depth alone.
        for (var trial = 0; trial < 2; trial++)
        {
            var bus = new Bus(4);
            const int total = 40;
            bus.Channel("acct", Cfg().WithCapacity(100).WithConcurrency(4));
            bus.Subscribe("acct", _ =>
            {
                Thread.Sleep(15);
                return true;
            });

            for (var i = 0; i < total; i++)
            {
                Assert.True(bus.Publish(MakeEnvelope("acct", i), TimeSpan.FromSeconds(1)));
            }

            // trial 0: timeout comfortably longer than the work -> drains
            // fully. trial 1: timeout deliberately far too short -> times
            // out mid-flight; accounting must still never over-count.
            var timeout = trial == 0 ? TimeSpan.FromSeconds(5) : TimeSpan.FromMilliseconds(50);
            var drained = bus.Shutdown(timeout);
            var stats = bus.GetStats()["acct"];

            if (drained)
            {
                Assert.Equal(total, stats.Delivered + stats.DeadLettered);
                Assert.Equal(0, stats.Depth);
            }
            else
            {
                Assert.True(stats.Delivered + stats.DeadLettered <= total);
            }
        }
    }

    [Fact]
    public void RepeatedCreateAndShutdownCyclesDoNotLeakThreads()
    {
        // General guard beyond the one specific ThreadPool-floor leak
        // already fixed and regression-tested
        // (ShutdownReleasesThreadPoolFloorBackDown below): construct-and-
        // fully-shutdown many Bus instances in a loop and confirm the
        // process's live thread count settles back down rather than
        // growing without bound.
        GC.Collect();
        GC.WaitForPendingFinalizers();
        var before = System.Diagnostics.Process.GetCurrentProcess().Threads.Count;

        for (var i = 0; i < 25; i++)
        {
            var bus = new Bus(4);
            bus.Channel("cycle", Cfg().WithCapacity(10));
            bus.Subscribe("cycle", _ => true);
            bus.Publish(MakeEnvelope("cycle", i), TimeSpan.FromSeconds(1));
            Assert.True(bus.Shutdown(TimeSpan.FromSeconds(2)));
        }

        GC.Collect();
        GC.WaitForPendingFinalizers();
        var after = System.Diagnostics.Process.GetCurrentProcess().Threads.Count;

        // Generous tolerance - the shared ThreadPool and test-runner
        // scaffolding both add/remove threads for reasons unrelated to Bus;
        // this guards against unbounded growth, not exact equality.
        Assert.True(after <= before + 20,
            $"thread count grew from {before} to {after} across 25 create/shutdown cycles");
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
    public void NackRetriesThenSucceedsOnFinalAttemptWithNoAttemptLeak()
    {
        var bus = new Bus(4);
        var tries = 0;
        var delivered = new BlockingCollection<bool>();
        bus.Channel("flaky2", Cfg().WithCapacity(10).WithMaxAttempts(3));
        bus.Subscribe("flaky2", _ =>
        {
            var n = Interlocked.Increment(ref tries);
            if (n < 3) return false; // nack twice
            delivered.Add(true);
            return true; // succeed on the 3rd (final allowed) attempt
        });

        bus.Publish(MakeEnvelope("flaky2", "x"), TimeSpan.FromSeconds(1));
        Assert.True(delivered.TryTake(out _, TimeSpan.FromSeconds(5)), "expected eventual delivery");
        bus.Shutdown(TimeSpan.FromSeconds(5));

        Assert.Equal(3, tries);
        Assert.Equal(1, bus.GetStats()["flaky2"].Delivered);
        Assert.Equal(0, bus.GetStats()["flaky2"].DeadLettered);
        Assert.Equal(0, AttemptsMapCount(bus, "flaky2")); // no leak in the per-envelope attempts map
    }

    [Fact]
    public void ChannelWithNoConsumersDeadLettersImmediately()
    {
        var bus = new Bus(4);
        bus.Channel("orphan", Cfg().WithCapacity(10));
        // Deliberately no Subscribe() call.
        Assert.True(bus.Publish(MakeEnvelope("orphan", "x"), TimeSpan.FromSeconds(1)));

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (bus.GetStats()["orphan"].DeadLettered == 0 && DateTime.UtcNow < deadline)
        {
            Thread.Sleep(10);
        }
        bus.Shutdown(TimeSpan.FromSeconds(5));

        Assert.Equal(1, bus.GetStats()["orphan"].DeadLettered);
        Assert.Equal(0, bus.GetStats()["orphan"].Delivered);
    }

    // White-box helper for NackRetriesThenSucceedsOnFinalAttemptWithNoAttemptLeak:
    // Channel._attempts has no public accessor (by design - it's an
    // implementation detail, not part of the port's observable contract),
    // so reflection is the only way to assert "no leak" without widening
    // the public API just for a test. Routed through the non-generic
    // IDictionary/ICollection interfaces (which ConcurrentDictionary<,>
    // implements) rather than naming the internal Channel type directly -
    // that name isn't accessible from this assembly without an
    // InternalsVisibleTo this port doesn't have, and adding one just for a
    // test would widen the port's real API surface for no production reason.
    private static int AttemptsMapCount(Bus bus, string channelName)
    {
        var channelsField = typeof(Bus).GetField("_channels", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        var channels = (System.Collections.IDictionary)channelsField.GetValue(bus)!;
        var channel = channels[channelName]!;
        var attemptsField = channel.GetType().GetField("_attempts", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        var attempts = (System.Collections.ICollection)attemptsField.GetValue(channel)!;
        return attempts.Count;
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

using System.Collections.Concurrent;
using System.Threading;
using Ra.Common;

// The bus: a registry of stages drained by the shared .NET ThreadPool.

namespace Ra.SedaBus;

public enum Delivery
{
    /// <summary>One consumer handles each envelope (round-robin across consumers).</summary>
    PointToPoint,

    /// <summary>Every consumer handles every envelope.</summary>
    PubSub,
}

public enum Backpressure
{
    /// <summary>Producer blocks (up to the publish timeout) until there is room.</summary>
    Block,

    /// <summary><see cref="Bus.Publish"/> returns false immediately when the stage queue is full.</summary>
    Reject,

    /// <summary>Silently discard the envelope being offered.</summary>
    DropNewest,

    /// <summary>Evict the oldest queued envelope to make room.</summary>
    DropOldest,
}

/// <summary>
/// Handles envelopes for a stage. Return true to ack, false to nack (the
/// envelope is retried up to the stage's <see cref="ChannelConfig.MaxAttempts"/>,
/// then dead-lettered). Throwing is treated as a nack.
/// </summary>
public delegate bool Consumer(Envelope envelope);

/// <summary>Invoked once an envelope finishes its itinerary (every routing-slip hop acked).</summary>
public delegate void CompleteCallback(Envelope envelope);

public sealed record ChannelConfig
{
    public int Capacity { get; init; } = 1024;
    public int Concurrency { get; init; } = 1;
    public Delivery Delivery { get; init; } = Delivery.PointToPoint;
    public Backpressure Backpressure { get; init; } = Backpressure.Block;
    public int MaxAttempts { get; init; } = 1;

    public ChannelConfig WithCapacity(int n) => this with { Capacity = Math.Max(1, n) };
    public ChannelConfig WithConcurrency(int n) => this with { Concurrency = Math.Max(1, n) };
    public ChannelConfig WithDelivery(Delivery d) => this with { Delivery = d };
    public ChannelConfig WithBackpressure(Backpressure b) => this with { Backpressure = b };
    public ChannelConfig WithMaxAttempts(int n) => this with { MaxAttempts = Math.Max(1, n) };
}

public sealed record Stats
{
    public int Depth { get; init; }
    public long Enqueued { get; init; }
    public long Delivered { get; init; }
    public long Nacked { get; init; }
    public long Dropped { get; init; }
    public long DeadLettered { get; init; }
}

/// <summary>
/// A staged, broker-less message bus. A plain reference type — unlike the
/// Rust/C++ ports there is no Arc/shared_ptr wrapper to hand around; the CLR
/// GC already keeps a shared <see cref="Bus"/> instance alive for as long as
/// anything references it (including a queued ThreadPool work item's closure).
/// </summary>
public sealed class Bus
{
    // Envelopes a single drain task handles before releasing its permit and
    // rescheduling. Amortises scheduling cost without starving other stages.
    private const int Batch = 16;

    private readonly ConcurrentDictionary<string, Channel> _channels = new();
    private readonly ConcurrentDictionary<string, string> _dlq = new();
    private readonly ConcurrentDictionary<string, CompleteCallback> _callbacks = new();

    // Work items queued on the shared ThreadPool but not yet finished.
    // Needed because, unlike Rust/C++/Java's owned pool, the ThreadPool is
    // process-wide and can't be "joined" — Shutdown must know explicitly
    // when the last in-flight drain has actually completed, not just that
    // every channel's queue looked empty at some polling instant.
    private long _inFlight;

    private volatile bool _running = true;
    private volatile bool _accepting = true;
    private bool _poolFloorReleased;

    // The process-wide ThreadPool floor is a single global setting, not a
    // per-instance one — every live Bus in the process contends for it, and
    // it's a real, found-by-audit resource leak if nothing ever lowers it
    // back down. See RaisePoolFloor's own comment.
    private static readonly object PoolFloorLock = new();
    private static readonly Dictionary<Bus, int> ActivePoolFloorRequests = new();
    private static bool _poolFloorBaselineCaptured;
    private static int _poolFloorBaselineMinWorker;
    private static int _poolFloorBaselineMinIo;

    /// <summary>
    /// Create and start a bus. <paramref name="workers"/> raises the shared
    /// .NET ThreadPool's minimum thread count as a floor (0 defaults to
    /// <see cref="Environment.ProcessorCount"/>), so this bus's workload
    /// isn't stalled behind the pool's default gradual thread-injection rate
    /// under a sudden burst. The pool itself remains shared with the rest of
    /// the process — see the design notes for why that's the idiomatic
    /// choice here rather than a hand-rolled dedicated pool.
    /// </summary>
    public Bus(int workers = 0)
    {
        Workers = workers > 0 ? workers : Environment.ProcessorCount;
        RaisePoolFloor();
    }

    public int Workers { get; }

    // Found by an independent production-readiness audit: the constructor
    // used to call ThreadPool.SetMinThreads directly and nothing ever
    // called it again, so the floor only ever ratcheted up — across
    // repeated create-and-dispose cycles of a single Bus, or multiple
    // concurrent ones, it never came back down. Fixed by tracking each live
    // Bus's own requested floor and recomputing the applied value as the
    // max across every currently-live request (falling back to the
    // pre-any-Bus baseline once none are left) — releasing one bus's
    // request, in Shutdown/ShutdownNow, lowers the floor back toward what
    // the others still need instead of leaking it forever.
    private void RaisePoolFloor()
    {
        lock (PoolFloorLock)
        {
            if (!_poolFloorBaselineCaptured)
            {
                ThreadPool.GetMinThreads(out _poolFloorBaselineMinWorker, out _poolFloorBaselineMinIo);
                _poolFloorBaselineCaptured = true;
            }
            ActivePoolFloorRequests[this] = Workers;
            ApplyPoolFloorLocked();
        }
    }

    private void ReleasePoolFloor()
    {
        lock (PoolFloorLock)
        {
            if (_poolFloorReleased) return;
            _poolFloorReleased = true;
            ActivePoolFloorRequests.Remove(this);
            ApplyPoolFloorLocked();
        }
    }

    // Caller must hold PoolFloorLock.
    private static void ApplyPoolFloorLocked()
    {
        var targetWorker = _poolFloorBaselineMinWorker;
        foreach (var requested in ActivePoolFloorRequests.Values)
        {
            if (requested > targetWorker) targetWorker = requested;
        }
        ThreadPool.SetMinThreads(targetWorker, _poolFloorBaselineMinIo);
    }

    /// <summary>Register a stage. Re-registering a name is a no-op.</summary>
    public Bus Channel(string name, ChannelConfig? config = null)
    {
        _channels.TryAdd(name, new Channel(name, config ?? new ChannelConfig()));
        return this;
    }

    /// <summary>Attach a consumer to a stage. Creates the stage with defaults if needed.</summary>
    public Bus Subscribe(string channelName, Consumer consumer)
    {
        GetOrCreate(channelName, new ChannelConfig()).AddConsumer(consumer);
        return this;
    }

    /// <summary>Route dead letters from <paramref name="source"/> to the channel named <paramref name="dlq"/>.</summary>
    public Bus SetDeadLetterChannel(string source, string dlq)
    {
        GetOrCreate(dlq, new ChannelConfig().WithCapacity(4096).WithBackpressure(Backpressure.DropOldest));
        _dlq[source] = dlq;
        return this;
    }

    private Channel GetOrCreate(string name, ChannelConfig config) =>
        _channels.GetOrAdd(name, n => new Channel(n, config));

    public IReadOnlyDictionary<string, Stats> GetStats() =>
        _channels.ToDictionary(kv => kv.Key, kv => kv.Value.StatsSnapshot());

    // -- publishing -----------------------------------------------------

    /// <summary>
    /// Publish an envelope to the channel named by its current route
    /// (<see cref="EnvelopeHelpers.TargetService"/> — <c>env.GetRoute()?.Service</c>).
    /// </summary>
    public bool Publish(Envelope env, TimeSpan? timeout = null)
    {
        if (!_running || !_accepting) return false;
        var target = EnvelopeHelpers.TargetService(env);
        if (target is null)
        {
            Warn($"envelope has no current route; dropping envelope {env.Id}");
            return false;
        }
        if (!_channels.TryGetValue(target, out var ch))
        {
            Warn($"no channel '{target}'; dropping envelope {env.Id}");
            return false;
        }
        DateTime? deadline = timeout is null ? null : DateTime.UtcNow + timeout.Value;
        if (!ch.Offer(env, deadline)) return false;
        Schedule(ch);
        return true;
    }

    /// <summary>
    /// Publish and invoke <paramref name="onComplete"/> once the envelope
    /// finishes its itinerary (all routing-slip hops acked).
    /// </summary>
    public bool PublishWithCallback(Envelope env, TimeSpan? timeout, CompleteCallback onComplete)
    {
        _callbacks[env.Id] = onComplete;
        var ok = Publish(env, timeout);
        if (!ok) _callbacks.TryRemove(env.Id, out _);
        return ok;
    }

    // -- scheduling / draining ----------------------------------------

    private void Schedule(Channel ch)
    {
        while (ch.Depth() > 0 && ch.TryAcquire())
        {
            Interlocked.Increment(ref _inFlight);
            if (!ThreadPool.QueueUserWorkItem(_ => Drain(ch)))
            {
                Interlocked.Decrement(ref _inFlight);
                ch.Release();
                return;
            }
        }
    }

    private void Drain(Channel ch)
    {
        try
        {
            for (var i = 0; i < Batch; i++)
            {
                if (!_running) break;
                var env = ch.Poll();
                if (env is null) break;
                Process(ch, env);
            }
        }
        finally
        {
            ch.Release();
            Interlocked.Decrement(ref _inFlight);
        }
        if (_running) Schedule(ch);
    }

    private void Process(Channel ch, Envelope env)
    {
        var consumers = ch.SnapshotConsumers();
        if (consumers.Count == 0)
        {
            Warn($"channel '{ch.Name}' has no consumers; dead-lettering {env.Id}");
            DeadLetter(ch, env);
            return;
        }

        var attempt = ch.BumpAttempt(env.Id);
        bool ok;
        if (ch.Config.Delivery == Delivery.PubSub)
        {
            ok = true;
            foreach (var c in consumers) ok = SafeReceive(c, env) && ok;
        }
        else
        {
            var idx = ch.NextRoundRobin(consumers.Count);
            ok = SafeReceive(consumers[idx], env);
        }

        if (ok)
        {
            ch.RecordDelivered();
            ch.ClearAttempt(env.Id);
            CompleteHop(env);
        }
        else if (attempt < ch.Config.MaxAttempts)
        {
            ch.RecordNacked();
            ch.Requeue(env);
        }
        else
        {
            ch.RecordNacked();
            ch.ClearAttempt(env.Id);
            DeadLetter(ch, env);
        }
    }

    private void CompleteHop(Envelope env)
    {
        if (env.DynamicRoutingSlip.PeekAtNextRoute() is not null)
        {
            env.Ratchet();
            Publish(env, TimeSpan.FromSeconds(5));
            return;
        }
        if (_callbacks.TryRemove(env.Id, out var cb))
        {
            try { cb(env); }
            catch (Exception ex) { Warn($"on_complete callback threw for {env.Id}: {ex.Message}"); }
        }
    }

    private void DeadLetter(Channel ch, Envelope env)
    {
        ch.RecordDeadLettered();
        if (_dlq.TryGetValue(ch.Name, out var dlqName) && _channels.TryGetValue(dlqName, out var dlqCh))
        {
            dlqCh.Offer(env, DateTime.UtcNow);
            Schedule(dlqCh);
        }
        _callbacks.TryRemove(env.Id, out _);
    }

    // -- lifecycle ------------------------------------------------------

    /// <summary>Stop accepting Publish() calls; in-flight work finishes either way.</summary>
    public void Pause() => _accepting = false;

    public void Resume()
    {
        if (_running) _accepting = true;
    }

    /// <summary>
    /// Stop accepting, wait until every queue is drained and every in-flight
    /// drain has completed (up to <paramref name="timeout"/>). Returns
    /// whether it drained fully.
    /// </summary>
    public bool Shutdown(TimeSpan timeout)
    {
        _accepting = false;
        var drained = AwaitDrain(timeout);
        _running = false;
        ReleasePoolFloor();
        return drained;
    }

    /// <summary>Stop accepting immediately, without waiting for queues to drain.</summary>
    public void ShutdownNow()
    {
        _accepting = false;
        _running = false;
        ReleasePoolFloor();
    }

    private bool AwaitDrain(TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            if (IsFullyDrained()) return true;
            if (DateTime.UtcNow >= deadline) return IsFullyDrained();
            Thread.Sleep(5);
        }
    }

    private bool IsFullyDrained() =>
        Interlocked.Read(ref _inFlight) == 0 && _channels.Values.All(c => c.Depth() == 0);

    private static bool SafeReceive(Consumer c, Envelope env)
    {
        try
        {
            return c(env);
        }
        catch (Exception ex)
        {
            Warn($"consumer threw handling {env.Id}: {ex.Message}");
            return false;
        }
    }

    private static void Warn(string msg) => Console.Error.WriteLine($"[seda-bus] {msg}");
}

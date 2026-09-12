using System.Collections.Concurrent;
using System.Threading;
using Ra.Common;

namespace Ra.SedaBus;

/// <summary>
/// One stage: a bounded queue plus its consumers.
///
/// The queue is two <see cref="ConcurrentQueue{T}"/> lanes - a lock-free,
/// BCL-provided MPMC structure, not a single <c>lock</c>-guarded
/// <see cref="LinkedList{T}"/> shared by every producer and consumer on the
/// stage (the original design; measured as a real, confirmed lock-contention
/// cliff under <c>par</c> - <c>p50</c> in the tens of microseconds, <c>p99</c>
/// jumping 1,000-3,500x, unstable trial-to-trial - see
/// seda-bus-compare/RESULTS.md). A prior attempt at a hand-rolled two-lock
/// queue made latency worse, not better, and was reverted; an independent
/// production-readiness audit's working theory is that it introduced a wait
/// on a shared .NET ThreadPool thread inside <see cref="Bus"/>'s
/// <c>Drain</c> work items, which is the one thing that pool punishes
/// savagely (thread injection is throttled to roughly one new thread per
/// half-second to a second under sustained demand). This design avoids that
/// trap structurally: <see cref="Offer"/>'s only blocking wait
/// (<see cref="Backpressure.Block"/>) runs on the caller's own thread inside
/// <see cref="Bus.Publish"/>, never inside a <c>Drain</c> ThreadPool work
/// item - mirroring why the original single-lock design was itself
/// "accidentally ThreadPool-safe" per that same audit.
///
/// Retries (<see cref="Requeue"/>) go in a separate lane, always drained
/// ahead of fresh admissions - "put a nacked envelope back at the head for
/// another attempt" without needing a linked list's O(1) front-insert (which
/// <see cref="ConcurrentQueue{T}"/> doesn't support) or a hand-rolled
/// capacity-headroom scheme: both lanes are logically unbounded
/// <see cref="ConcurrentQueue{T}"/>s, and <see cref="Config"/>'s
/// <see cref="ChannelConfig.Capacity"/> is enforced ourselves, against fresh
/// admissions only, exactly as every other port in this comparison does
/// around its own lock-free primitive (Rust's <c>ArrayQueue</c>, C++'s
/// <c>TwoLockQueue</c>).
///
/// Concurrency permits are a <see cref="SemaphoreSlim"/>, not hand-rolled -
/// .NET, like the JVM, ships a real one.
/// </summary>
internal sealed class Channel
{
    public string Name { get; }
    public ChannelConfig Config { get; }

    private readonly ConcurrentQueue<Envelope> _retryQueue = new();
    private readonly ConcurrentQueue<Envelope> _mainQueue = new();

    // Only touched by a blocked Offer (rare) and Poll's notify when someone
    // is actually waiting (also rare: this benchmark's capacity is never
    // exhausted, and neither is most production traffic most of the time).
    // A plain object + Monitor.Wait/Pulse - .NET's condition-variable
    // equivalent - not a SemaphoreSlim: gating the Pulse behind _waiters
    // needs the same lock Offer holds while re-checking capacity, to close
    // the lost-wakeup race described on Offer below (mirrors the identical
    // fix applied to seda-bus-rust's Channel).
    private readonly object _waitLock = new();
    private int _waiters;

    private readonly SemaphoreSlim _permits;

    private readonly ReaderWriterLockSlim _consumersLock = new();
    private readonly List<Consumer> _consumers = [];
    private long _rr;

    // Per-hop delivery attempts, keyed by envelope id (mirrors
    // SEDAMessageChannel.attempts in seda-bus-java / Channel._attempts in
    // seda-bus-python). Lives on the channel, not the envelope, because
    // Ra.Common.Envelope has no attempts field of its own.
    private readonly ConcurrentDictionary<string, int> _attempts = new();

    private long _enqueued;
    private long _delivered;
    private long _nacked;
    private long _dropped;
    private long _deadLettered;

    public Channel(string name, ChannelConfig config)
    {
        // ChannelConfig's fluent With* builder methods clamp every value to
        // >=1, but ChannelConfig is a public record with public init-only
        // properties - `new ChannelConfig { Capacity = 0 }` bypasses that
        // clamping entirely. Found by the correctness suite: a Capacity of
        // 0 makes Offer's `Depth() < Capacity` check always false, which
        // under the default Block policy hangs the very first Publish call
        // forever rather than failing fast. Validating here, independent of
        // how the config was built, is the single choke point every
        // Channel construction path (Bus.Channel, Bus.Subscribe's
        // GetOrCreate, SetDeadLetterChannel) already goes through.
        if (config.Capacity < 1)
            throw new ArgumentOutOfRangeException(nameof(config), config.Capacity, "ChannelConfig.Capacity must be at least 1.");
        if (config.Concurrency < 1)
            throw new ArgumentOutOfRangeException(nameof(config), config.Concurrency, "ChannelConfig.Concurrency must be at least 1.");
        if (config.MaxAttempts < 1)
            throw new ArgumentOutOfRangeException(nameof(config), config.MaxAttempts, "ChannelConfig.MaxAttempts must be at least 1.");
        Name = name;
        Config = config;
        _permits = new SemaphoreSlim(config.Concurrency, config.Concurrency);
    }

    public int Depth() => _mainQueue.Count + _retryQueue.Count;

    /// <summary>
    /// Admit an envelope, honouring the stage's back-pressure policy.
    /// <paramref name="deadline"/> is null for an unbounded wait under Block,
    /// or a point in time after which admission gives up.
    /// </summary>
    public bool Offer(Envelope env, DateTime? deadline)
    {
        while (true)
        {
            if (Depth() < Config.Capacity)
            {
                _mainQueue.Enqueue(env);
                Interlocked.Increment(ref _enqueued);
                return true;
            }

            switch (Config.Backpressure)
            {
                case Backpressure.Reject:
                case Backpressure.DropNewest:
                    Interlocked.Increment(ref _dropped);
                    return false;

                case Backpressure.DropOldest:
                    if (TryEvictOldest()) Interlocked.Increment(ref _dropped);
                    continue; // retry the enqueue

                case Backpressure.Block:
                default:
                    lock (_waitLock)
                    {
                        Interlocked.Increment(ref _waiters);
                        // Close a lost-wakeup window: a slot can free
                        // between the lock-free Depth() check above and
                        // taking _waitLock/registering as a waiter here.
                        // Poll() only pulses when _waiters > 0 at the
                        // moment it dequeues; if that dequeue happened
                        // before we incremented _waiters, no pulse was
                        // sent and none ever will be for this iteration.
                        // Re-checking Depth() now, still holding
                        // _waitLock (the same lock Poll()'s pulse path
                        // takes), catches that case directly - either the
                        // freed slot is visible immediately and we retry
                        // without waiting, or no dequeue has happened yet
                        // and any dequeue from here on sees _waiters > 0
                        // and pulses us, since we hold _waitLock
                        // continuously through to Monitor.Wait below.
                        if (Depth() < Config.Capacity)
                        {
                            Interlocked.Decrement(ref _waiters);
                            continue;
                        }
                        if (deadline is null)
                        {
                            Monitor.Wait(_waitLock);
                        }
                        else
                        {
                            var remaining = deadline.Value - DateTime.UtcNow;
                            if (remaining <= TimeSpan.Zero)
                            {
                                Interlocked.Decrement(ref _waiters);
                                Interlocked.Increment(ref _dropped);
                                return false;
                            }
                            Monitor.Wait(_waitLock, remaining);
                        }
                        Interlocked.Decrement(ref _waiters);
                    }
                    continue; // retry; a spurious/timed-out wake just re-checks Depth()
            }
        }
    }

    public Envelope? Poll()
    {
        if (_retryQueue.TryDequeue(out var env))
        {
            MaybeNotify();
            return env;
        }
        if (_mainQueue.TryDequeue(out env))
        {
            MaybeNotify();
            return env;
        }
        return null;
    }

    /// <summary>Put a nacked envelope back at the head for another attempt.</summary>
    public void Requeue(Envelope env)
    {
        _retryQueue.Enqueue(env);
        MaybeNotify();
    }

    private void MaybeNotify()
    {
        if (Volatile.Read(ref _waiters) > 0)
        {
            lock (_waitLock) Monitor.Pulse(_waitLock);
        }
    }

    /// <summary>
    /// Evicts whatever Poll() would return next (retry lane takes priority,
    /// same as Poll itself) - "oldest" in the queue's own priority order,
    /// not necessarily strict arrival order once a retry has jumped the line.
    /// </summary>
    private bool TryEvictOldest() => _retryQueue.TryDequeue(out _) || _mainQueue.TryDequeue(out _);

    public bool TryAcquire() => _permits.Wait(0);

    public void Release() => _permits.Release();

    public void AddConsumer(Consumer consumer)
    {
        _consumersLock.EnterWriteLock();
        try { _consumers.Add(consumer); }
        finally { _consumersLock.ExitWriteLock(); }
    }

    public IReadOnlyList<Consumer> SnapshotConsumers()
    {
        _consumersLock.EnterReadLock();
        try { return [.. _consumers]; }
        finally { _consumersLock.ExitReadLock(); }
    }

    public int NextRoundRobin(int n) => (int)((Interlocked.Increment(ref _rr) - 1) % n);

    public int BumpAttempt(string id) => _attempts.AddOrUpdate(id, 1, (_, n) => n + 1);
    public void ClearAttempt(string id) => _attempts.TryRemove(id, out _);

    public void RecordDelivered() => Interlocked.Increment(ref _delivered);
    public void RecordNacked() => Interlocked.Increment(ref _nacked);
    public void RecordDeadLettered() => Interlocked.Increment(ref _deadLettered);

    public Stats StatsSnapshot() => new()
    {
        Depth = Depth(),
        Enqueued = Interlocked.Read(ref _enqueued),
        Delivered = Interlocked.Read(ref _delivered),
        Nacked = Interlocked.Read(ref _nacked),
        Dropped = Interlocked.Read(ref _dropped),
        DeadLettered = Interlocked.Read(ref _deadLettered),
    };
}

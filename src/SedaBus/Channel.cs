using System.Collections.Concurrent;
using System.Threading;
using Ra.Common;

namespace Ra.SedaBus;

/// <summary>
/// One stage: a bounded queue plus its consumers. The queue is a
/// <see cref="LinkedList{T}"/> guarded by a monitor lock (<c>lock</c> +
/// <see cref="Monitor.Wait(object, TimeSpan)"/>/<see cref="Monitor.Pulse"/> —
/// .NET's built-in condition-variable equivalent), since it needs both FIFO
/// admission and LIFO requeue-to-front for retries. Concurrency permits are
/// a <see cref="SemaphoreSlim"/>, not hand-rolled — .NET, like the JVM, ships
/// a real one.
/// </summary>
internal sealed class Channel
{
    public string Name { get; }
    public ChannelConfig Config { get; }

    private readonly object _lock = new();
    private readonly LinkedList<Envelope> _queue = new();
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
        Name = name;
        Config = config;
        _permits = new SemaphoreSlim(config.Concurrency, config.Concurrency);
    }

    public int Depth()
    {
        lock (_lock) return _queue.Count;
    }

    /// <summary>
    /// Admit an envelope, honouring the stage's back-pressure policy.
    /// <paramref name="deadline"/> is null for an unbounded wait under Block,
    /// or a point in time after which admission gives up.
    /// </summary>
    public bool Offer(Envelope env, DateTime? deadline)
    {
        lock (_lock)
        {
            while (_queue.Count >= Config.Capacity)
            {
                if (Config.Backpressure is Backpressure.Reject or Backpressure.DropNewest)
                {
                    Interlocked.Increment(ref _dropped);
                    return false;
                }
                if (Config.Backpressure == Backpressure.DropOldest)
                {
                    _queue.RemoveFirst();
                    Interlocked.Increment(ref _dropped);
                    break;
                }
                // Block.
                if (deadline is null)
                {
                    Monitor.Wait(_lock);
                }
                else
                {
                    var remaining = deadline.Value - DateTime.UtcNow;
                    if (remaining <= TimeSpan.Zero)
                    {
                        Interlocked.Increment(ref _dropped);
                        return false;
                    }
                    Monitor.Wait(_lock, remaining);
                }
            }
            _queue.AddLast(env);
            Interlocked.Increment(ref _enqueued);
            return true;
        }
    }

    public Envelope? Poll()
    {
        lock (_lock)
        {
            if (_queue.Count == 0) return null;
            var env = _queue.First!.Value;
            _queue.RemoveFirst();
            Monitor.Pulse(_lock);
            return env;
        }
    }

    /// <summary>Put a nacked envelope back at the head for another attempt.</summary>
    public void Requeue(Envelope env)
    {
        lock (_lock) _queue.AddFirst(env);
    }

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

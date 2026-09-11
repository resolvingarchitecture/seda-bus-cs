# seda-bus-cs — design notes

A C# / .NET 8 port of the `seda-bus` design (see [`seda-bus/DESIGN.md`](../DESIGN.md)
for the shared model), following [`seda-bus-java`](../seda-bus-java/)'s
concurrency model — a real, built-in, shared thread pool and a real semaphore
type, since .NET has both, unlike Rust/C++ — and
[`seda-bus-python`](../seda-bus-python/)/[`seda-bus-ts`](../seda-bus-ts/)/
[`seda-bus-cpp`](../seda-bus-cpp/)'s envelope choice (`ra-common`'s
`Envelope`, not a standalone one). Follows
[`ra-common-cs`](../../common/ra-common-cs/)'s conventions (`Ra.*`
namespaces, PascalCase members, `System.Text.Json.Nodes` for JSON,
`Nullable`/`ImplicitUsings` enabled).

## Decisions

- **Depends on `ra-common-cs`; the envelope is `Ra.Common.Envelope`.** Same
  call as `seda-bus-cpp` (see that port's `DESIGN.md` for the mistake that
  preceded it and the correction): three of the five prior ports
  (Java, Python, TS) carry `ra-common`'s `Envelope`, only Rust doesn't yet.
  `EnvelopeHelpers.cs` is the thin ergonomic layer over the richer type —
  `MakeEnvelope`/`TargetService`/`EnvelopePayload`/`SetPayload`, mirroring
  `seda-bus-python`'s `make_envelope`/`target_service`. `payload` is
  `JsonNode?`, not raw bytes.
  - `Ra.Common.Envelope` is a `sealed` reference type, so it cannot be
    subclassed or aliased via inheritance the way `Ra.Common.Envelope` itself
    can't be wrapped; `seda-bus-cs` therefore doesn't attempt a
    `SedaBus.Envelope` re-export type the way TS's `export { Envelope }`
    does — consumers reference `Ra.Common.Envelope` directly (`using
    Ra.Common;`). C# has no first-class module-level re-export the way
    JS/Python's import systems do, so this is the natural idiom, not a
    workaround.
  - Per-hop `Attempts` moved off the envelope and onto the channel, keyed by
    envelope id (`Channel.BumpAttempt`/`ClearAttempt`, a
    `ConcurrentDictionary<string, int>`) — since `Ra.Common.Envelope` has no
    `Attempts` field. Mirrors `seda-bus-java`'s `SEDAMessageChannel.attempts`.
  - Routing: `MakeEnvelope` pushes the itinerary tail-first then `to` last
    (`Envelope.AddRoute` does `List<Route>.Insert(0, ...)`, a LIFO push), so
    the first `GetRoute()`/`CurrentRoute()` pop yields `to`. `Bus.CompleteHop`
    checks `env.DynamicRoutingSlip.PeekAtNextRoute() is not null` then calls
    `env.Ratchet()` to advance before republishing — same shape as the
    Python/C++ ports' `_complete_hop`/`CompleteHop`.
- **The shared, process-wide .NET `ThreadPool` drains every stage — no
  hand-rolled pool.** Rust and C++ hand-roll a fixed-size pool because
  neither ships one; .NET (like the JVM) does, and `ThreadPool` is already
  real, work-stealing, and elastically sized. `Bus`'s constructor calls
  `ThreadPool.SetMinThreads` to raise the floor to `workers` (default
  `Environment.ProcessorCount`) so a burst of work doesn't stall behind the
  pool's default gradual thread-injection rate — advisory, not an owned,
  fixed-size pool the way Rust/C++/Java's is. This is deliberately the
  bigger structural divergence in this port: the "one shared worker pool"
  invariant from the shared design (§1.2) is only approximately true here —
  it's shared with the *entire process*, not scoped to one `Bus` instance.
  - **Consequence: `Shutdown` can't `Join()` the pool** (you can't wait for
    "everything" on a pool used by unrelated code too). Fixed with an
    explicit `_inFlight` counter (`Interlocked.Increment`/`Decrement` around
    every `Schedule`/`Drain` cycle) — `AwaitDrain` waits for both `_inFlight
    == 0` *and* every channel's `Depth() == 0` before declaring the bus
    drained. Without this, `Shutdown` could return while a `Drain` call was
    still mid-consumer-call (e.g. still inside a slow subscriber), which none
    of the OS-thread-pool ports (Rust/C++/Java, which genuinely `join()`/
    `awaitTermination()` their own pool) have to guard against explicitly.
- **`SemaphoreSlim` for per-channel concurrency permits**, not a hand-rolled
  atomic CAS loop (Rust/C++) or a bare `BoundedSemaphore` substitute — .NET
  ships a real one, same role as Java's `java.util.concurrent.Semaphore`.
  `TryAcquire()` is `_permits.Wait(0)` (non-blocking); `Release()` is
  `_permits.Release()`.
- **`Monitor.Wait`/`Monitor.Pulse` on a plain `lock` object as the
  condition-variable equivalent** for the per-stage queue (a
  `LinkedList<Envelope>`, needed for O(1) `AddFirst` on retry-requeue, which
  a `Queue<T>`/`System.Threading.Channels.Channel<T>` don't support
  directly). `Channel.Offer` under `Backpressure.Block` re-checks the `while`
  loop after each `Monitor.Wait(lock, remaining)` exactly like the
  Rust/C++ ports' `wait_until`/`wait_timeout` + manual re-check.
- **`ConcurrentDictionary` for the channel registry, DLQ map, callback map,
  and per-channel attempts map** — no manual locking needed for any of them
  (unlike C++'s `shared_mutex`-guarded maps), since .NET's BCL concurrent
  collections are lock-striped/lock-free internally. This is a genuine
  simplification over the Rust/C++ ports, not just a style choice — there's
  no idiomatic reason to hand-roll synchronization .NET already gives you.
- **`ReaderWriterLockSlim` for each stage's consumer list** — the one place a
  bare `ConcurrentDictionary`/collection doesn't fit (a plain `List<Consumer>`
  needs external synchronization for add + snapshot-read), and the list is
  read on every single envelope but written only at setup time, so allowing
  concurrent readers is worth the extra type — same reasoning as `seda-bus-cpp`'s
  `std::shared_mutex` there.
- **`ChannelConfig`/`Stats` are C# `record`s with `init`-only properties and
  `With*` fluent methods built on `with` expressions** (`this with { Capacity
  = n }`) — the direct C# idiom for "immutable value, fluent modified copy,"
  matching Rust's consuming builder (`ChannelConfig::default().capacity(..)`)
  and C++'s copy-and-return methods, but using a first-class language feature
  instead of hand-writing the copy.
- **`Consumer`/`CompleteCallback` are delegate types**
  (`delegate bool Consumer(Envelope envelope)`), not an interface — matches
  every other port's "a stage handler is a closure" shape (C++'s
  `std::function<bool(Envelope&)>`, Python's `Callable[[Envelope], bool]`),
  and lets lambdas subscribe directly with no adapter.
- **`Bus` is a plain class, no `Arc`/`shared_ptr` wrapper.** Unlike Rust/C++,
  which need explicit shared ownership so a pool job can keep the bus state
  alive across a drain, the CLR's GC already does this — any object with a
  live reference (including one captured in a `ThreadPool.QueueUserWorkItem`
  closure) stays alive automatically.
- **A thrown exception from a consumer is caught and treated as a nack**
  (`Bus.SafeReceive`), matching every other port's "a misbehaving consumer
  must not take down a worker" rule.
- **`BATCH` is 16** (`Bus.Batch`), matching Rust/Python/C++, not Java's 64 or
  TS's 32.
- **No persistence, no datatype channels, no pull model** — same gaps as
  Rust/Python/TS/C++ (§2.1 of the shared design). Only `seda-bus-java` has
  these.

## What's deliberately not ported

Same as every non-Java sibling: no adaptive controller (shared design §3), no
retry backoff, no priority queues. Unbounded dead-letter channels by default
is mitigated the same way as Rust/C++: `SetDeadLetterChannel` gives the DLQ
`WithCapacity(4096)` + `WithBackpressure(DropOldest)`.

## Testing

`tests/SedaBus.Tests` is an xUnit suite (matching `ra-common-cs`'s test
project conventions) with two files: `EnvelopeTests.cs` (`MakeEnvelope`,
`TargetService`, payload round-trip, sender/headers, and the
`Ratchet`/`PeekAtNextRoute` slip-walk) and `BusTests.cs` — a port of the same
nine integration tests every other port carries (round-robin, pub/sub
fan-out, routing-slip itinerary, back-pressure, retry/dead-letter, drain-on-
shutdown, pause/resume, unknown channel, and a six-thread/3000-envelope
exactly-once stress test). `System.Collections.Concurrent.BlockingCollection<T>`
(BCL, not hand-rolled) stands in for the other ports' hand-rolled test queue
helper (C++'s `TestQueue<T>`, Rust's `mpsc::channel`) — .NET ships one.

All 13 tests pass, stable across five repeated runs. Not verified under a
sanitizer/race detector — .NET doesn't have an equivalent tool in this
ecosystem's toolchain the way Rust/C++ do (TSan), so correctness here rests
on the test suite plus the same manual reasoning applied to the other ports'
concurrency primitives (every shared-state access goes through a lock,
`Interlocked`, `SemaphoreSlim`, or a `ConcurrentDictionary`).

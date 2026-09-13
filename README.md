<div align="center">
  <h1>seda-bus-cs</h1>
  <p><strong>Resolving Architecture &mdash; Clarity in Design</strong></p>
  <p>A small, broker-less, <strong>staged</strong> message bus for C# / .NET.</p>
</div>

Work is decomposed into stages (`Channel`s) connected by bounded queues. The
shared, process-wide .NET `ThreadPool` drains every stage; each stage has its
own concurrency limit (a `SemaphoreSlim`) so none can monopolise it. There is
no broker.

The envelope carried on the bus is `Ra.Common.Envelope` — the same wrapper
`seda-bus-java`/`-python`/`-ts`/`-cpp` carry via their `ra-common` ports.
`seda-bus-cs` depends on its [`ra-common-cs`](../../common/ra-common-cs/)
sibling; beyond that, no dependency outside the BCL.

```csharp
using Ra.SedaBus;
using static Ra.SedaBus.EnvelopeHelpers;

var bus = new Bus(4); // floors the shared ThreadPool at 4 worker threads

bus.Channel("ingest",    new ChannelConfig().WithCapacity(1000));
bus.Channel("transform", new ChannelConfig().WithCapacity(1000).WithConcurrency(4));
bus.Channel("sink",      new ChannelConfig().WithCapacity(1000));

bus.Subscribe("ingest",    _ => true);
bus.Subscribe("transform", e =>
{
    var s = EnvelopePayload(e)!.GetValue<string>().ToUpperInvariant();
    SetPayload(e, s);
    return true;
});
bus.Subscribe("sink", e =>
{
    // ... consume EnvelopePayload(e)
    return true;
});

bus.Publish(
    MakeEnvelope("ingest", "hello", new[] { "transform", "sink" }),
    TimeSpan.FromSeconds(1));

bus.Shutdown(TimeSpan.FromSeconds(5));
```

`MakeEnvelope(to, payload, slip, sender, headers)` / `TargetService(env)` are
thin ergonomic helpers over `Ra.Common.Envelope`'s richer routing API
(`AddRoute` / `GetRoute` / `Ratchet`) — the same shape as `seda-bus-python`'s
`make_envelope`/`target_service`. `payload` is `JsonNode?` (any
JSON-representable value, via `System.Text.Json.Nodes`), read back with
`EnvelopePayload(env)`.

## Features

| | |
|---|---|
| **Bounded stages** | each channel has a capacity — admission control |
| **Back-pressure policy** | `Block` / `Reject` / `DropNewest` / `DropOldest` per stage |
| **Per-stage concurrency** | how many envelopes a stage may process at once |
| **Delivery** | `PointToPoint` (round-robin) or `PubSub` (fan-out) |
| **Routing slips** | an envelope carries an itinerary of stages to visit |
| **Retry + dead-letter** | nacked envelopes retry up to `MaxAttempts`, then route to a DLQ |
| **Metrics** | per-stage enqueued / delivered / nacked / dropped / dead-lettered / depth |
| **Graceful shutdown** | stop accepting, drain in-flight work within a timeout |

`Bus` is a plain reference type — unlike the Rust/C++ ports there is no
`Arc`/`shared_ptr` wrapper to hand around; the CLR keeps it alive for as long
as anything references it, including a queued `ThreadPool` work item's
closure.

Per-hop delivery `Attempts` are tracked on the *channel*, keyed by envelope
id — not on the envelope itself — since `Ra.Common.Envelope` has no such
field (mirrors `seda-bus-java`'s `SEDAMessageChannel.attempts` and
`seda-bus-python`'s `Channel._attempts`).

## Why the shared `ThreadPool`, not a hand-rolled one

Rust and C++ hand-roll a fixed-size thread pool because neither ships one in
its standard library. .NET (like the JVM) does — a real, work-stealing,
elastically-sized pool used by the whole process. Hand-rolling a second one
here would be fighting the platform, not matching it; `seda-bus-java` doesn't
hand-roll one either (`Executors.newFixedThreadPool`). See `DESIGN.md` for
what this trades away (the pool can't be *joined* on shutdown, since it's
shared) and how that's handled (an explicit in-flight counter).

## Building

Assumes the monorepo layout (`ra-common-cs` checked out as a sibling at
`../../common/ra-common-cs`):

```sh
dotnet build SedaBus.sln
dotnet test tests/SedaBus.Tests/SedaBus.Tests.csproj
dotnet run --project examples/Pipeline
```

Targets **.NET 8**.

## Correctness suite coverage

See [`seda-bus-design/CORRECTNESS_SUITE.md`](https://github.com/resolvingarchitecture/seda-bus-design/blob/master/CORRECTNESS_SUITE.md) for what
C1–C7 mean. All in `tests/SedaBus.Tests/BusTests.cs`.

| # | Property | Test(s) |
|---|---|---|
| C1 | Backpressure: Block | `BlockBackpressureNoLostWakeupUnderSaturation` |
| C1 | Backpressure: Reject | `BackpressureRejectsWhenTheQueueIsFull` |
| C1 | Backpressure: DropNewest | `DropNewestBehavesLikeReject` |
| C1 | Backpressure: DropOldest | `DropOldestEvictsInsteadOfRejecting` |
| C2 | Retry → dead-letter | `NackRetriesThenDeadLetters`, `NackRetriesThenSucceedsOnFinalAttemptWithNoAttemptLeak`, `ChannelWithNoConsumersDeadLettersImmediately` |
| C3 | Consumer failure isolation | `ThrowingConsumerNacksInsteadOfCrashingTheBus` |
| C4 | Shutdown accounting | `ShutdownAccountingHoldsUnderConcurrentDelayedProcessing` |
| C5 | Config validation | `InvalidChannelConfigFailsFastAtConstruction` (fails fast — see `Channel`'s constructor) |
| C6 | No resource leak | `ShutdownReleasesThreadPoolFloorBackDown` (the specific ThreadPool-floor leak this session fixed), `RepeatedCreateAndShutdownCyclesDoNotLeakThreads` (general guard) |
| C7 | Concurrency correctness | `ConcurrentProducersDeliverExactlyOnce` |

## What this is not

SEDA's original design also included a **controller** that watched per-stage
latency and queue depth at runtime and re-tuned thread allocation and shed
load automatically. That adaptive controller is not implemented here — every
setting is static configuration. See the shared
[`seda-bus-design`](https://github.com/resolvingarchitecture/seda-bus-design) §3 for what a `2.0` controller would need.

## Companion implementations

- [`seda-bus-java`](../seda-bus-java/) — the original; `ra-common` integration, guaranteed delivery, datatype channels, LIFO routing slip.
- [`seda-bus-rust`](../seda-bus-rust/) — the closest concurrency-model match among the pre-C# ports for a language with no built-in thread pool, but on its own standalone `Envelope`, not yet rewired onto `ra-common-rust`.
- [`seda-bus-python`](../seda-bus-python/) — carries `ra_common.Envelope` as of its `0.2.0` rewire; built to exercise free-threaded (PEP 703) CPython.
- [`seda-bus-ts`](../seda-bus-ts/) — carries `ra-common`'s `Envelope` as of its `0.2.0` rewire; event-loop model with an optional `Worker`-thread transport.
- [`seda-bus-cpp`](../seda-bus-cpp/) — header-only, hand-rolled thread pool (no BCL/std equivalent in C++), also carries `ra::common::Envelope`.

`seda-bus-cs` follows `seda-bus-java`'s concurrency model (a real, built-in,
shared thread pool and a real semaphore, not hand-rolled ones) and
`seda-bus-python`/`-ts`/`-cpp`'s envelope choice (`ra-common`'s `Envelope`).

See [`seda-bus-design`](https://github.com/resolvingarchitecture/seda-bus-design) for the shared design and a full
comparison table across all ports.

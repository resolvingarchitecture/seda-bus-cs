// A three-stage pipeline: ingest -> transform -> sink, via a routing slip.
//
//   dotnet run --project examples/Pipeline

using System.Collections.Concurrent;
using Ra.SedaBus;
using static Ra.SedaBus.EnvelopeHelpers;

var bus = new Bus(4);

bus.Channel("ingest", new ChannelConfig().WithCapacity(100));
bus.Channel("transform", new ChannelConfig().WithCapacity(100).WithConcurrency(2));
bus.Channel("sink", new ChannelConfig().WithCapacity(100));

bus.Subscribe("ingest", e =>
{
    e.SetHeader("seen_by", "ingest");
    return true;
});
bus.Subscribe("transform", e =>
{
    var s = EnvelopePayload(e)!.GetValue<string>().ToUpperInvariant();
    SetPayload(e, s);
    return true;
});

var results = new BlockingCollection<string>();
bus.Subscribe("sink", e =>
{
    results.Add(EnvelopePayload(e)!.GetValue<string>());
    return true;
});

foreach (var word in new[] { "alpha", "bravo", "charlie", "delta", "echo" })
{
    bus.Publish(MakeEnvelope("ingest", word, new[] { "transform", "sink" }), TimeSpan.FromSeconds(1));
}

var outWords = new List<string>();
for (var i = 0; i < 5; i++)
{
    if (results.TryTake(out var w, TimeSpan.FromSeconds(5))) outWords.Add(w);
}
outWords.Sort();

Console.WriteLine("sink saw: " + string.Join(" ", outWords));

bus.Shutdown(TimeSpan.FromSeconds(5));
foreach (var (name, s) in bus.GetStats())
{
    Console.WriteLine(
        $"  {name} depth={s.Depth} enqueued={s.Enqueued} delivered={s.Delivered} nacked={s.Nacked} dropped={s.Dropped} dead_lettered={s.DeadLettered}");
}

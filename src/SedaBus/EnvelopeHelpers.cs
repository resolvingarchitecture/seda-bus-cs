using System.Text.Json.Nodes;
using Ra.Common;

namespace Ra.SedaBus;

/// <summary>
/// The bus carries <see cref="Ra.Common.Envelope"/> — the same wrapper
/// seda-bus-java/-python/-ts carry via their ra-common ports, and
/// seda-bus-cpp carries via ra-common-cpp. Routing is driven by the
/// envelope's <c>DynamicRoutingSlip</c>: each hop targets <c>route.Service</c>;
/// the slip is walked one hop at a time with <see cref="Envelope.Ratchet"/>.
///
/// These helpers keep the ergonomic <c>to</c> / <c>payload</c> / <c>slip</c>
/// shape the other ports use on top of the richer ra-common type.
/// </summary>
public static class EnvelopeHelpers
{
    // seda-bus routes by service, not operation; ra-common still wants a
    // value there.
    private const string Op = "RECEIVE";

    /// <summary>
    /// Build a document envelope addressed to channel <paramref name="to"/>,
    /// then visiting each name in <paramref name="slip"/> in order.
    /// </summary>
    public static Envelope MakeEnvelope(
        string to,
        JsonNode? payload = null,
        IReadOnlyList<string>? slip = null,
        string? sender = null,
        IReadOnlyDictionary<string, string>? headers = null)
    {
        var env = Envelope.Document();
        // ra-common slips are LIFO: push the itinerary tail-first, then `to`
        // last, so Ratchet()/GetRoute() yields `to`, then slip[0], slip[1], ...
        if (slip is { Count: > 0 })
        {
            for (var i = slip.Count - 1; i >= 0; i--) env.AddRoute(slip[i], Op);
        }
        env.AddRoute(to, Op);
        if (payload is not null) env.AddContent(payload);
        if (sender is not null) env.Client = sender;
        if (headers is not null)
        {
            foreach (var (k, v) in headers) env.SetHeader(k, v);
        }
        return env;
    }

    /// <summary>The channel name the envelope is currently headed for.</summary>
    public static string? TargetService(Envelope env) => env.GetRoute()?.Service;

    /// <summary>The document CONTENT value (what <see cref="MakeEnvelope"/> stored).</summary>
    public static JsonNode? EnvelopePayload(Envelope env) => env.Content();

    public static void SetPayload(Envelope env, JsonNode? payload) => env.AddContent(payload);
}

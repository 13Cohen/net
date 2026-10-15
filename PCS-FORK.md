# Experimental proxy reliability branch

Base: golang/net v0.50.0, ebddb99633e0fc35d135f62e9400678492c1d3be.
The upstream module path remains golang.org/x/net. Maintained independently of PCS.

## Opt-in HTTP2 observation

Server.Observe receives bounded, immutable snapshots on the serve goroutine;
the callback MUST NOT block. ObserveEnabled optionally gates snapshots until an
application has authenticated. Nil Observe preserves the unobserved path.
Snapshots include exact DATA connection/stream send windows, oldest eight queued
stream heads, pending bytes/frames, current frame write age and stream, handler
queue depth, active stream/handler counts, control queue, peer max frame size and
initial stream window, frame progress/reset/window-update counters, and PING
queued/written/ACK ages. UTC anchors coexist with monotonic duration calculations.
Request contexts expose ObservationStreamID for connection-local correlation.

No headers, remote/target addresses, payloads or opaque PING data are exported.
Snapshot examples are bounded, not exhaustive. Queue age includes handler channel
and scheduler wait; the maximum is cumulative. Completion means the frame writer
accepted the data (possibly into its buffer), NOT remote delivery. The serve loop
cannot sample itself while it is stalled: consumers must retain snapshot age.

## PING ACK budget

PingTimeoutAfterWrite is false by default. Enabling it requires a nonzero
WriteByteTimeout: health PING explicitly flushes on the write goroutine, and its
ACK deadline starts after that successful flush. A matching ACK arriving before
the write completion notification must not rearm a stale ACK timer. Stuck writes
remain bounded by WriteByteTimeout. An unanswered successfully written PING still
closes the connection after PingTimeout. The legacy option remains a comparison
baseline; tests reproduce its premature close when PING cannot leave the buffer.

## Scheduler experiment

NewProxyWriteScheduler prioritizes up to eight zero-DATA stream heads before a
normal round-robin turn. Control precedence, per-stream order, and DATA windows
are preserved. This is experimental, NOT the default scheduler. Queue tests show
new HEADERS bypass 540672 bytes in a 33-bulk-stream scenario without starving DATA.
Initial end-to-end measurements regressed tails/throughput, so this option must
remain disabled unless repeatable complete-path measurements justify activation.

Validation: full HTTP2/HPACK suite; synthetic-time baseline/fixed PING backpressure,
unanswered PING and writer timeout; exact zero-stream-window observation/privacy;
non-DATA priority and DATA fairness; race checks. Integrating Caddy performs the
logging asynchronously using its existing connection budget and immutable snapshot.

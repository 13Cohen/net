# Bounded HTTP/2 queue evidence

This candidate is paired with the independent ordinary/preferred scheduling cursors.
It adds diagnostics; payload, authentication, flow control and FIFO ordering are unchanged.

The optional observer emits `queue_evidence_version=1` for the built-in round-robin
schedulers. Other/custom schedulers have no detail and version zero (unknown).
The oldest eight pending stream examples and the global control queue inspect at
most 1024 queued requests each. Counts describe that prefix only when `truncated`
is true. Ages/indexes of an absent or uninspected DATA request are -1, not zero.
Logical requests, partially selected DATA, empty DATA and wire frames differ.

`head_age_ms` and `first_data_age_ms` start when the serve loop inserts the request
into its scheduler. `first_data_request_age_ms` additionally includes handler
handoff; `first_data_pre_queue_ms` isolates that prior interval. Partial consumption
retains both timestamps. Legacy `queue_age_ms` / `max_queue_ms` retain their older
request/continuous-nonempty semantics and must not substitute for DATA age.
None of these timestamps measures TLS/TCP delivery.

`selections` counts startFrameWrite attempts: synchronous WINDOW_UPDATE operations
are visible even when periodic snapshots miss them. `frames` includes synthetic
flush, direct GOAWAY/SETTINGS_ACK and failed writes; it is not scheduler Pop count,
wire frame count or successful delivery. Counters are cumulative from connection
creation, not reset upon authentication. `last_selected_age_ms` is unknown before
a stream selection. Use differences on one connection/stream epoch.

Queue introspection is after ObserveEnabled. Caddy enables published snapshots
only after authenticated activity on the connection; this is connection-level,
not a separate per-stream authentication decision. Lightweight timestamps/counters
remain gated by Observe != nil and can include pre-authentication writes. Snapshots
contain fixed types and numeric values, no body, headers, account or target address.
Serve-loop ownership avoids new queue locks; snapshots are immutable after export.

The extra time.Time adds approximately 24 bytes per queued request even with an
observer disabled. The bounded scan and independent cursor still require combined
binary CPU/RSS/latency tests: microbenchmarks alone do not prove production benefit.
The private/public acceptance record identifies source pins and runtime settings.

# Independent HTTP/2 reader observation

Based on `700333ce4c7df194cfdbbbb86d4edf950fc7df37`. The optional
`Server.ObserveReadState` callback receives a bounded concurrent-safe handle
before readFrames starts. Nil disables allocation and tracking. The callback
must not block or log unauthenticated data. Authentication and emission remain
the caller's responsibility; registration does not enable the other observer.

Snapshot does not depend on progress in the serve owner. It reports a sampling
Unix timestamp, phase/monotonic age, cumulative decoded frame count and explicit
best_effort. Phase and its start offset share one atomic value; frame count is
sampled separately. It exports no frames, contents, addresses or stream IDs.
No per-frame logs, callback, syscall or allocation is introduced.

Phases: starting, reading_frame, handoff, await_serve, stopped. reading_frame can
include socket/TLS work or incomplete frame input; handoff/await_serve can include
scheduling. These are observations, not peer fault or root-cause categories.
The existing protocol processing and error paths are unchanged.

Deterministic synctest coverage drives real readFrames with an incomplete frame,
a deliberately absent serve consumer, a deliberately withheld processing gate,
and close. It verifies phase/age progress without a serve callback and nil/off
behavior. Protocol, race and paired workload results accompany the consuming
Caddy candidate; this handle cannot by itself measure end-to-end performance.

2026-10-15 17:32:40–17:35:29 UTC: all six focused net tests passed, including
the new three reader tests and existing PING/flow/privacy cases. Race and full
protocol/performance validation remain release gates at this source commit.

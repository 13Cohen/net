# Independent cursors for bounded proxy scheduler progress

This candidate fixes the starvation reproduced in `fd1fd11e`: nine continuously
refilled stream WINDOW_UPDATE queues caused the old proxy scheduler to skip a
ready DATA queue for all 1000 observed selections. Production still uses the
ordinary round-robin scheduler unless explicitly configured otherwise.

Preferred zero-payload selections now rotate a separate `priorityHead`. Only an
ordinary round-robin selection advances `head`. Both cursors belong to the same
ring and are updated when their queue closes; closing the last stream clears
both, and opening the first stream initializes both. Queue pooling, per-stream
FIFO, flow credit consumption and global control priority are unchanged.

For a fixed ring of N streams without global control backlog, at most eight
preferred selections occur before an ordinary turn. Preferred selections cannot
skip the ordinary cursor. A continuously writable queue therefore receives an
ordinary turn within at most 9*N successful selections. This is a selection
bound, not a wall-time bound: a current socket write can still block, credit can
run out, and global control frames retain their previous strict priority. Empty
DATA also has zero payload and is distinguished from WINDOW_UPDATE in the tests.

Keeping only the ordinary cursor would also prevent the original starvation,
but it would repeatedly start preferred scans at that same position. A separate
preferred cursor distributes those turns across multiple feedback streams while
preserving the ordinary progress bound. The implementation adds one pointer per
scheduler and no per-frame allocation or synchronization.

## Validation scope

The regression now requires the former nine-WINDOW_UPDATE counterexample to
make DATA progress. Additional tests cover different queue counts and shuffled
orders up to 512 streams; multiple DATA and feedback queues; HEADERS and empty
DATA; ordinary and preferred cursor closure; new streams and recycled queues;
zero connection/stream credit and partial DATA; FIFO and final completion; and
the unchanged continuous-global-control starvation boundary.

The test-local 16 KiB variants remain models of a Consume quantum, not a new
production frame-size change. Historical results in
`pcs-scheduler-fifo-results.json` describe the old candidate and remain preserved.

The targeted suite passed in 0.305 seconds. In the nine-feedback-stream
regression, the fixed policy selects DATA 11 times per 1000 Pops instead of zero
(ordinary RR selects it 100 times). The bound is progress, not equal frame share.
For 168 feedback frames before 1552 bytes of DATA, the fixed policy selects the
target on Pop 183, after 917,504 bytes from competing streams; production RR
requires Pop 505 and 22,020,096 competing bytes. These are model results.

The isolated benchmark compares ordinary RR, a test-only copy of the old shared
cursor implementation, and this candidate. It measures selected bytes and
process CPU time around Pop/refill work, with 1/16/128/512 DATA queues and mixed
feedback/DATA queues. It is not socket throughput, delivered bytes, client
latency or full proxy CPU overhead. In particular, a starved benchmark can have
low CPU per selection while delivering no DATA at all.

Three short repetitions report zero allocations per Pop at this resolution.
For 1/16/128/512 DATA queues, median candidate process CPU per Pop was
139/256/735/3215 ns. At 512 queues, ordinary RR was 165 ns and the old preference
3523 ns: the existing O(N) search when no preferred heads exist remains visible.
These short results disclose uncertainty and do not satisfy a full proxy
performance gate. See [`pcs-scheduler-fairness-results.json`](pcs-scheduler-fairness-results.json)
for every sample, test name and resource limit. The 129.440-second validation
window includes compilation; it is not scheduler execution time.

## Reproduce

Use the repository's pinned Go dependencies and Go 1.25.6:

```sh
GOMAXPROCS=1 GOWORK=off go test -p 1 -mod=readonly -count=1 -timeout=60s \
  -run 'Test(SchedulerFIFODiagnostic|RoundRobinScheduler|ProxyScheduler)' ./http2
GOMAXPROCS=1 GOWORK=off go test -p 1 -mod=readonly -run '^$' \
  -bench '^BenchmarkProxySchedulerFairness$' -benchtime=20000x -count=3 \
  -benchmem ./http2
```

The CPU-time benchmark is Linux-specific and uses process user+system time;
wall ns/op under a CPU quota is not substituted for CPU time. Full proxy
integration still needs content validation, TLS/authentication, concurrent
bidirectional streams, reset/shutdown, realistic network conditions and a
controlled baseline/candidate performance comparison before enabling the policy.

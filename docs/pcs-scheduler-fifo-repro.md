# Same-stream feedback backlog: isolated scheduler reproduction

This branch adds tests only, based on production net `8f16458b`. It does not
change the production scheduler or authorize enabling the experimental scheduler.

A production incident had a 1552-byte response DATA request pending during a
413.619-second Flush. Timestamp-ordered snapshots show the same stream's logical
queued-frame count **decreasing 169 → 1**, while pending DATA stayed 1552 bytes
and sampled stream/connection credit remained positive. A sorted distinct-value
list `[1, …, 169]` must not be interpreted as the chronological direction.

Those counters indicate zero-payload logical requests draining. They do not
identify their frame types: `DataSize()==0` includes empty DATA, HEADERS and
WINDOW_UPDATE. Stream WINDOW_UPDATE is a concrete source-supported explanation:
it carries a stream pointer, enters that stream's FIFO, and costs one default
round-robin turn just like another stream's 64 KiB DATA. Its small synchronous
write can be invisible to one-second current-writer snapshots. Connection-level
WINDOW_UPDATE has no stream pointer and enters the separate global control queue.

## Fixture and measured results

The fixture queues 168 stream WINDOW_UPDATE requests followed by 1552 bytes of
DATA on A. Streams B and C each maintain one 64 KiB DATA request, refilling only
after its last chunk. Connection and stream credit are ample, and peer max frame
size is 16,777,215 bytes. This tests a mechanism, not the exact incident history.

| Scheduler | Pop selecting A's DATA | Other DATA selected first |
|---|---:|---:|
| Unmodified production round-robin | 505 | 22,020,096 B |
| Existing `NewProxyWriteScheduler` | 191 | 1,441,792 B |
| Round-robin, test-only 16 KiB quantum model | 505 | 5,505,024 B |
| Proxy preference, test-only 16 KiB quantum model | 191 | 360,448 B |

The existing proxy preference lowers competing bytes in this fixture by 93.45%.
This is **not a measured network speed or latency improvement**. The 16 KiB
variants copy the bounded Consume behavior from `9aff5f47` into a test-local
scheduler; they do not validate that candidate's integration or binary.

Seven test groups / twenty subtests passed, including the two existing scheduler
tests. Checks cover FIFO/content, final completion channel, partial consumption,
independent connection/stream credit, zero-credit HEADERS and empty DATA, sustained
stream WINDOW_UPDATE, close during queued/partial work, and recycled queues.
With continuous stream WINDOW_UPDATE, the existing proxy preference allows DATA
after at most eight such selections. **Continuous global control remains an
unbounded priority boundary in every mode**: the test deliberately records 512
control selections without DATA. It does not claim that boundary is fixed.

## Reproduce

With Go 1.25.6 and the repository's pinned dependencies:

```sh
GOMAXPROCS=1 GOWORK=off go test -p 1 -mod=readonly -count=1 -timeout=30s \
  -run 'Test(SchedulerFIFODiagnostic|RoundRobinScheduler|ProxySchedulerHeadersAndFairness)' \
  -v ./http2
```

The recorded run used an isolated container with no network, 0.25 CPU,
384 MiB memory and a 180-second outer cap. Its 133.217-second wall window included
cold dependency compilation; package test runtime was 0.204 seconds. Cgroup CPU
and memory include compilation and are not scheduler overhead measurements.
Raw result fields and all passing test names are in
[`pcs-scheduler-fifo-results.json`](pcs-scheduler-fifo-results.json).
Only a copyright comment was added after the successful run.

## Evidence and release gates still needed

Before attributing the incident to WINDOW_UPDATE, add bounded queue-head kind and
head age, queued counts by frame kind (distinguishing empty DATA), and cumulative
selection counts by kind. Reuse existing periodic bounded samples; avoid per-frame
logs and full-queue scans. Keep stream FIFO and global control metrics separate.

Before enabling a scheduler, run a separate interleaved baseline/candidate network
comparison with bidirectional traffic, many concurrent streams, bulk plus small
responses, realistic RTT/bandwidth and loss, global controls, zero-window recovery,
reset/cancel and shutdown. Validate complete payloads and TLS/authentication,
measure small-response tails, bulk goodput and CPU per delivered byte, and agree
thresholds before execution. Preserve the existing 16 KiB network results as a
separate experiment. Do not promote stream WINDOW_UPDATE to global control without
explicit closed-stream lifecycle handling, or assume this fixture solves global
control starvation.

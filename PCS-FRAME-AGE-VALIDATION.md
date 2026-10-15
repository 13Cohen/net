# Last completed frame age after receive evidence

This candidate is based on deployed `8f16458b2f0f7c90a5db45e61e726443eebbbbec`.
It carries the existing `fe41f5ea` observation-only correction forward without
changing the deployed receive observer, retry settings, or PING timeout behavior.
The production release remains unchanged pending a later reviewed release.

`last_frame_read_age_ms` means time since the **serve loop consumed a successfully
decoded frame**. It is `-1` before any completed frame. A complete frame rejected
later by protocol processing still counts as a decoded frame. EOF, truncated
header/payload, and other read failures do not count. This timestamp is not a
kernel packet-arrival timestamp and can lag a completed reader handoff.

Consumer audit: `observationLastRead` is only read by `ServerObservation`.
Caddy stores/logs that metadata and PCS uses it for diagnostic analysis. No
consumer requires an attempt-return timestamp. The actual `lastFrameTime`
variable passed to `handlePingTimer` retains its original update behavior;
this patch cannot extend or shorten the configured health timeout.

## Validation on 2026-10-15

UTC `18:42:40.736388–18:44:53.300796`, one serialized experiment lock,
`GOMAXPROCS=1`, Docker network `none`, 0.4 CPU, 512 MiB memory with no swap,
Go 1.25.6, offline module lookup. The retained Caddy race builder reused its
existing compiler cache; no production proxy or global networking changed.

The deployed source with **only the same added regression tests** fails all
six intended scenarios: EOF after 3 seconds, truncated header, truncated PING
payload, synthetic read error, successfully decoded PING followed by EOF,
and EOF before any completed frame. Each old result incorrectly becomes 0 ms.
The candidate passes all ten observer/reader/PING top-level tests, including
these scenarios, both ordinarily and with `-race`.

Commands, each with `-p 1 -mod=readonly -buildvcs=false -count=1 -timeout 60s`:

```
go test -run 'TestProxyObservation(EOF|FailedFrame|SuccessfulFrame|NoCompleted)' -v ./http2
go test -run 'TestProxy(ReadObservation|Observation|Ping)' -v ./http2
go test -race -run 'TestProxy(ReadObservation|Observation|Ping)' -v ./http2
```

The first command is run against the unmodified deployed implementation with
the new tests and must fail; the following two are run against the candidate.
This validation has no throughput or production stability claim. It does not
repeat the entire HTTP/2 protocol suite or a performance matrix for this
observation-only candidate; those remain release-level decisions. No Caddy
binary or image was rebuilt or deployed for this fix.

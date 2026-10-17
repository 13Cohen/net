# Fixed DATA quantum experiment: H2-DATA-20261017-1115

Status: **HOLD — experimental source, not approved for production and not deployed.** Based on `ca43f763b381b813d5fb2c1344a332a0441f91ab` and the already accepted independent-cursor proxy policy. This revision adds an explicit choice between that same policy's original DATA turn and a 16384-byte DATA turn. It does not revisit WINDOW_UPDATE coalescing, change receive windows or advertised/peer maximum frame size, alter flow control, use adaptive limits, or modify the host network.

## Incident and hypothesis

The driving production evidence is the 2026-10-17 event where a 1099-byte response DATA request waited approximately 5.443 seconds with positive credit, with two 64 KiB DATA selections ahead and seconds-long underlying writes. This is distinct from the earlier WINDOW_UPDATE-backlog hypothesis. A smaller DATA turn may reduce the extra delay imposed by other large streams after the current write returns. It cannot preempt a socket write that is already blocked or fix TCP loss.

`NewProxyWriteSchedulerWithDataQuantum` accepts only `0` and `16384`; other values return an error without creating a scheduler. It first constructs the existing proxy scheduler, preserving its two cursors, at-most-eight preferred turns, per-stream FIFO and strict global-control priority. Only the ordinary queue's `consume` argument changes. The existing `Consume` applies the minimum of quantum, peer maximum frame size, stream credit and connection credit.

The constructors used before this experiment retain zero quantum. Zero preserves behavior, not a claim of zero cost: the ordinary Pop path now has one additional field check. Same-binary 0/16384 comparison isolates the quantum; it does not measure every difference between a new zero-config binary and an older released binary.

## Completion and cancellation invariants

Existing `FrameWriteRequest.Consume` gives intermediate fragments no completion channel and no END_STREAM flag, keeps the original channel/flag on the final fragment, and debits exactly the selected DATA bytes. The remaining request stays at the head of its original stream queue. This revision does not introduce an independent buffer or goroutine.

`serverConn.closeStream` still signals `stream.cw` to release a waiting handler and calls scheduler `CloseStream` to discard queued remainders. A discarded remainder must not report successful completion. Already selected DATA is copied into `Framer.wbuf` on the serve goroutine before asynchronous transport writing, so a canceled handler does not leave that writer referencing a user buffer. These are inherited mechanisms, not a newly stronger cancellation guarantee.

The scheduler tests cover invalid/zero equivalence, two pending bulk turns ahead of the exact 1099-byte target, full payload and final completion, FIFO and empty END_STREAM, independent flow credit, a smaller peer frame maximum, partial-close/recycling, and the nine-feedback-stream fairness regression. The pure selection fixture yields 131072 versus 32768 competing DATA bytes before the target; this is not a measured latency or delivered-byte result.

`TestProxyDataQuantumTLSCancelAndConnectionClose` adds actual verified TLS 1.3 and ALPN h2 with the 16 KiB constructor explicitly enabled and client receive frame maximum 16777215. A single explicit ClientConn prevents silent reconnection. The client reads only a prefix of a body larger than its automatic stream window, verifies the handler is still active, verifies a concurrent exact small response, cancels the bulk, waits for handler exit, and verifies another small response on that same connection. A second incomplete bulk is released by closing the outer connection. This is a functional lifecycle gate, not a speed test. The explicitly enabled test passed both normally (0.443 seconds) and under the race detector (2.695 seconds). Do not infer this result from default-scheduler suite results.

## Validation and release boundaries

The root-coordinated functional checks passed: full HTTP2 (63.364 seconds), HPACK (0.027 seconds), targeted scheduler race (1.902 seconds), Caddy proxy tests (2.715 seconds), and forwardproxy diagnostics off/on (8.687/8.704 seconds). The added, explicitly enabled real-TLS lifecycle test passed normally and under race as described above. Forwardproxy's first isolated ACL failures resulted from missing offline host mappings after container restart; the failed log is preserved separately from the corrected-environment rerun. No production source changed for that fixture correction. These are functional results, not performance acceptance.

Performance acceptance uses actual client receipt of the complete target response, exact payload checks, delivered bulk bytes, per-run CPU and CPU per decimal MB. The experiment's frozen, pre-execution rule fails the CPU gate only when **both** relative CPU cost increases by more than 20% **and** absolute cost increases by more than 0.01 CPU-seconds per client-received decimal MB. All individual values remain visible; a relative increase alone is not relabeled as a decrease. Other gates require the main response P95 to improve by at least 30% and 100 milliseconds, at least three of four paired directions to improve, bulk goodput to retain at least 90%, RSS growth at most 16 MiB, and secondary latency regression at most 20% or 50 milliseconds. This new experiment does not revise the earlier C1 experiment or its separate failed 25% CPU gate.

## Completed v1: 28 runs, HOLD

All six profiles completed: eight main-profile runs (four per quantum) and four runs for each of five controls. Both variants used the same binary and fair proxy policy; only DATA quantum changed. All payload checks passed. The primary metric is nearest-rank P95 within each run, followed by the median of the four run-P95 values per main variant; it is not a pooled 160-request P95. Secondary profiles have two run values per variant. Latency excludes client workload semaphore waiting and starts at the HTTP request write on preconnected, primed tunnels. Upload latency includes the upload. This is an isolated shaped-network experiment, not natural-user or public-Internet throughput.

| Profile | Runs | Small P95: q0 → q16K (seconds) | Bulk goodput: q0 → q16K (decimal MB/s) | Proxy CPU per client-received MB: q0 → q16K (seconds) |
| --- | ---: | ---: | ---: | ---: |
| weak-single-largepeer | 8 | 7.511113 → 5.659346 | 0.034110 → 0.033861 | 0.082256 → 0.089579 |
| weak-loss-single | 4 | 2.369542 → 1.722260 | 0.113372 → 0.110747 | 0.044985 → 0.046712 |
| weak-multi4 | 4 | 6.975418 → 5.236069 | 0.108907 → 0.110488 | 0.069882 → 0.078373 |
| high-multi4 | 4 | 0.788637 → 0.887262 | 11.206450 → 10.894003 | 0.010272 → 0.011486 |
| high-single-bulk | 4 | not measured | 7.265915 → 7.164652 | 0.010411 → 0.012570 |
| compat16k | 4 | 2.423253 → 2.274948 | 0.106094 → 0.105643 | 0.052485 → 0.050368 |

Main P95 fell from 7.511113 to 5.659346 seconds: **24.65%**, or 1.851767 seconds. Three of four paired directions improved, but the required 30% reduction did not pass. The fourth pair direction must remain visible; the per-pair reductions were 9.54%, -6.60%, 35.63%, and 43.76%. Main bulk goodput retained 99.27%; CPU per client-received MB increased by 8.90% (0.007323 CPU-seconds/MB). The other numerical guardrails passed, but **the overall candidate remains HOLD and was not deployed**. A high-bandwidth single-bulk control had a 20.74% relative CPU increase, with only 0.002159 CPU-seconds/MB absolute increase; this passes the predeclared joint CPU rule without hiding the measured increase.

Independent first-wave analysis still found roughly 3.5 seconds (3.228–3.999 seconds across the retained main runs) between all four first-wave small-response Flush calls returning and the earliest client completion. During that interval, fresh H2 snapshots showed no pending request from that small-response set while TCP SendQ/notsent remained nonzero and ACKs advanced. This is a **set-level lower bound**, not an exact per-stream Flush-to-delivery join, and it does not identify physical packet loss as the cause. A smaller scheduler turn cannot eliminate already admitted TCP backlog or interrupt an active TLS write.

The v2 plan is prepared only: quantum 0/16384 crossed with isolated-namespace `tcp_notsent_lowat` 65536/16384, keeping the binary and workload fixed and adding an explicit synthetic lane-to-stream join. It has **not established a benefit** and does not supersede the v1 negative acceptance result. Namespace settings also affect the experiment's client/origin sockets; this confound must remain explicit. There is no production socket-setting or source-pin change in this branch.

## Exact tested-source provenance

The v1 binary SHA256 is `ee3b8c4c8baad0ce3658ca9187f00e0039d810a8be8898f4796f284036555781`; the client fixture binary SHA256 is `2bb68602ddd9c41a2c0c7d8377c97f3788fdc0efccb37ac83fa507a64db7d0de`. The unchanged runtime Go-file hashes below match the source manifest captured for that build. Later documentation and lifecycle-test additions do not change runtime Go code.

| Repository / runtime file | SHA256 |
| --- | --- |
| net: `http2/writesched_roundrobin.go` | `af1a17d431827eb8eedf6f26edc98813a42591e0cd99c7366a99c94422a51f0f` |
| caddy: `modules/caddyhttp/app.go` | `6dddba96067f37ac02491707a2a5cc65130fdff04d6fe80e714c399c8e221ded` |
| caddy: `modules/caddyhttp/proxy_data_quantum.go` | `081962880d24bdff3dd0c13a8286e563cbb3b5eb0adcfa3e38bfefe10d24a617` |

Private root-coordinated evidence is retained under `loop-20261017T1753Z`: `candidate-source-sha256.json`, `functional-validation.json`, `build/quantum-tls-lifecycle{,-race}.log`, and all six `lab/results/data-v1/*/matrix.json` files. The separate incident review retains `mechanism-data-v1/tcp-backlog-first-wave.json`. These artifacts contain the original run-level data; this public document exports aggregates and hashes only. The unchanged v1 plan and inputs are retained separately from `lab-v2`. No workload, compile, or performance rerun was performed to prepare this source-only review commit.

## TLS record boundary: a conditional follow-up, not an established CPU cause

Read-only review used the builder's Go 1.25.6 sources. `crypto/tls/conn.go` selects up to 16384 plaintext bytes per application record once dynamic record sizing has reached that maximum (or is explicitly disabled). The implemented boost threshold is 128 KiB of `bytesSent`; the nearby generic comment discussing 1 MB does not override the code. Earlier records grow arithmetically and may be smaller. TLS 1.3 with a 16-byte AEAD tag adds a five-byte outer header and one encrypted inner content-type byte, so a full record is 16406 bytes.

For a warmed connection with an empty HTTP2 4096-byte buffered writer, an unpadded 16384-byte DATA payload is passed to TLS with its nine-byte H2 header as 16393 bytes, requiring two TLS records. A 65536-byte payload plus nine bytes requires five. Four separately written 16384-byte DATA frames can therefore require eight records to carry the same 65536 payload bytes, compared with five for one large DATA frame. This does not establish the previous experiment's CPU regression cause: extra scheduler turns, fragment objects, asynchronous frame work and encryption/record overhead all remain candidates.

The exact record arithmetic does not apply unchanged to cold dynamic-record growth, pending HTTP2 buffered headers/control frames, padding, partial/error writes or handshake buffering. In particular, a nonempty H2 buffer first flushes a filled 4096-byte buffer, changing TLS call boundaries. Do not infer record counts solely from logical DATA size or one sampled 16406-byte raw write.

If the fixed 16384 experiment fails CPU gates, a separate, predeclared 16375-byte DATA candidate could align an unpadded H2 frame to 16384 plaintext bytes. It is not enabled or accepted by this revision. It can save records only in applicable write-buffer/dynamic-record states and creates five H2 DATA fragments for an exact 65536-byte payload. A falsifiable follow-up should count actual below-TLS record headers/writes without payload logging on verified TLS, compare empty/pending H2 buffers and cold/warm connections, then remeasure exact client delivery and CPU per MB. The current experiment and thresholds must finish unchanged first.

// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package http2

import (
	"fmt"
	"math"
	"syscall"
	"testing"
)

// Test-only reference: the pre-fix proxy Pop from fd1fd11e, to measure the
// implementation change separately from enabling preference over production RR.
type oldSharedCursorScheduler struct{ *roundRobinWriteScheduler }

func (ws *oldSharedCursorScheduler) Pop() (FrameWriteRequest, bool) {
	if !ws.control.empty() {
		return ws.control.shift(), true
	}
	if ws.head == nil {
		return FrameWriteRequest{}, false
	}
	if ws.preferHeaders && ws.headerBurst < 8 {
		q := ws.head
		for {
			if !q.empty() && q.peek().DataSize() == 0 {
				if wr, ok := q.consume(math.MaxInt32); ok {
					ws.head = q.next
					ws.headerBurst++
					return wr, true
				}
			}
			q = q.next
			if q == ws.head {
				break
			}
		}
	}
	ws.headerBurst = 0
	q := ws.head
	for {
		if wr, ok := q.consume(math.MaxInt32); ok {
			ws.head = q.next
			return wr, true
		}
		q = q.next
		if q == ws.head {
			break
		}
	}
	return FrameWriteRequest{}, false
}

func fairnessCPUSeconds() float64 {
	var r syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &r); err != nil {
		panic(err)
	}
	return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
}

// Scheduled bytes are queue-selection work, not bytes written to a socket or
// delivered to a client. CPU is process user+system time, excluding setup, and
// therefore distinct from wall ns/op under the experimental CPU quota.
func BenchmarkProxySchedulerFairness(b *testing.B) {
	for _, pair := range [][2]int{{0, 1}, {0, 16}, {0, 128}, {0, 512}, {9, 1}, {32, 32}} {
		for _, mode := range []string{"rr", "old_proxy", "fair_proxy"} {
			b.Run(fmt.Sprintf("WU%d_DATA%d/%s", pair[0], pair[1], mode), func(b *testing.B) {
				base := newRoundRobinWriteScheduler().(*roundRobinWriteScheduler)
				var ws WriteScheduler = base
				if mode != "rr" {
					base.preferHeaders = true
				}
				if mode == "old_proxy" {
					ws = &oldSharedCursorScheduler{base}
				}
				sc := &serverConn{maxFrameSize: 16777215}
				sc.flow.add(1 << 30)
				requests := make(map[uint32]FrameWriteRequest)
				payload := make([]byte, 65536)
				for i := 0; i < pair[0]+pair[1]; i++ {
					st := &stream{id: uint32(i*2 + 1), sc: sc}
					st.flow.add(1 << 30)
					st.flow.setConnFlow(&sc.flow)
					ws.OpenStream(st.id, OpenStreamOptions{})
					wr := fifoDiagnosticWU(st)
					if i >= pair[0] {
						wr = FrameWriteRequest{stream: st, write: &writeData{streamID: st.id, p: payload}}
					}
					requests[st.id] = wr
					ws.Push(wr)
				}
				var scheduled int64
				b.ReportAllocs()
				b.ResetTimer()
				before := fairnessCPUSeconds()
				for i := 0; i < b.N; i++ {
					wr, ok := ws.Pop()
					if !ok {
						b.Fatal("all-ready queue blocked")
					}
					n := wr.DataSize()
					scheduled += int64(n)
					wr.stream.flow.add(int32(n))
					sc.flow.add(int32(n))
					ws.Push(requests[wr.StreamID()])
				}
				cpu := fairnessCPUSeconds() - before
				b.StopTimer()
				b.ReportMetric(cpu*1e9/float64(b.N), "cpu-ns/op")
				b.ReportMetric(float64(scheduled)/float64(b.N), "scheduled-B/op")
				if cpu > 0 {
					b.ReportMetric(float64(scheduled)/(1<<20)/cpu, "scheduled-MiB/CPU-s")
				}
			})
		}
	}
}

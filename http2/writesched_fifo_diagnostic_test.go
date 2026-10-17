// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package http2

import (
	"bytes"
	"encoding/json"
	"math"
	"testing"
	"time"
)

// Diagnostic only: production Pop is used unmodified for the first two modes.
// The last two modes copy that same Pop algorithm with the existing experimental
// 16 KiB Consume cap from 9aff5f47. No production source is changed by this test.
type fifoDiagnosticQuantum struct{ *roundRobinWriteScheduler }

func (ws *fifoDiagnosticQuantum) Pop() (FrameWriteRequest, bool) {
	if !ws.control.empty() {
		return ws.control.shift(), true
	}
	if ws.head == nil {
		return FrameWriteRequest{}, false
	}
	if ws.preferHeaders && ws.headerBurst < 8 {
		q := ws.priorityHead
		for {
			if !q.empty() && q.peek().DataSize() == 0 {
				if wr, ok := q.consume(math.MaxInt32); ok {
					ws.priorityHead = q.next
					ws.headerBurst++
					return wr, true
				}
			}
			q = q.next
			if q == ws.priorityHead {
				break
			}
		}
	}
	ws.headerBurst = 0
	q := ws.head
	for {
		if wr, ok := q.consume(16 << 10); ok {
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

var fifoDiagnosticModes = []string{"production_rr", "fair_proxy", "rr_quantum16k_model", "proxy_quantum16k_model"}

func fifoDiagnosticScheduler(mode string) (WriteScheduler, *roundRobinWriteScheduler) {
	base := newRoundRobinWriteScheduler().(*roundRobinWriteScheduler)
	if mode == "fair_proxy" || mode == "proxy_quantum16k_model" {
		base = NewProxyWriteScheduler().(*roundRobinWriteScheduler)
	}
	if mode == "rr_quantum16k_model" || mode == "proxy_quantum16k_model" {
		return &fifoDiagnosticQuantum{base}, base
	}
	return base, base
}

func fifoDiagnosticStreams(ws WriteScheduler, credit int32) []*stream {
	sc := &serverConn{maxFrameSize: 16777215}
	sc.flow.add(credit)
	var streams []*stream
	for _, id := range []uint32{1, 3, 5} {
		st := &stream{id: id, sc: sc}
		st.flow.add(credit)
		st.flow.setConnFlow(&sc.flow)
		ws.OpenStream(id, OpenStreamOptions{})
		streams = append(streams, st)
	}
	return streams
}

func fifoDiagnosticData(st *stream, n int) FrameWriteRequest {
	return FrameWriteRequest{stream: st, write: &writeData{streamID: st.id, p: bytes.Repeat([]byte{byte(st.id)}, n)}, observationEnqueued: time.Unix(1, 0)}
}

func fifoDiagnosticWU(st *stream) FrameWriteRequest {
	return FrameWriteRequest{stream: st, write: writeWindowUpdate{streamID: st.id, n: 4096}}
}

func fifoDiagnosticKind(wr FrameWriteRequest) string {
	switch wr.write.(type) {
	case *writeData:
		if wr.DataSize() == 0 {
			return "empty_data"
		}
		return "data"
	case writeWindowUpdate:
		if wr.stream == nil {
			return "connection_window_update"
		}
		return "stream_window_update"
	case *writeResHeaders:
		return "headers"
	default:
		return "other"
	}
}

// Reproduces a mechanism, not the exact unknown production frame types/timing.
func TestSchedulerFIFODiagnosticBacklog(t *testing.T) {
	for _, mode := range fifoDiagnosticModes {
		t.Run(mode, func(t *testing.T) {
			ws, base := fifoDiagnosticScheduler(mode)
			ss := fifoDiagnosticStreams(ws, 1<<30)
			for i := 0; i < 168; i++ {
				ws.Push(fifoDiagnosticWU(ss[0]))
			}
			wanted := fifoDiagnosticData(ss[0], 1552)
			wanted.done = make(chan error, 1)
			ws.Push(wanted)
			for _, st := range ss[1:] {
				ws.Push(fifoDiagnosticData(st, 65536))
			}
			counts := map[string]int{}
			otherBytes := 0
			popIndex := 0
			targetWU := 0
			for popIndex < 2000 {
				wr, ok := ws.Pop()
				if !ok {
					t.Fatal("unexpected blocked queue")
				}
				popIndex++
				counts[fifoDiagnosticKind(wr)]++
				if wr.StreamID() == 1 {
					if wr.DataSize() > 0 {
						if targetWU != 168 || wr.DataSize() != 1552 || wr.done != wanted.done || !bytes.Equal(wr.write.(*writeData).p, wanted.write.(*writeData).p) {
							t.Fatal("FIFO/payload/done mismatch")
						}
						wr.replyToWriter(nil)
						if err := <-wanted.done; err != nil {
							t.Fatal(err)
						}
						break
					}
					targetWU++
					// Same actual writeQueue still holds exactly 1552 DATA bytes.
					q := base.streams[1]
					frames := len(q.currQueue) - q.currPos + len(q.nextQueue)
					if frames != 169-targetWU {
						t.Fatalf("logical queue count %d", frames)
					}
				} else {
					otherBytes += wr.DataSize()
					if !bytes.Equal(wr.write.(*writeData).p, bytes.Repeat([]byte{byte(wr.StreamID())}, wr.DataSize())) {
						t.Fatal("other payload changed")
					}
					// Exactly one logical bulk request per competing stream, like a
					// blocking application Write: refill only after its final chunk.
					if !wr.observationPartial {
						ws.Push(fifoDiagnosticData(wr.stream, 65536))
					}
				}
			}
			wantPops, wantBytes := 505, 22020096
			switch mode {
			case "fair_proxy":
				wantPops, wantBytes = 183, 917504
			case "rr_quantum16k_model":
				wantBytes = 5505024
			case "proxy_quantum16k_model":
				wantPops, wantBytes = 183, 229376
			}
			if popIndex != wantPops || otherBytes != wantBytes {
				t.Fatalf("got pops=%d bytes=%d; want %d %d", popIndex, otherBytes, wantPops, wantBytes)
			}
			result, _ := json.Marshal(map[string]any{"mode": mode, "target_data_pop_1_based": popIndex, "other_data_bytes_before": otherBytes, "popped_frame_kinds": counts, "target_logical_frames_first_last": []int{169, 1}, "target_pending_bytes_until_selected": 1552})
			t.Logf("FIFO_RESULT %s", result)
		})
	}
}

func TestSchedulerFIFODiagnosticZeroCreditAndKinds(t *testing.T) {
	for _, mode := range fifoDiagnosticModes {
		t.Run(mode, func(t *testing.T) {
			ws, _ := fifoDiagnosticScheduler(mode)
			ss := fifoDiagnosticStreams(ws, 0)
			for _, wr := range []FrameWriteRequest{fifoDiagnosticWU(ss[0]), {stream: ss[0], write: &writeResHeaders{streamID: 1}}, fifoDiagnosticData(ss[0], 0), fifoDiagnosticData(ss[0], 17), fifoDiagnosticWU(ss[0])} {
				ws.Push(wr)
			}
			for _, kind := range []string{"stream_window_update", "headers", "empty_data"} {
				wr, ok := ws.Pop()
				if !ok || fifoDiagnosticKind(wr) != kind {
					t.Fatalf("zero-credit FIFO want %s", kind)
				}
				if ss[0].flow.n != 0 || ss[0].sc.flow.n != 0 {
					t.Fatal("non-payload frame consumed credit")
				}
			}
			if _, ok := ws.Pop(); ok {
				t.Fatal("nonempty DATA or following WU bypassed zero credit/FIFO")
			}
			ss[0].flow.add(9)
			ss[0].sc.flow.add(7)
			wr, ok := ws.Pop()
			if !ok || wr.DataSize() != 7 || !wr.observationPartial || wr.done != nil {
				t.Fatal("partial flow consumption")
			}
			if _, ok := ws.Pop(); ok {
				t.Fatal("connection credit bypassed")
			}
			ss[0].sc.flow.add(10)
			wr, ok = ws.Pop()
			if !ok || wr.DataSize() != 2 {
				t.Fatal("stream credit bypassed")
			}
			if _, ok := ws.Pop(); ok {
				t.Fatal("exhausted stream credit bypassed")
			}
			ss[0].flow.add(8)
			wr, ok = ws.Pop()
			if !ok || wr.DataSize() != 8 || wr.observationPartial {
				t.Fatal("final remainder wrong")
			}
			wr, ok = ws.Pop()
			if !ok || fifoDiagnosticKind(wr) != "stream_window_update" {
				t.Fatal("following WU lost")
			}
		})
	}
}

func TestSchedulerFIFODiagnosticSustainedStreamWU(t *testing.T) {
	for _, mode := range fifoDiagnosticModes {
		t.Run(mode, func(t *testing.T) {
			ws, _ := fifoDiagnosticScheduler(mode)
			ss := fifoDiagnosticStreams(ws, 1<<30)
			ws.Push(fifoDiagnosticWU(ss[0]))
			ws.Push(fifoDiagnosticData(ss[1], 65536))
			gap, maxGap, dataPops := 0, 0, 0
			for i := 0; i < 512; i++ {
				wr, ok := ws.Pop()
				if !ok {
					t.Fatal("unexpected empty")
				}
				if wr.StreamID() == 1 {
					gap++
					if gap > maxGap {
						maxGap = gap
					}
					ws.Push(fifoDiagnosticWU(ss[0]))
				} else {
					dataPops++
					gap = 0
					if !wr.observationPartial {
						ws.Push(fifoDiagnosticData(ss[1], 65536))
					}
				}
			}
			if dataPops == 0 || maxGap > 17 {
				t.Fatalf("DATA starvation: data=%d gap=%d", dataPops, maxGap)
			}
			t.Logf("FAIRNESS mode=%s max_stream_WU_before_DATA=%d DATA_pops=%d", mode, maxGap, dataPops)
		})
	}
}

// Regression: the old shared cursor selected no DATA with nine always-ready WU
// queues. Separate preferred and ordinary cursors must keep DATA progressing.
func TestSchedulerFIFODiagnosticManyStreamWUBoundary(t *testing.T) {
	for _, mode := range fifoDiagnosticModes {
		t.Run(mode, func(t *testing.T) {
			ws, _ := fifoDiagnosticScheduler(mode)
			sc := &serverConn{maxFrameSize: 16777215}
			sc.flow.add(1 << 30)
			for i := uint32(0); i < 10; i++ {
				st := &stream{id: i*2 + 1, sc: sc}
				st.flow.add(1 << 30)
				st.flow.setConnFlow(&sc.flow)
				ws.OpenStream(st.id, OpenStreamOptions{})
				if i < 9 {
					ws.Push(fifoDiagnosticWU(st))
				} else {
					ws.Push(fifoDiagnosticData(st, 65536))
				}
			}
			dataPops := 0
			for i := 0; i < 1000; i++ {
				wr, ok := ws.Pop()
				if !ok {
					t.Fatal("unexpected empty queue")
				}
				if wr.DataSize() == 0 {
					ws.Push(fifoDiagnosticWU(wr.stream))
				} else {
					dataPops++
					if !wr.observationPartial {
						ws.Push(fifoDiagnosticData(wr.stream, 65536))
					}
				}
			}
			want := 100
			if mode == "fair_proxy" || mode == "proxy_quantum16k_model" {
				want = 11
			}
			if dataPops != want {
				t.Fatalf("boundary changed: DATA pops=%d want=%d", dataPops, want)
			}
			t.Logf("MANY_STREAM_WU_BOUNDARY mode=%s replenished_WU_streams=9 total_pops=1000 DATA_pops=%d", mode, dataPops)
		})
	}
}

func TestSchedulerFIFODiagnosticGlobalControlBoundary(t *testing.T) {
	for _, mode := range fifoDiagnosticModes {
		t.Run(mode, func(t *testing.T) {
			ws, _ := fifoDiagnosticScheduler(mode)
			ss := fifoDiagnosticStreams(ws, 1<<30)
			ws.Push(fifoDiagnosticData(ss[0], 1552))
			control := FrameWriteRequest{write: writeWindowUpdate{n: 4096}}
			for i := 0; i < 163; i++ {
				ws.Push(control)
			}
			for i := 0; i < 163; i++ {
				wr, ok := ws.Pop()
				if !ok || !wr.isControl() {
					t.Fatal("finite control priority changed")
				}
			}
			wr, ok := ws.Pop()
			if !ok || wr.DataSize() != 1552 {
				t.Fatal("DATA did not resume after finite global controls")
			}
			ws.Push(fifoDiagnosticData(ss[0], 1552))
			// Deliberately documents the unchanged limit: sustained global control
			// refill can starve DATA in every mode. This is not a fairness PASS.
			for i := 0; i < 512; i++ {
				ws.Push(control)
				wr, ok = ws.Pop()
				if !ok || !wr.isControl() {
					t.Fatal("unexpected global-control policy change")
				}
			}
			wr, ok = ws.Pop()
			if !ok || wr.DataSize() != 1552 {
				t.Fatal("DATA lost")
			}
			t.Logf("CONTROL_BOUNDARY mode=%s finite_controls_before_DATA=163 sustained_control_pops_without_DATA=512", mode)
		})
	}
}

func TestSchedulerFIFODiagnosticCloseLifecycle(t *testing.T) {
	for _, mode := range fifoDiagnosticModes {
		t.Run(mode, func(t *testing.T) {
			ws, base := fifoDiagnosticScheduler(mode)
			ss := fifoDiagnosticStreams(ws, 1<<30)
			for i := 0; i < 168; i++ {
				ws.Push(fifoDiagnosticWU(ss[0]))
			}
			ws.Push(fifoDiagnosticData(ss[0], 1552))
			ws.Push(FrameWriteRequest{stream: ss[0], write: &writeResHeaders{streamID: 1}})
			ws.Push(fifoDiagnosticData(ss[0], 0))
			ws.Push(fifoDiagnosticData(ss[1], 65536))
			ws.Push(FrameWriteRequest{write: writeWindowUpdate{n: 4096}})
			ws.CloseStream(1)
			if _, exists := base.streams[1]; exists {
				t.Fatal("closed queue retained")
			}
			ws.OpenStream(7, OpenStreamOptions{}) // exercises recycled queue clearing
			total := 0
			for i := 0; i < 10; i++ {
				wr, ok := ws.Pop()
				if !ok {
					break
				}
				if wr.StreamID() == 1 || wr.StreamID() == 7 {
					t.Fatal("stale closed frames resurrected")
				}
				total += wr.DataSize()
			}
			if total != 65536 {
				t.Fatal("other stream lost")
			}
			ws.Push(fifoDiagnosticData(ss[1], 65536))
			_, _ = ws.Pop()
			ws.CloseStream(3)
			if _, ok := ws.Pop(); ok {
				t.Fatal("closed partial remainder retained")
			}
			ws.CloseStream(5)
			ws.CloseStream(7)
			if base.head != nil {
				t.Fatal("ring not empty")
			}
		})
	}
}

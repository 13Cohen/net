// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package http2

import (
	"fmt"
	"math/rand"
	"testing"
)

func fairnessRequest(st *stream, kind int) FrameWriteRequest {
	switch kind {
	case 0:
		return fifoDiagnosticWU(st)
	case 1:
		return FrameWriteRequest{stream: st, write: &writeData{streamID: st.id, p: []byte{1}}}
	case 2:
		return FrameWriteRequest{stream: st, write: &writeData{streamID: st.id}}
	default:
		return FrameWriteRequest{stream: st, write: &writeResHeaders{streamID: st.id}}
	}
}

func checkFairnessRing(t *testing.T, ws *roundRobinWriteScheduler) {
	t.Helper()
	if len(ws.streams) == 0 {
		if ws.head != nil || ws.priorityHead != nil {
			t.Fatal("cursor retained after last close")
		}
		return
	}
	if ws.head == nil || ws.priorityHead == nil {
		t.Fatal("nil cursor with open streams")
	}
	seen := make(map[*writeQueue]bool)
	q := ws.head
	for i := 0; i < len(ws.streams); i++ {
		if q == nil || q.next == nil || q.prev == nil || q.next.prev != q || q.prev.next != q || seen[q] {
			t.Fatal("invalid ring")
		}
		seen[q] = true
		q = q.next
	}
	if q != ws.head || !seen[ws.priorityHead] {
		t.Fatal("cursor outside ring")
	}
	for _, q := range ws.streams {
		if !seen[q] {
			t.Fatal("map queue outside ring")
		}
	}
}

// A fixed N-stream ring, no global control backlog, and continuously writable
// heads implies one successful ordinary turn per at most nine Pops. Since only
// ordinary turns move head, every such stream must progress within 9*N Pops.
func TestProxySchedulerBoundedProgress(t *testing.T) {
	for _, pair := range [][2]int{{0, 1}, {1, 1}, {8, 1}, {9, 1}, {10, 1}, {17, 1}, {18, 1}, {31, 1}, {64, 1}, {1, 8}, {9, 8}, {18, 4}, {64, 16}, {256, 256}} {
		for seed := int64(0); seed < 4; seed++ {
			t.Run(fmt.Sprintf("zero%d_data%d_order%d", pair[0], pair[1], seed), func(t *testing.T) {
				ws := NewProxyWriteScheduler().(*roundRobinWriteScheduler)
				n := pair[0] + pair[1]
				kinds := make([]int, n)
				for i := pair[0]; i < n; i++ {
					kinds[i] = 1
				}
				if seed > 0 {
					rand.New(rand.NewSource(seed)).Shuffle(n, func(i, j int) { kinds[i], kinds[j] = kinds[j], kinds[i] })
				}
				sc := &serverConn{maxFrameSize: 16777215}
				sc.flow.add(1 << 30)
				requests := make(map[uint32]FrameWriteRequest, n)
				last := make(map[uint32]int, n)
				for i, kind := range kinds {
					st := &stream{id: uint32(2*i + 1), sc: sc}
					st.flow.add(1 << 30)
					st.flow.setConnFlow(&sc.flow)
					ws.OpenStream(st.id, OpenStreamOptions{})
					requests[st.id] = fairnessRequest(st, kind)
					ws.Push(requests[st.id])
				}
				bound := 9 * n
				for step := 1; step <= 3*bound; step++ {
					wr, ok := ws.Pop()
					if !ok {
						t.Fatal("all-ready queue blocked")
					}
					id := wr.StreamID()
					if step-last[id] > bound {
						t.Fatalf("stream%d waited%d Pops, bound%d", id, step-last[id], bound)
					}
					last[id] = step
					ws.Push(requests[id])
				}
				for id := range requests {
					if last[id] == 0 || 3*bound-last[id] >= bound {
						t.Fatalf("stream%d starved at tail", id)
					}
				}
				checkFairnessRing(t, ws)
			})
		}
	}
}

func TestProxySchedulerZeroPayloadKindsProgress(t *testing.T) {
	for _, kind := range []int{0, 2, 3} {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			ws := NewProxyWriteScheduler().(*roundRobinWriteScheduler)
			sc := &serverConn{maxFrameSize: 16777215}
			sc.flow.add(1 << 30)
			requests := make(map[uint32]FrameWriteRequest)
			counts := make(map[uint32]int)
			for i := uint32(0); i < 12; i++ {
				st := &stream{id: i*2 + 1, sc: sc}
				st.flow.add(1 << 30)
				st.flow.setConnFlow(&sc.flow)
				ws.OpenStream(st.id, OpenStreamOptions{})
				k := kind
				if i >= 9 {
					k = 1
				}
				requests[st.id] = fairnessRequest(st, k)
				ws.Push(requests[st.id])
			}
			for i := 0; i < 9*12*3; i++ {
				wr, ok := ws.Pop()
				if !ok {
					t.Fatal("blocked")
				}
				counts[wr.StreamID()]++
				ws.Push(requests[wr.StreamID()])
			}
			for id := range requests {
				if counts[id] == 0 {
					t.Fatalf("stream%d starved", id)
				}
			}
		})
	}
}

func TestProxySchedulerCursorLifecycle(t *testing.T) {
	for _, closeCursor := range []string{"regular", "priority", "other"} {
		t.Run(closeCursor, func(t *testing.T) {
			ws := NewProxyWriteScheduler().(*roundRobinWriteScheduler)
			sc := &serverConn{maxFrameSize: 16777215}
			sc.flow.add(1 << 30)
			requests := make(map[uint32]FrameWriteRequest)
			nextID := uint32(1)
			open := func(kind int) {
				st := &stream{id: nextID, sc: sc}
				nextID += 2
				st.flow.add(1 << 30)
				st.flow.setConnFlow(&sc.flow)
				ws.OpenStream(st.id, OpenStreamOptions{})
				requests[st.id] = fairnessRequest(st, kind)
				ws.Push(requests[st.id])
			}
			for i := 0; i < 12; i++ {
				open(i % 2)
			}
			for step := 0; step < 240; step++ {
				if step%7 == 0 {
					q := ws.head
					if closeCursor == "priority" {
						q = ws.priorityHead
					}
					if closeCursor == "other" {
						q = ws.head.next.next
					}
					for id, candidate := range ws.streams {
						if candidate == q {
							ws.CloseStream(id)
							delete(requests, id)
							break
						}
					}
					open(step % 2) // reuses the just-cleared queue
				}
				checkFairnessRing(t, ws)
				wr, ok := ws.Pop()
				if !ok {
					t.Fatal("blocked")
				}
				request, exists := requests[wr.StreamID()]
				if !exists {
					t.Fatal("closed stream returned")
				}
				ws.Push(request)
			}
			for id := range requests {
				ws.CloseStream(id)
				checkFairnessRing(t, ws)
			}
			open(1)
			checkFairnessRing(t, ws)
			wr, ok := ws.Pop()
			if !ok || wr.DataSize() != 1 {
				t.Fatal("fresh stream after complete close failed")
			}
		})
	}
}

func TestProxySchedulerPreferredTurnKeepsRegularCursor(t *testing.T) {
	ws := NewProxyWriteScheduler().(*roundRobinWriteScheduler)
	ss := fifoDiagnosticStreams(ws, 1<<30)
	ws.Push(fairnessRequest(ss[0], 1))
	ws.Push(fairnessRequest(ss[1], 0))
	ws.Push(fairnessRequest(ss[2], 0))
	ordinary := ws.head
	for i := 0; i < 8; i++ {
		wr, ok := ws.Pop()
		if !ok || wr.DataSize() != 0 {
			t.Fatal("preferred turn missing")
		}
		if ws.head != ordinary {
			t.Fatal("preferred turn advanced ordinary cursor")
		}
		ws.Push(fairnessRequest(wr.stream, 0))
	}
	wr, ok := ws.Pop()
	if !ok || wr.StreamID() != ss[0].id || wr.DataSize() != 1 {
		t.Fatal("ordinary DATA turn skipped")
	}
}

// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package http2

import (
	"bytes"
	"math"
	"reflect"
	"testing"
)

func quantumScheduler(t *testing.T, quantum int32) WriteScheduler {
	t.Helper()
	ws, err := NewProxyWriteSchedulerWithDataQuantum(quantum)
	if err != nil {
		t.Fatal(err)
	}
	return ws
}

func quantumStream(ws WriteScheduler, sc *serverConn, id uint32, credit int32) *stream {
	st := &stream{id: id, sc: sc}
	st.flow.add(credit)
	st.flow.setConnFlow(&sc.flow)
	ws.OpenStream(id, OpenStreamOptions{})
	return st
}

func TestProxyDataQuantumValidationAndZeroEquivalence(t *testing.T) {
	for _, invalid := range []int32{-1, 1, 16383, 16385, 32768, math.MaxInt32} {
		if ws, err := NewProxyWriteSchedulerWithDataQuantum(invalid); err == nil || ws != nil {
			t.Fatalf("invalid quantum %d accepted", invalid)
		}
	}
	type selection struct {
		Stream uint32
		Kind   string
		Data   []byte
		End    bool
	}
	run := func(ws WriteScheduler) []selection {
		sc := &serverConn{maxFrameSize: 16777215}
		sc.flow.add(1 << 24)
		for i := uint32(1); i <= 5; i += 2 {
			st := quantumStream(ws, sc, i, 1<<20)
			ws.Push(FrameWriteRequest{stream: st, write: writeWindowUpdate{streamID: i, n: 4096}})
			ws.Push(FrameWriteRequest{stream: st, write: &writeData{streamID: i, p: bytes.Repeat([]byte{byte(i)}, 65536)}})
			ws.Push(FrameWriteRequest{stream: st, write: &writeData{streamID: i, endStream: true}})
		}
		ws.Push(FrameWriteRequest{write: writeWindowUpdate{n: 4096}})
		var out []selection
		for {
			wr, ok := ws.Pop()
			if !ok {
				break
			}
			x := selection{Stream: wr.StreamID(), Kind: fifoDiagnosticKind(wr)}
			if wd, ok := wr.write.(*writeData); ok {
				x.Data, x.End = append([]byte(nil), wd.p...), wd.endStream
			}
			out = append(out, x)
		}
		return out
	}
	if !reflect.DeepEqual(run(NewProxyWriteScheduler()), run(quantumScheduler(t, 0))) {
		t.Fatal("zero quantum changed existing proxy policy")
	}
}

// This is a selection/byte regression, not a latency result. It reproduces two
// pending 64 KiB DATA turns ahead of the incident's 1099-byte response. A frame
// already in a socket write remains outside the scheduler's preemption control.
func TestProxyDataQuantumTwoBulkAhead(t *testing.T) {
	for _, quantum := range []int32{0, 16384} {
		ws := quantumScheduler(t, quantum)
		sc := &serverConn{maxFrameSize: 16777215}
		sc.flow.add(1 << 20)
		for i := uint32(1); i <= 5; i += 2 {
			st := quantumStream(ws, sc, i, 1<<20)
			n := 65536
			if i == 5 {
				n = 1099
			}
			ws.Push(FrameWriteRequest{stream: st, write: &writeData{streamID: i, p: bytes.Repeat([]byte{byte(i)}, n)}})
		}
		ahead := 0
		for i := 0; i < 3; i++ {
			wr, ok := ws.Pop()
			if !ok {
				t.Fatal("unexpected empty scheduler")
			}
			if wr.StreamID() == 5 {
				if i != 2 || wr.DataSize() != 1099 {
					t.Fatal("target selection/content")
				}
				break
			}
			ahead += wr.DataSize()
		}
		want := 2 * 65536
		if quantum != 0 {
			want = 2 * int(quantum)
		}
		if ahead != want {
			t.Fatalf("quantum=%d competing bytes=%d want=%d", quantum, ahead, want)
		}
		if sc.maxFrameSize != 16777215 {
			t.Fatal("peer frame limit changed")
		}
		t.Logf("quantum=%d competing DATA bytes before 1099B target=%d (no network timing)", quantum, ahead)
	}
}

func TestProxyDataQuantumPartialFIFOAndCompletion(t *testing.T) {
	ws := quantumScheduler(t, 16384)
	sc := &serverConn{maxFrameSize: 16777215}
	sc.flow.add(1 << 20)
	st := quantumStream(ws, sc, 1, 1<<20)
	payload := make([]byte, 65539)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	done, ended := make(chan error, 1), make(chan error, 1)
	ws.Push(FrameWriteRequest{stream: st, write: &writeData{streamID: 1, p: payload}, done: done})
	ws.Push(FrameWriteRequest{stream: st, write: writeWindowUpdate{streamID: 1, n: 4096}})
	ws.Push(FrameWriteRequest{stream: st, write: &writeData{streamID: 1, endStream: true}, done: ended})
	var received []byte
	wu, eof := false, false
	for {
		wr, ok := ws.Pop()
		if !ok {
			break
		}
		switch w := wr.write.(type) {
		case *writeData:
			if len(w.p) > 16384 {
				t.Fatal("DATA turn exceeds quantum")
			}
			if len(w.p) == 0 {
				if !wu || !w.endStream || wr.done != ended {
					t.Fatal("empty END_STREAM reordered")
				}
				eof = true
			} else {
				if wu || eof || w.endStream {
					t.Fatal("DATA reordered or ended early")
				}
				received = append(received, w.p...)
				if len(received) < len(payload) && wr.done != nil {
					t.Fatal("partial write completed handler")
				}
				if len(received) == len(payload) && wr.done != done {
					t.Fatal("final completion lost")
				}
			}
		case writeWindowUpdate:
			if !bytes.Equal(received, payload) || wu || w.n != 4096 {
				t.Fatal("following WU bypassed DATA")
			}
			wu = true
		default:
			t.Fatal("unexpected frame")
		}
		wr.replyToWriter(nil)
	}
	if !eof || !wu || !bytes.Equal(received, payload) {
		t.Fatal("incomplete payload/FIFO")
	}
	for _, ch := range []chan error{done, ended} {
		select {
		case err := <-ch:
			if err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatal("missing completion")
		}
		select {
		case <-ch:
			t.Fatal("duplicate completion")
		default:
		}
	}
	if sc.flow.n != (1<<20)-int32(len(payload)) || st.flow.n != sc.flow.n {
		t.Fatal("flow accounting changed")
	}
}

func TestProxyDataQuantumFlowPeerLimitAndClose(t *testing.T) {
	ws := quantumScheduler(t, 16384)
	sc := &serverConn{maxFrameSize: 8192}
	sc.flow.add(7000)
	st := quantumStream(ws, sc, 1, 9000)
	done := make(chan error, 1)
	ws.Push(FrameWriteRequest{stream: st, write: &writeData{streamID: 1, p: make([]byte, 65536), endStream: true}, done: done})
	pop := func(want int) {
		t.Helper()
		wr, ok := ws.Pop()
		if !ok || wr.DataSize() != want || wr.done != nil || wr.write.(*writeData).endStream {
			t.Fatalf("partial got=%d available=%v want=%d", wr.DataSize(), ok, want)
		}
	}
	pop(7000)
	if _, ok := ws.Pop(); ok {
		t.Fatal("connection credit bypassed")
	}
	sc.flow.add(20000)
	pop(2000)
	if _, ok := ws.Pop(); ok {
		t.Fatal("stream credit bypassed")
	}
	st.flow.add(20000)
	pop(8192)
	if sc.maxFrameSize != 8192 {
		t.Fatal("peer frame limit changed")
	}
	ws.CloseStream(1)
	if _, ok := ws.Pop(); ok {
		t.Fatal("closed stream remainder retained")
	}
	// CloseStream itself drops queued frames; server close-notification, rather
	// than scheduler completion, releases the handler. Do not invent completion.
	select {
	case <-done:
		t.Fatal("closed remainder spuriously succeeded")
	default:
	}
	fresh := quantumStream(ws, sc, 3, 100)
	ws.Push(FrameWriteRequest{stream: fresh, write: &writeData{streamID: 3, p: []byte("fresh")}})
	wr, ok := ws.Pop()
	if !ok || wr.StreamID() != 3 || !bytes.Equal(wr.write.(*writeData).p, []byte("fresh")) {
		t.Fatal("recycled queue retained old frames")
	}
	ws.CloseStream(3)
	checkFairnessRing(t, ws.(*roundRobinWriteScheduler))
}

func TestProxyDataQuantumPreservesFairnessAndGlobalControl(t *testing.T) {
	ws := quantumScheduler(t, 16384)
	sc := &serverConn{maxFrameSize: 16777215}
	sc.flow.add(1 << 28)
	for i := uint32(0); i < 10; i++ {
		st := quantumStream(ws, sc, 2*i+1, 1<<28)
		if i < 9 {
			ws.Push(fifoDiagnosticWU(st))
		} else {
			ws.Push(fifoDiagnosticData(st, 65536))
		}
	}
	// The global-control priority is intentionally unchanged.
	ws.Push(FrameWriteRequest{write: writeWindowUpdate{n: 4096}})
	if wr, ok := ws.Pop(); !ok || wr.StreamID() != 0 {
		t.Fatal("global priority changed")
	}
	last, progress := 0, 0
	for step := 1; step <= 900; step++ {
		wr, ok := ws.Pop()
		if !ok {
			t.Fatal("ready DATA starved")
		}
		if wr.StreamID() == 19 {
			if step-last > 90 || wr.DataSize() != 16384 {
				t.Fatal("ordinary cursor/quantum progress violated")
			}
			last, progress = step, progress+1
			// Refill only after a full original request. Avoid extra queued DATA.
			if progress%4 == 0 {
				ws.Push(fifoDiagnosticData(wr.stream, 65536))
			}
		} else {
			ws.Push(fifoDiagnosticWU(wr.stream))
		}
	}
	if progress == 0 || 900-last >= 90 {
		t.Fatal("DATA starved at tail")
	}
	checkFairnessRing(t, ws.(*roundRobinWriteScheduler))
}

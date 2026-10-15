package http2

import "testing"

func TestProxyQuantumLargePeerFrames(t *testing.T) {
	for _, bounded := range []bool{false, true} {
		var ws WriteScheduler = newRoundRobinWriteScheduler()
		if bounded {
			ws = NewBoundedDataWriteScheduler()
		}
		sc := &serverConn{maxFrameSize: 16777215}
		for id := uint32(1); id <= 65; id += 2 {
			st := &stream{id: id, sc: sc}
			st.flow.add(1 << 20)
			ws.OpenStream(id, OpenStreamOptions{})
			ws.Push(FrameWriteRequest{stream: st, write: &writeData{streamID: id, p: make([]byte, 65536)}})
		}
		st := &stream{id: 99, sc: sc}
		ws.OpenStream(99, OpenStreamOptions{})
		ws.Push(FrameWriteRequest{stream: st, write: &writeResHeaders{streamID: 99}})
		ahead := 0
		for {
			wr, ok := ws.Pop()
			if !ok {
				t.Fatal("lost headers")
			}
			if wr.StreamID() == 99 {
				break
			}
			ahead += wr.DataSize()
		}
		want := 33 * 65536
		if bounded {
			want = 33 * 16384
		}
		if ahead != want {
			t.Fatalf("bounded=%v bytes=%d want=%d", bounded, ahead, want)
		}
		t.Logf("bounded=%v DATA bytes ahead of new headers=%d", bounded, ahead)
	}
}

func TestProxyQuantumFlowAndCompletion(t *testing.T) {
	ws := NewBoundedDataWriteScheduler()
	sc := &serverConn{maxFrameSize: 16777215}
	st := &stream{id: 1, sc: sc}
	st.flow.add(10000)
	done := make(chan error, 1)
	ws.OpenStream(1, OpenStreamOptions{})
	ws.Push(FrameWriteRequest{stream: st, write: &writeData{streamID: 1, p: make([]byte, 65536), endStream: true}, done: done})
	first, ok := ws.Pop()
	if !ok || first.DataSize() != 10000 || first.done != nil || first.write.(*writeData).endStream {
		t.Fatal("initial credit/end semantics")
	}
	if _, ok := ws.Pop(); ok {
		t.Fatal("DATA bypassed zero credit")
	}
	st.flow.add(65536)
	total := 10000
	final := false
	for {
		wr, ok := ws.Pop()
		if !ok {
			break
		}
		if wr.DataSize() > 16384 {
			t.Fatal("quantum exceeded")
		}
		total += wr.DataSize()
		if wr.done != nil {
			if total != 65536 || wr.done != done || !wr.write.(*writeData).endStream {
				t.Fatal("premature final completion")
			}
			final = true
		}
	}
	if total != 65536 || !final {
		t.Fatal("lost data/final notification")
	}
}

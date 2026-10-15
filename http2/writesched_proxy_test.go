package http2

import "testing"

func TestProxySchedulerHeadersAndFairness(t *testing.T) {
	for _, optimized := range []bool{false, true} {
		var ws WriteScheduler = newRoundRobinWriteScheduler()
		if optimized {
			ws = NewProxyWriteScheduler()
		}
		sc := &serverConn{maxFrameSize: 16384}
		for i := uint32(1); i <= 65; i += 2 {
			st := &stream{id: i, sc: sc}
			st.flow.add(1 << 20)
			ws.OpenStream(i, OpenStreamOptions{})
			ws.Push(FrameWriteRequest{stream: st, write: &writeData{streamID: i, p: make([]byte, 65536)}})
		}
		h := &stream{id: 99, sc: sc}
		ws.OpenStream(99, OpenStreamOptions{})
		ws.Push(FrameWriteRequest{stream: h, write: &writeResHeaders{streamID: 99}})
		bytesBefore := 0
		for {
			wr, ok := ws.Pop()
			if !ok {
				t.Fatal("lost headers")
			}
			if wr.StreamID() == 99 {
				break
			}
			bytesBefore += wr.DataSize()
		}
		if optimized && bytesBefore != 0 {
			t.Fatal("headers remained behind bulk data", bytesBefore)
		}
		if !optimized && bytesBefore != 33*16384 {
			t.Fatal("baseline changed", bytesBefore)
		}
		t.Logf("optimized=%v bulk_bytes_before_new_headers=%d", optimized, bytesBefore)
	}
	// Repeated header arrivals must not starve DATA.
	ws := NewProxyWriteScheduler()
	sc := &serverConn{maxFrameSize: 16384}
	data := &stream{id: 1, sc: sc}
	data.flow.add(1 << 20)
	ws.OpenStream(1, OpenStreamOptions{})
	ws.Push(FrameWriteRequest{stream: data, write: &writeData{streamID: 1, p: make([]byte, 65536)}})
	found := false
	for i := uint32(3); i < 23; i += 2 {
		st := &stream{id: i, sc: sc}
		ws.OpenStream(i, OpenStreamOptions{})
		ws.Push(FrameWriteRequest{stream: st, write: &writeResHeaders{streamID: i}})
		wr, ok := ws.Pop()
		if !ok {
			t.Fatal("empty")
		}
		if wr.StreamID() == 1 {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("DATA starved by new headers")
	}
}

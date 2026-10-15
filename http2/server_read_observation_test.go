//go:build go1.25 || goexperiment.synctest

package http2

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestProxyReadObservationIndependentPhases(t *testing.T) {
	synctestTest(t, func(t testing.TB) {
		a, b := net.Pipe()
		defer a.Close()
		defer b.Close()
		r := &ServerReadState{origin: time.Now()}
		sc := &serverConn{framer: NewFramer(io.Discard, a), readFrameCh: make(chan readFrameResult),
			doneServing: make(chan struct{}), observationReader: r}
		done := make(chan struct{})
		go func() { sc.readFrames(); close(done) }()
		want := func(phase string, frames uint64) {
			t.Helper()
			synctest.Wait()
			o := r.Snapshot()
			if o.Phase != phase || o.FramesDecoded != frames {
				t.Fatalf("got %+v, want %s/%d", o, phase, frames)
			}
		}
		want("reading_frame", 0)
		// Bytes arrived and were consumed, but the frame header is incomplete.
		b.Write([]byte{0, 0, 0})
		time.Sleep(2 * time.Second)
		want("reading_frame", 0)
		if r.Snapshot().PhaseAgeMS < 2000 {
			t.Fatal("partial frame reset the reading phase age")
		}
		// Complete an empty SETTINGS frame. No serve loop exists in this test:
		// independently sampled handoff state must still advance.
		b.Write([]byte{4, 0, 0, 0, 0, 0})
		want("handoff", 1)
		time.Sleep(time.Second)
		if r.Snapshot().PhaseAgeMS < 1000 {
			t.Fatal("handoff age unavailable without serve callback")
		}
		res := <-sc.readFrameCh
		want("await_serve", 1)
		time.Sleep(time.Second)
		if r.Snapshot().PhaseAgeMS < 1000 {
			t.Fatal("serve-processing wait age missing")
		}
		res.readMore()
		want("reading_frame", 1)
		close(sc.doneServing)
		b.Close()
		<-done
		want("stopped", 1)
		encoded, _ := json.Marshal(r.Snapshot())
		for _, private := range []string{"127.0.0.1", "stream_id", "payload", "SETTINGS"} {
			if strings.Contains(string(encoded), private) {
				t.Fatal("private frame data exposed")
			}
		}
	})
}

func TestProxyReadObservationDisabled(t *testing.T) {
	var r *ServerReadState
	r.setPhase(readPhaseReading)
	if r.Snapshot() != nil {
		t.Fatal("disabled observation must be unknown")
	}
}

func BenchmarkProxyReadObservationTransitions(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		name := "off"
		var r *ServerReadState
		if enabled {
			name = "on"
			r = &ServerReadState{origin: time.Now()}
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				r.setPhase(readPhaseReading)
				if r != nil {
					r.frames.Add(1)
				}
				r.setPhase(readPhaseHandoff)
				r.setPhase(readPhaseAwaitServe)
			}
		})
	}
}

func TestProxyReadObservationHookIndependentOfEnableGate(t *testing.T) {
	synctestTest(t, func(t testing.TB) {
		var state *ServerReadState
		calls := 0
		st := newServerTester(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }, func(s *Server) {
			s.ObserveEnabled = func(context.Context) bool { return false }
			s.ObserveReadState = func(_ context.Context, r *ServerReadState) { state = r; calls++ }
		})
		st.greet()
		synctest.Wait()
		if calls != 1 || state == nil || state.Snapshot().Phase != "reading_frame" {
			t.Fatal("reader registration unavailable independently of enabled serve observation")
		}
		before := state.Snapshot().FramesDecoded
		st.writePing(false, [8]byte{1})
		_ = readFrame[*PingFrame](t, st)
		synctest.Wait()
		if state.Snapshot().FramesDecoded <= before || calls != 1 {
			t.Fatal("reader state did not advance or hook repeated")
		}
	})
}

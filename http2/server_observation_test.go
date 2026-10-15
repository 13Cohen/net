//go:build go1.25 || goexperiment.synctest

package http2

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestProxyPingACKBudgetStartsAfterFlush(t *testing.T) {
	for _, fixed := range []bool{false, true} {
		name := "legacy"
		if fixed {
			name = "after_flush"
		}
		t.Run(name, func(t *testing.T) {
			synctestTest(t, func(t testing.TB) {
				st := newServerTester(t, func(w http.ResponseWriter, r *http.Request) {}, func(s *Server) {
					s.ReadIdleTimeout = time.Second
					s.PingTimeout = 2 * time.Second
					s.WriteByteTimeout = 10 * time.Second
					s.PingTimeoutAfterWrite = fixed
				})
				st.greet()
				st.cc.(*synctestNetConn).SetReadBufferSize(0)
				st.advance(4 * time.Second)
				if !fixed {
					if !st.cc.(*synctestNetConn).IsClosedByPeer() {
						t.Fatal("baseline did not reproduce premature close")
					}
					return
				}
				if st.cc.(*synctestNetConn).IsClosedByPeer() {
					t.Fatal("PING closed before it could flush")
				}
				st.cc.(*synctestNetConn).SetReadBufferSize(65536)
				synctest.Wait()
				pf := readFrame[*PingFrame](t, st)
				st.advance(time.Second)
				st.writePing(true, pf.Data)
				st.wantIdle()
				st.advance(time.Second)
				_ = readFrame[*PingFrame](t, st)
				st.advance(2 * time.Second)
				st.wantClosed() // still closes an actually unanswered PING
			})
		})
	}
}

func TestProxyPingBlockedWriterStillBounded(t *testing.T) {
	synctestTest(t, func(t testing.TB) {
		st := newServerTester(t, nil, func(s *Server) {
			s.ReadIdleTimeout = time.Second
			s.PingTimeout = time.Second
			s.WriteByteTimeout = 3 * time.Second
			s.PingTimeoutAfterWrite = true
		})
		st.greet()
		st.cc.(*synctestNetConn).SetReadBufferSize(0)
		st.advance(5 * time.Second)
		st.wantClosed()
	})
}

func TestProxyObservationFlowAndPrivacy(t *testing.T) {
	synctestTest(t, func(t testing.TB) {
		var snapshots []ServerObservation
		st := newServerTester(t, func(w http.ResponseWriter, r *http.Request) {
			if ObservationStreamID(r.Context()) != 1 {
				t.Error("stream context missing")
			}
			w.Write([]byte("secret-payload"))
			w.(http.Flusher).Flush()
		}, func(s *Server) {
			s.Observe = func(_ context.Context, o ServerObservation) { snapshots = append(snapshots, o) }
		})
		st.greet()
		st.writeSettings(Setting{SettingInitialWindowSize, 0})
		st.wantSettingsAck()
		st.bodylessReq1()
		_ = readFrame[*HeadersFrame](t, st)
		st.advance(2 * time.Second)
		synctest.Wait()
		if len(snapshots) == 0 {
			t.Fatal("no pending snapshots")
		}
		last := snapshots[len(snapshots)-1]
		if last.FlowBlockedStreams != 1 || len(last.Examples) != 1 || last.Examples[0].SendWindow != 0 || last.Examples[0].Available != 0 {
			t.Fatalf("not a stream flow wait: %+v", last)
		}
		encoded, _ := json.Marshal(last)
		for _, secret := range []string{"secret-payload", "127.0.0.1", "authorization"} {
			if strings.Contains(string(encoded), secret) {
				t.Fatal("private data exported")
			}
		}
		st.writeWindowUpdate(1, 65535)
		_ = readFrame[*DataFrame](t, st)
	})
}

// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package http2

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestQueueObservationFeedbackAheadOfData(t *testing.T) {
	ws, base := fifoDiagnosticScheduler("production_rr")
	ss := fifoDiagnosticStreams(ws, 1<<30)
	now := time.Unix(1000, 0)
	for i := 0; i < 168; i++ {
		wr := fifoDiagnosticWU(ss[0])
		wr.observationEnqueued = now.Add(-72 * time.Second)
		wr.observationQueuedAt = now.Add(-70 * time.Second)
		ws.Push(wr)
	}
	wr := fifoDiagnosticData(ss[0], 1552)
	wr.observationEnqueued = now.Add(-12 * time.Second)
	wr.observationQueuedAt = now.Add(-10 * time.Second)
	ws.Push(wr)
	head, pos, credit := base.head, base.streams[1].currPos, ss[0].flow.available()
	o, ok := base.observeStreamQueue(1, now)
	if !ok || o.TotalFrames != 169 || o.ScannedFrames != 169 || o.Truncated || o.HeadKind != "window_update" || o.HeadAgeMS != 70000 || o.FirstDataAgeMS != 10000 || o.FirstDataRequestAgeMS != 12000 || o.FirstDataPreQueueMS != 2000 || o.FramesBeforeData != 168 || o.ScannedDataBytes != 1552 || o.Counts.WindowUpdate != 168 || o.Counts.Data != 1 {
		t.Fatalf("wrong exact FIFO evidence: %+v", o)
	}
	if base.head != head || base.streams[1].currPos != pos || ss[0].flow.available() != credit {
		t.Fatal("observation mutated scheduler or flow")
	}
	for i := 0; i < 168; i++ {
		wr, ok := ws.Pop()
		if !ok || wr.DataSize() != 0 {
			t.Fatal("feedback order changed")
		}
	}
	next, _ := base.observeStreamQueue(1, now.Add(time.Second))
	if next.HeadKind != "data" || next.TotalFrames != 1 || next.FramesBeforeData != 0 || next.FirstDataAgeMS != 11000 || next.ScannedDataBytes != 1552 {
		t.Fatalf("DATA did not reach head: %+v", next)
	}
	if o.Counts.WindowUpdate != 168 { // prior immutable value survives mutations
		t.Fatal("old observation changed")
	}
}

func TestQueueObservationBoundAndUnknown(t *testing.T) {
	ws, base := fifoDiagnosticScheduler("production_rr")
	ss := fifoDiagnosticStreams(ws, 1<<30)
	now := time.Unix(1000, 0)
	for i := 0; i < 1025; i++ {
		ws.Push(fifoDiagnosticWU(ss[0]))
	}
	data := fifoDiagnosticData(ss[0], 1552)
	data.observationQueuedAt = data.observationEnqueued
	ws.Push(data)
	o, _ := base.observeStreamQueue(1, now)
	if !o.Truncated || o.TotalFrames != 1026 || o.ScannedFrames != 1024 || o.Counts.WindowUpdate != 1024 || o.FramesBeforeData != -1 || o.FirstDataAgeMS != -1 || o.HeadAgeMS != -1 {
		t.Fatalf("truncation/unknown hidden: %+v", o)
	}
	ws.Pop()
	ws.Pop()
	o, _ = base.observeStreamQueue(1, now)
	if o.Truncated || o.TotalFrames != 1024 || o.Counts.WindowUpdate != 1023 || o.FramesBeforeData != 1023 || o.FirstDataAgeMS != 999000 {
		t.Fatalf("complete prefix after drain: %+v", o)
	}
	ws.CloseStream(1)
	if _, ok := base.observeStreamQueue(1, now); ok {
		t.Fatal("closed stream still reported")
	}
	ws.OpenStream(7, OpenStreamOptions{})
	o, _ = base.observeStreamQueue(7, now)
	if o.TotalFrames != 0 || o.ScannedFrames != 0 || o.HeadAgeMS != -1 || o.FramesBeforeData != -1 || o.HeadKind != "" {
		t.Fatalf("recycled queue leaked evidence: %+v", o)
	}
	if observationAgeAt(now, now.Add(time.Second)) != -1 {
		t.Fatal("future age is not unknown")
	}
}

func TestQueueObservationKindsAndPartialData(t *testing.T) {
	ws, base := fifoDiagnosticScheduler("production_rr")
	ss := fifoDiagnosticStreams(ws, 1<<30)
	ss[0].sc.maxFrameSize = 7
	now := time.Unix(1000, 0)
	wr := fifoDiagnosticData(ss[0], 17)
	wr.observationEnqueued = now.Add(-5 * time.Second)
	wr.observationQueuedAt = now.Add(-3 * time.Second)
	ws.Push(wr)
	ws.Push(fifoDiagnosticData(ss[0], 0))
	ws.Push(FrameWriteRequest{stream: ss[0], write: &writeResHeaders{streamID: 1, httpResCode: 200}})
	ws.Push(fifoDiagnosticWU(ss[0]))
	ws.Push(FrameWriteRequest{write: writeWindowUpdate{n: 4096}})
	control := base.observeControlQueue(now)
	if control.TotalFrames != 1 || control.Counts.WindowUpdate != 1 || control.HeadKind != "window_update" {
		t.Fatal(control)
	}
	ws.Pop() // connection-level WINDOW_UPDATE is in a separate queue
	var selected SelectionObservation
	for _, remaining := range []int{10, 3, 0} {
		wr, ok := ws.Pop()
		if !ok {
			t.Fatal("DATA unexpectedly blocked")
		}
		selected.add(wr)
		o, _ := base.observeStreamQueue(1, now)
		if o.ScannedDataBytes != remaining || o.Counts.EmptyData != 1 || o.Counts.Headers != 1 || o.Counts.WindowUpdate != 1 {
			t.Fatalf("partial accounting: %+v", o)
		}
		if remaining > 0 && (o.TotalFrames != 4 || o.FirstDataAgeMS != 3000 || o.FirstDataPreQueueMS != 2000 || o.HeadKind != "data") {
			t.Fatalf("logical enqueue age lost: %+v", o)
		}
		if remaining == 0 && (o.TotalFrames != 3 || o.FramesBeforeData != -1 || o.HeadKind != "empty_data") {
			t.Fatalf("empty DATA counted as payload: %+v", o)
		}
	}
	for i := 0; i < 3; i++ {
		wr, ok := ws.Pop()
		if !ok {
			t.Fatal("remaining frame missing")
		}
		selected.add(wr)
	}
	if selected.Frames != 6 || selected.DataBytes != 17 || selected.WindowUpdateFrames != 1 {
		t.Fatalf("selection != delivery counters: %+v", selected)
	}
}

type queueObservationHiddenScheduler struct{ WriteScheduler }

func TestQueueObservationGateBoundsAndPrivacy(t *testing.T) {
	ws := newRoundRobinWriteScheduler().(*roundRobinWriteScheduler)
	now := time.Now()
	sc := &serverConn{baseCtx: context.Background(), streams: make(map[uint32]*stream), writeSched: ws}
	var snapshots []ServerObservation
	sc.srv = &Server{Observe: func(_ context.Context, o ServerObservation) { snapshots = append(snapshots, o) }}
	for i := uint32(1); i <= 23; i += 2 {
		st := &stream{id: i, sc: sc, observationFrames: 1, observationPending: 13, observationQueued: now.Add(-time.Duration(i) * time.Second)}
		st.observationSelections = SelectionObservation{Frames: uint64(i), WindowUpdateFrames: uint64(i)}
		st.observationLastSelected = now.Add(-time.Second)
		sc.streams[i] = st
		ws.OpenStream(i, OpenStreamOptions{})
		ws.Push(FrameWriteRequest{stream: st, observationEnqueued: st.observationQueued, observationQueuedAt: st.observationQueued,
			write: &writeData{streamID: i, p: []byte("secret-body!!")}})
	}
	sc.srv.ObserveEnabled = func(context.Context) bool { return false }
	sc.observe("test")
	if len(snapshots) != 0 || sc.observationSequence != 0 {
		t.Fatal("disabled observer ran")
	}
	sc.srv.ObserveEnabled = func(context.Context) bool { return true }
	sc.observe("test")
	o := snapshots[0]
	if o.QueueEvidenceVersion != 1 || o.Scheduler != "round_robin" || len(o.Examples) != 8 || o.Examples[0].ID != 23 {
		t.Fatalf("bounded oldest examples missing: %+v", o)
	}
	for _, e := range o.Examples {
		if e.QueueDetail == nil || e.QueueDetail.TotalFrames != 1 || e.QueueDetail.HeadKind != "data" || e.QueueDetail.FirstDataAgeMS < 0 || e.Selections.WindowUpdateFrames != uint64(e.ID) || e.LastSelectedAgeMS < 0 {
			t.Fatal("queue/progress evidence missing")
		}
	}
	encoded, err := json.Marshal(o)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"secret-body", "127.0.0.1", "authorization"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatal("private data exported")
		}
	}
	ws.CloseStream(23)
	if again, _ := json.Marshal(o); !reflect.DeepEqual(again, encoded) {
		t.Fatal("snapshot mutated after close")
	}
	sc.writeSched = queueObservationHiddenScheduler{ws}
	sc.observe("unsupported")
	last := snapshots[len(snapshots)-1]
	if last.QueueEvidenceVersion != 0 || last.ControlQueueDetail != nil {
		t.Fatal("unsupported scheduler misreported")
	}
	for _, e := range last.Examples {
		if e.QueueDetail != nil {
			t.Fatal("unsupported stream misreported")
		}
	}
}

func BenchmarkQueueObservationBoundedScan(b *testing.B) {
	for _, n := range []int{169, 1024, 10000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			q := new(writeQueue)
			now := time.Now()
			for i := 0; i < n; i++ {
				q.push(FrameWriteRequest{write: writeWindowUpdate{n: 4096}, observationEnqueued: now, observationQueuedAt: now})
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = observeWriteQueue(q, now)
			}
		})
	}
}

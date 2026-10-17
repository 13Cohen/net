// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.
package http2

import (
	"context"
	"sort"
	"time"
)

type observationStreamKey struct{}

// ObservationStreamID returns a connection-local ID, or zero when not observed.
func ObservationStreamID(ctx context.Context) uint32 {
	id, _ := ctx.Value(observationStreamKey{}).(uint32)
	return id
}

// ServerObservation contains no headers, addresses, payload or PING opaque data.
// It is an immutable value copied by the optional observer on the serve goroutine.
type ServerObservation struct {
	Event                   string               `json:"event"`
	Sequence                uint64               `json:"sequence"`
	UnixMS                  int64                `json:"unix_ms"`
	SendWindow              int32                `json:"connection_send_window"`
	InitialStreamWindow     int32                `json:"initial_stream_window"`
	MaxFrameSize            int32                `json:"max_frame_size"`
	Streams                 int                  `json:"streams"`
	Handlers                uint32               `json:"handlers"`
	HandlerWriteQueue       int                  `json:"handler_write_queue"`
	ControlQueue            int                  `json:"control_queue"`
	Writing                 bool                 `json:"writing"`
	WriteKind               string               `json:"write_kind"`
	WriteStream             uint32               `json:"write_stream"`
	WriteAgeMS              float64              `json:"write_age_ms"`
	MaxQueueMS              float64              `json:"max_queue_ms"`
	LastFrameReadAgeMS      float64              `json:"last_frame_read_age_ms"`
	PingPending             bool                 `json:"ping_pending"`
	PingQueuedAgeMS         float64              `json:"ping_queued_age_ms"`
	PingWrittenAgeMS        float64              `json:"ping_written_age_ms"`
	LastPingACKAgeMS        float64              `json:"last_ping_ack_age_ms"`
	PingACKBudgetAfterFlush bool                 `json:"ping_ack_budget_after_flush"`
	WindowUpdates           uint64               `json:"window_updates"`
	ResetsReceived          uint64               `json:"resets_received"`
	FramesWritten           uint64               `json:"frames_completed"`
	DataWritten             uint64               `json:"data_bytes_completed"`
	PendingStreams          int                  `json:"pending_streams"`
	FlowBlockedStreams      int                  `json:"flow_blocked_streams"`
	Examples                []StreamObservation  `json:"examples,omitempty"`
	QueueEvidenceVersion    int                  `json:"queue_evidence_version"`
	Scheduler               string               `json:"scheduler,omitempty"`
	ControlQueueDetail      *QueueObservation    `json:"control_queue_detail,omitempty"`
	Selections              SelectionObservation `json:"selections"`
}

type StreamObservation struct {
	ID                uint32               `json:"stream_id"`
	SendWindow        int32                `json:"stream_send_window"`
	Available         int32                `json:"available_data_credit"`
	PendingBytes      int                  `json:"pending_data_bytes"`
	QueuedFrames      int                  `json:"queued_frames"`
	QueueAgeMS        float64              `json:"queue_age_ms"`
	QueueDetail       *QueueObservation    `json:"queue_detail,omitempty"`
	Selections        SelectionObservation `json:"selections"`
	LastSelectedAgeMS float64              `json:"last_selected_age_ms"`
}

func observationAge(t time.Time) float64 {
	if t.IsZero() {
		return -1
	}
	return float64(time.Since(t)) / float64(time.Millisecond)
}
func observationFrameKind(wr FrameWriteRequest) string {
	switch wr.write.(type) {
	case *writeData:
		if wr.DataSize() == 0 {
			return "empty_data"
		}
		return "data"
	case *writeResHeaders, write100ContinueHeadersFrame:
		return "headers"
	case writeWindowUpdate:
		return "window_update"
	case writeSettings, writeSettingsAck:
		return "settings"
	case *writeGoAway:
		return "goaway"
	case *writePing:
		return "ping"
	case writePingAck:
		return "ping_ack"
	case flushFrameWriter:
		return "flush"
	case StreamError, handlerPanicRST:
		return "reset"
	default:
		return "control"
	}
}
func (sc *serverConn) observe(event string) {
	if sc.srv.Observe == nil || (sc.srv.ObserveEnabled != nil && !sc.srv.ObserveEnabled(sc.baseCtx)) {
		return
	}
	sc.observationSequence++
	now := time.Now()
	o := ServerObservation{Event: event, Sequence: sc.observationSequence, UnixMS: now.UnixMilli(), SendWindow: sc.flow.n,
		InitialStreamWindow: sc.initialStreamSendWindowSize, MaxFrameSize: sc.maxFrameSize, Streams: len(sc.streams), Handlers: sc.curHandlers,
		HandlerWriteQueue: len(sc.wantWriteFrameCh), ControlQueue: sc.queuedControlFrames, Writing: sc.writingFrame,
		WriteKind: sc.observationWriteKind, WriteStream: sc.observationWriteStream, WriteAgeMS: observationAge(sc.observationWriteAt),
		MaxQueueMS: sc.observationMaxQueueMS, LastFrameReadAgeMS: observationAge(sc.observationLastRead),
		PingPending: sc.pingSent, PingQueuedAgeMS: observationAge(sc.observationPingQueued), PingWrittenAgeMS: observationAge(sc.observationPingWritten),
		LastPingACKAgeMS: observationAge(sc.observationPingACK), PingACKBudgetAfterFlush: sc.pingTimeoutAfterWrite,
		WindowUpdates: sc.observationWindowUpdates, ResetsReceived: sc.observationResetsReceived,
		FramesWritten: sc.observationFramesWritten, DataWritten: sc.observationDataWritten,
		Selections: sc.observationSelections}
	for _, st := range sc.streams {
		if st.observationFrames <= 0 {
			continue
		}
		o.PendingStreams++
		if st.observationPending > 0 && st.flow.available() <= 0 {
			o.FlowBlockedStreams++
		}
		if len(o.Examples) < 8 || observationAgeAt(now, st.observationQueued) > o.Examples[len(o.Examples)-1].QueueAgeMS {
			o.Examples = append(o.Examples, StreamObservation{ID: st.id, SendWindow: st.flow.n,
				Available: st.flow.available(), PendingBytes: st.observationPending,
				QueuedFrames: st.observationFrames, QueueAgeMS: observationAgeAt(now, st.observationQueued),
				Selections: st.observationSelections, LastSelectedAgeMS: observationAgeAt(now, st.observationLastSelected)})
			sort.Slice(o.Examples, func(i, j int) bool { return o.Examples[i].QueueAgeMS > o.Examples[j].QueueAgeMS })
			if len(o.Examples) > 8 {
				o.Examples = o.Examples[:8]
			}
		}
	}
	if qs, ok := sc.writeSched.(schedulerQueueObserver); ok {
		o.QueueEvidenceVersion = 1
		o.Scheduler = qs.observationSchedulerName()
		control := qs.observeControlQueue(now)
		if control.TotalFrames > 0 {
			o.ControlQueueDetail = &control
		}
		for i := range o.Examples {
			if q, ok := qs.observeStreamQueue(o.Examples[i].ID, now); ok {
				o.Examples[i].QueueDetail = &q
			}
		}
	}
	sc.srv.Observe(sc.baseCtx, o)
}

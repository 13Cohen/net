// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package http2

import "time"

// Queue snapshots are only requested by an enabled server observer, on the
// serve goroutine. Inspect at most nine queues (eight stream examples and the
// control queue), with a fixed per-queue scan bound. No payload is copied.
const observationQueueScanLimit = 1024

// QueueFrameCounts counts logical requests in the inspected FIFO prefix, not
// wire frames. A partially selected DATA request remains one logical request.
type QueueFrameCounts struct {
	Data         int `json:"data,omitempty"`
	EmptyData    int `json:"empty_data,omitempty"`
	Headers      int `json:"headers,omitempty"`
	WindowUpdate int `json:"window_update,omitempty"`
	Ping         int `json:"ping,omitempty"`
	Reset        int `json:"reset,omitempty"`
	Other        int `json:"other,omitempty"`
}

// QueueObservation is a bounded, read-only FIFO snapshot. Counts describe only
// ScannedFrames when Truncated is true. The head and first positive-payload DATA
// found in the prefix are exact; a negative age or index means unknown/absent,
// not zero wait. DATA age starts at its actual scheduler insertion, and is retained
// across partial consumption. Request age also includes handler handoff before
// scheduler insertion; PreQueueMS separates that delay. Neither age measures
// TLS or TCP delivery time.
type QueueObservation struct {
	TotalFrames           int              `json:"total_frames"`
	ScannedFrames         int              `json:"scanned_frames"`
	Truncated             bool             `json:"truncated"`
	HeadKind              string           `json:"head_kind"`
	HeadAgeMS             float64          `json:"head_age_ms"`
	FirstDataAgeMS        float64          `json:"first_data_age_ms"`
	FirstDataRequestAgeMS float64          `json:"first_data_request_age_ms"`
	FirstDataPreQueueMS   float64          `json:"first_data_pre_queue_ms"`
	FramesBeforeData      int              `json:"frames_before_first_data"`
	ScannedDataBytes      int              `json:"scanned_data_bytes"`
	Counts                QueueFrameCounts `json:"counts"`
}

// SelectionObservation counts calls to startFrameWrite, including synchronous
// frames that can be missed by a periodic current-writer snapshot. These are
// cumulative write starts within this stream/connection, not completed delivery.
// Frames also includes synthetic flush and direct connection-control writes;
// failed starts count too. It is not a scheduler Pop or wire-frame counter.
type SelectionObservation struct {
	Frames             uint64 `json:"frames"`
	DataBytes          uint64 `json:"data_bytes"`
	WindowUpdateFrames uint64 `json:"window_update_frames"`
}

func (o *SelectionObservation) add(wr FrameWriteRequest) {
	o.Frames++
	o.DataBytes += uint64(wr.DataSize())
	if _, ok := wr.write.(writeWindowUpdate); ok {
		o.WindowUpdateFrames++
	}
}

type schedulerQueueObserver interface {
	observeStreamQueue(uint32, time.Time) (QueueObservation, bool)
	observeControlQueue(time.Time) QueueObservation
	observationSchedulerName() string
}

func (ws *roundRobinWriteScheduler) observationSchedulerName() string {
	if ws.preferHeaders {
		return "proxy_round_robin"
	}
	return "round_robin"
}

func (ws *roundRobinWriteScheduler) observeStreamQueue(id uint32, now time.Time) (QueueObservation, bool) {
	q, ok := ws.streams[id]
	if !ok {
		return QueueObservation{}, false
	}
	return observeWriteQueue(q, now), true
}

func (ws *roundRobinWriteScheduler) observeControlQueue(now time.Time) QueueObservation {
	return observeWriteQueue(&ws.control, now)
}

func observationAgeAt(now, then time.Time) float64 {
	if then.IsZero() || then.After(now) {
		return -1
	}
	return float64(now.Sub(then)) / float64(time.Millisecond)
}

func observeWriteQueue(q *writeQueue, now time.Time) QueueObservation {
	o := QueueObservation{TotalFrames: len(q.currQueue) - q.currPos + len(q.nextQueue),
		HeadAgeMS: -1, FirstDataAgeMS: -1, FirstDataRequestAgeMS: -1, FirstDataPreQueueMS: -1, FramesBeforeData: -1}
	o.Truncated = o.TotalFrames > observationQueueScanLimit
	for _, part := range [2][]FrameWriteRequest{q.currQueue[q.currPos:], q.nextQueue} {
		for i := range part {
			if o.ScannedFrames == observationQueueScanLimit {
				return o
			}
			wr := &part[i]
			if o.ScannedFrames == 0 {
				o.HeadKind = observationFrameKind(*wr)
				o.HeadAgeMS = observationAgeAt(now, wr.observationQueuedAt)
			}
			size := wr.DataSize()
			if size > 0 {
				if o.FramesBeforeData < 0 {
					o.FramesBeforeData = o.ScannedFrames
					o.FirstDataAgeMS = observationAgeAt(now, wr.observationQueuedAt)
					o.FirstDataRequestAgeMS = observationAgeAt(now, wr.observationEnqueued)
					if !wr.observationQueuedAt.IsZero() {
						o.FirstDataPreQueueMS = observationAgeAt(wr.observationQueuedAt, wr.observationEnqueued)
					}
				}
				o.ScannedDataBytes += size
			}
			switch wr.write.(type) {
			case *writeData:
				if size > 0 {
					o.Counts.Data++
				} else {
					o.Counts.EmptyData++
				}
			case *writeResHeaders, write100ContinueHeadersFrame:
				o.Counts.Headers++
			case writeWindowUpdate:
				o.Counts.WindowUpdate++
			case *writePing, writePingAck:
				o.Counts.Ping++
			case StreamError, handlerPanicRST:
				o.Counts.Reset++
			default:
				o.Counts.Other++
			}
			o.ScannedFrames++
		}
	}
	return o
}

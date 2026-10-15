// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package http2

import (
	"sync/atomic"
	"time"
)

const (
	readPhaseStarting uint64 = iota
	readPhaseReading
	readPhaseHandoff
	readPhaseAwaitServe
	readPhaseStopped
)

var readPhaseNames = [...]string{"starting", "reading_frame", "handoff", "await_serve", "stopped"}

// ServerReadState is a bounded, concurrent-safe metadata handle for one frame
// reader. It exposes no frame contents, addresses or stream identifiers. Only
// the readFrames goroutine writes it; Snapshot may run independently while the
// serve loop is blocked. It is allocated only when ObserveReadState is set.
type ServerReadState struct {
	origin time.Time
	// Phase and monotonic offset share one atomic value: a sample cannot attach
	// one phase's timestamp to another phase. Three low bits hold the phase.
	phase  atomic.Uint64
	frames atomic.Uint64
}

// ServerReadObservation describes a sample, not a claim about the peer. A
// reading_frame wait may include TCP, TLS buffering/decryption, or waiting for
// the rest of a frame. Handoff/await_serve can include goroutine scheduling.
// FramesDecoded is cumulative; it and the phase are sampled independently.
type ServerReadObservation struct {
	UnixMS        int64   `json:"unix_ms"`
	Phase         string  `json:"phase"`
	PhaseAgeMS    float64 `json:"phase_age_ms"`
	FramesDecoded uint64  `json:"frames_decoded"`
	BestEffort    bool    `json:"best_effort"`
}

func (r *ServerReadState) setPhase(phase uint64) {
	if r != nil {
		r.phase.Store(uint64(time.Since(r.origin).Nanoseconds())<<3 | phase)
	}
}

// Snapshot returns nil for an unsupported/unregistered reader. Age uses a
// monotonic clock; UnixMS is the independent sampling time, not a cached serve
// snapshot's time. The low-bit encoding can represent about 73 years.
func (r *ServerReadState) Snapshot() *ServerReadObservation {
	if r == nil {
		return nil
	}
	packed := r.phase.Load()
	now := time.Now()
	phase := packed & 7
	age := float64(now.Sub(r.origin)-time.Duration(packed>>3)) / float64(time.Millisecond)
	if phase >= uint64(len(readPhaseNames)) || age < 0 {
		return &ServerReadObservation{UnixMS: now.UnixMilli(), Phase: "unknown", PhaseAgeMS: -1, BestEffort: true}
	}
	return &ServerReadObservation{UnixMS: now.UnixMilli(), Phase: readPhaseNames[phase], PhaseAgeMS: age, FramesDecoded: r.frames.Load(), BestEffort: true}
}

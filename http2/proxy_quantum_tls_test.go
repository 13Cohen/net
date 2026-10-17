// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package http2

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

// A real, certificate-verified TLS connection exercises the enabled scheduler.
// One ClientConn pins all requests to the same outer connection. This checks
// cancel/connection-close lifecycle and exact content, not throughput or latency.
func TestProxyDataQuantumTLSCancelAndConnectionClose(t *testing.T) {
	cancelDone, closeDone := make(chan struct{}), make(chan struct{})
	payload := []byte("small-stream-survives-canceled-bulk")
	// Far larger than the client's automatic receive window: reading only a
	// prefix cannot allow the handler to finish before cancellation/close.
	bulkSize := int64(transportDefaultStreamFlow) * 4
	if bulkSize <= 65536 {
		t.Fatal("fixture requires an incomplete large response")
	}
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/small" {
			w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
			_, _ = w.Write(payload)
			return
		}
		var done chan struct{}
		switch r.URL.Path {
		case "/cancel":
			done = cancelDone
		case "/close":
			done = closeDone
		default:
			http.NotFound(w, r)
			return
		}
		defer close(done)
		w.Header().Set("Content-Length", strconv.FormatInt(bulkSize, 10))
		chunk := bytes.Repeat([]byte{0x6b}, 65536)
		for written := int64(0); written < bulkSize; {
			select {
			case <-r.Context().Done():
				return
			default:
			}
			n, err := w.Write(chunk)
			written += int64(n)
			if err != nil {
				return
			}
			if err := http.NewResponseController(w).Flush(); err != nil {
				return
			}
		}
	}))
	ts.EnableHTTP2 = true
	if err := ConfigureServer(ts.Config, &Server{NewWriteScheduler: func() WriteScheduler {
		ws, err := NewProxyWriteSchedulerWithDataQuantum(16384)
		if err != nil {
			panic(err)
		}
		return ws
	}}); err != nil {
		t.Fatal(err)
	}
	ts.StartTLS()
	defer ts.Close()
	roots := x509.NewCertPool()
	roots.AddCert(ts.Certificate())
	tlsConfig := &tls.Config{RootCAs: roots, NextProtos: []string{NextProtoTLS}, MinVersion: tls.VersionTLS13}
	raw, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", ts.Listener.Addr().String(), tlsConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if raw.ConnectionState().NegotiatedProtocol != NextProtoTLS || len(raw.ConnectionState().VerifiedChains) == 0 {
		t.Fatal("fixture requires verified TLS and ALPN h2")
	}
	tr := &Transport{TLSClientConfig: tlsConfig, MaxReadFrameSize: 16777215}
	cc, err := tr.NewClientConn(raw)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	ctx, stop := context.WithTimeout(context.Background(), 20*time.Second)
	defer stop()
	request := func(ctx context.Context, path string) *http.Response {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		res, err := cc.RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != http.StatusOK || res.ProtoMajor != 2 {
			t.Fatal("unexpected protocol/status")
		}
		return res
	}
	prefix := func(res *http.Response, done <-chan struct{}) {
		t.Helper()
		got := make([]byte, 4096)
		if _, err := io.ReadFull(res.Body, got); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, bytes.Repeat([]byte{0x6b}, len(got))) {
			t.Fatal("bulk prefix corrupted")
		}
		if res.ContentLength != bulkSize {
			t.Fatal("bulk fixture length changed")
		}
		select {
		case <-done:
			t.Fatal("bulk handler already finished; cancellation not exercised")
		default:
		}
	}
	wait := func(done <-chan struct{}, action string) {
		t.Helper()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal(fmt.Sprintf("handler did not exit after %s", action))
		}
	}
	checkSmall := func() {
		t.Helper()
		res := request(ctx, "/small")
		got, err := io.ReadAll(io.LimitReader(res.Body, int64(len(payload)+1)))
		res.Body.Close()
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatal("same-connection small response incomplete")
		}
	}

	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	first := request(cancelCtx, "/cancel")
	defer first.Body.Close()
	prefix(first, cancelDone)
	checkSmall() // A small stream progresses while the first bulk is incomplete.
	cancel()     // Transport emits RST_STREAM, keeping this ClientConn alive.
	first.Body.Close()
	wait(cancelDone, "stream cancellation")
	checkSmall() // The same explicit ClientConn cannot hide a reconnect.

	second := request(ctx, "/close")
	defer second.Body.Close()
	prefix(second, closeDone)
	if err := cc.Close(); err != nil {
		t.Fatal(err)
	}
	wait(closeDone, "outer connection close")
	if !cc.State().Closed {
		t.Fatal("ClientConn stayed open")
	}
	t.Log("verified TLS1.3; enabled 16KiB policy; partial bulk cancel preserves exact same-outer small stream; outer close releases pending bulk")
}

/*
Copyright 2025 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package proxy

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/go-logr/logr"
)

// engineStub is a fake vLLM: it answers /health with the status the test wants
// and counts how often it was asked.
func engineStub(t *testing.T, status int) (*httptest.Server, *int) {
	t.Helper()
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			http.NotFound(w, r)
			return
		}
		hits++
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func newHealthServer(t *testing.T, decode *httptest.Server, prefillHost string) *Server {
	t.Helper()
	var decoderURL *url.URL
	if decode != nil {
		u, err := url.Parse(decode.URL)
		if err != nil {
			t.Fatalf("parse decode url: %v", err)
		}
		decoderURL = u
	}
	return &Server{
		logger: logr.Discard(),
		config: Config{DecoderURL: decoderURL, PrefillerHost: prefillHost},
	}
}

func callHealth(t *testing.T, s *Server) int {
	t.Helper()
	rec := httptest.NewRecorder()
	s.healthHandler(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	return rec.Code
}

// TestHealthReportsUnavailableUntilDecodeIsReady pins the bug this handler
// exists for: the sidecar used to answer 200 unconditionally, so the pod went
// Ready while /v1/models still returned "The decode node is not ready" and the
// Service routed traffic into it.
func TestHealthReportsUnavailableUntilDecodeIsReady(t *testing.T) {
	decode, hits := engineStub(t, http.StatusServiceUnavailable)
	s := newHealthServer(t, decode, "")

	if got := callHealth(t, s); got != http.StatusServiceUnavailable {
		t.Fatalf("health with decode down = %d, want 503", got)
	}
	if *hits == 0 {
		t.Fatal("decode was never probed")
	}
}

func TestHealthIsOKWhenBothLegsAreUp(t *testing.T) {
	decode, _ := engineStub(t, http.StatusOK)
	prefill, prefillHits := engineStub(t, http.StatusOK)
	s := newHealthServer(t, decode, prefill.Listener.Addr().String())

	if got := callHealth(t, s); got != http.StatusOK {
		t.Fatalf("health with both legs up = %d, want 200", got)
	}
	if *prefillHits == 0 {
		t.Fatal("prefill was never probed")
	}
}

// A ready decoder does not make the pod usable: the prefill leg still has to
// answer, or the first long request fails after the pod already took traffic.
func TestHealthFailsWhenPrefillIsDown(t *testing.T) {
	decode, _ := engineStub(t, http.StatusOK)
	prefill, _ := engineStub(t, http.StatusServiceUnavailable)
	s := newHealthServer(t, decode, prefill.Listener.Addr().String())

	if got := callHealth(t, s); got != http.StatusServiceUnavailable {
		t.Fatalf("health with prefill down = %d, want 503", got)
	}
}

// No disaggregation configured means the sidecar is the whole data path, so it
// must not report itself unhealthy just because there is nothing to probe.
func TestHealthWithoutLegsIsOK(t *testing.T) {
	s := newHealthServer(t, nil, "")
	if got := callHealth(t, s); got != http.StatusOK {
		t.Fatalf("health with no legs = %d, want 200", got)
	}
}

/*
Copyright 2026 Kube AI Hub.

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
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/llm-d/llm-d-router/pkg/common/routing"
	"github.com/llm-d/llm-d-router/pkg/sidecar/constants"
	"github.com/llm-d/llm-d-router/test/sidecar/mock"
)

// chatBody is a minimal OpenAI chat-completions payload; the PD connector needs
// a parseable body before it will contact the prefill leg.
const chatBody = `{"model":"test","messages":[{"role":"user","content":"Hello"}],"max_tokens":8}`

type staticPrefillerFixture struct {
	proxy        *Server
	decode       *mock.ChatCompletionHandler
	prefill      *mock.ChatCompletionHandler
	prefillURL   string
	prefillImage string
}

// newStaticPrefillerProxy builds a proxy whose decode URL points at a decode
// stub and whose prefill leg is resolved from the static --prefiller-host
// setting rather than a request header.
func newStaticPrefillerProxy(t *testing.T, prefillerHost string) *staticPrefillerFixture {
	t.Helper()

	decodeHandler := &mock.ChatCompletionHandler{
		Role:      mock.RoleDecode,
		Connector: constants.KVConnectorNIXLV2,
	}
	decodeBackend := httptest.NewServer(decodeHandler)
	t.Cleanup(decodeBackend.Close)

	prefillHandler := &mock.ChatCompletionHandler{
		Role:      mock.RolePrefill,
		Connector: constants.KVConnectorNIXLV2,
	}
	prefillBackend := httptest.NewServer(prefillHandler)
	t.Cleanup(prefillBackend.Close)

	decodeURL, err := url.Parse(decodeBackend.URL)
	require.NoError(t, err)

	target := prefillerHost
	if target == "" {
		target = prefillBackend.URL[len("http://"):]
	}

	server := NewProxy(Config{
		Port:          "0",
		DecoderURL:    decodeURL,
		KVConnector:   constants.KVConnectorNIXLV2,
		PrefillerHost: target,
		// A colocated pod has no InferencePool, so SSRF allowlisting stays off
		// and the static sibling engine is dialled directly.
		EnableSSRFProtection: false,
	})
	server.allowlistValidator = &AllowlistValidator{enabled: false}

	return &staticPrefillerFixture{
		proxy:      server,
		decode:     decodeHandler,
		prefill:    prefillHandler,
		prefillURL: prefillBackend.URL[len("http://"):],
	}
}

// start runs the proxy in the background and returns its base address.
func (f *staticPrefillerFixture) start(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithCancel(newTestContext())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = f.proxy.Start(ctx)
	}()
	<-f.proxy.readyCh
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return "http://" + f.proxy.addr.String()
}

func postChat(t *testing.T, base, prefillHeader string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, base+"/v1/chat/completions", strings.NewReader(chatBody))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	if prefillHeader != "" {
		req.Header.Set(routing.PrefillEndpointHeader, prefillHeader)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
}

func TestStaticPrefillerUsedWithoutHeader(t *testing.T) {
	fixture := newStaticPrefillerProxy(t, "")
	base := fixture.start(t)

	postChat(t, base, "")

	require.Equal(t, int32(1), fixture.prefill.RequestCount.Load(), "static prefiller must receive the prefill leg")
	require.Equal(t, int32(1), fixture.decode.RequestCount.Load(), "decode still runs locally")
}

func TestHeaderPrefillerWinsOverStatic(t *testing.T) {
	// The static setting points at a dead address, so a request that succeeds
	// proves the header target was used instead.
	fixture := newStaticPrefillerProxy(t, "127.0.0.1:1")
	base := fixture.start(t)

	headerHandler := &mock.ChatCompletionHandler{
		Role:      mock.RolePrefill,
		Connector: constants.KVConnectorNIXLV2,
	}
	headerBackend := httptest.NewServer(headerHandler)
	defer headerBackend.Close()

	postChat(t, base, headerBackend.URL[len("http://"):])

	require.Equal(t, int32(1), headerHandler.RequestCount.Load(), "the routed prefill target must win over the static one")
	require.Equal(t, int32(0), fixture.prefill.RequestCount.Load(), "the static target must stay unused")
}

func TestStaticPrefillerSkippedWhenUnset(t *testing.T) {
	decodeHandler := &mock.ChatCompletionHandler{
		Role:      mock.RoleDecode,
		Connector: constants.KVConnectorNIXLV2,
	}
	decodeBackend := httptest.NewServer(decodeHandler)
	defer decodeBackend.Close()
	decodeURL, err := url.Parse(decodeBackend.URL)
	require.NoError(t, err)

	server := NewProxy(Config{Port: "0", DecoderURL: decodeURL, KVConnector: constants.KVConnectorNIXLV2})
	server.allowlistValidator = &AllowlistValidator{enabled: false}
	fixture := &staticPrefillerFixture{proxy: server, decode: decodeHandler}
	base := fixture.start(t)

	postChat(t, base, "")

	require.Equal(t, int32(1), fixture.decode.RequestCount.Load(), "without a static target the request is decode-only")
}

func TestOptionsPrefillerHostValidation(t *testing.T) {
	completed := func() *Options {
		opts := NewOptions()
		require.NoError(t, opts.Complete())
		return opts
	}

	opts := completed()
	opts.PrefillerHost = "127.0.0.1:8100"
	require.NoError(t, opts.Validate())

	opts = completed()
	opts.PrefillerHost = "127.0.0.1"
	require.ErrorContains(t, opts.Validate(), "--prefiller-host must be host:port")

	opts = completed()
	opts.PrefillerHost = ":8100"
	require.ErrorContains(t, opts.Validate(), "--prefiller-host must be host:port")
}

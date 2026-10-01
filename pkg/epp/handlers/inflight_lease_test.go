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

package handlers

import (
	"testing"
	"time"

	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	reqcommon "github.com/llm-d/llm-d-router/pkg/common/request"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

func TestInflightLeaseHandoff(t *testing.T) {
	const requestID = "lease-handoff-1"
	t.Cleanup(func() { releaseInflightLease(requestID) })

	received := time.Now().Add(-time.Second)
	requestStream := &RequestContext{
		IncomingModelName:        "client-model",
		TargetModelName:          "backend-model",
		Priority:                 3,
		RequestReceivedTimestamp: received,
		requestRunning:           true,
		Request: &Request{Headers: map[string]string{
			reqcommon.RequestIDHeaderKey: requestID,
			":path":                      "/v1/chat/completions",
		}},
		SchedulingRequest: &fwksched.InferenceRequest{FairnessID: "user-a"},
	}
	publishInflightLease(requestStream)

	require.True(t, leaveInflightForObserver(requestStream))
	if _, ok := inflightLeases.Load(requestID); !ok {
		t.Fatal("lease missing after publish")
	}

	observer := &RequestContext{Request: &Request{Headers: map[string]string{}}}
	require.True(t, adoptInflightLease(observer, requestID))
	require.True(t, observer.requestRunning)
	require.True(t, observer.responseObserver)
	require.Equal(t, "client-model", observer.IncomingModelName)
	require.Equal(t, "backend-model", observer.TargetModelName)
	require.Equal(t, 3, observer.Priority)
	require.Equal(t, "user-a", observer.SchedulingRequest.FairnessID)
	require.Equal(t, "/v1/chat/completions", observer.Request.Headers[":path"])
	require.Equal(t, received, observer.RequestReceivedTimestamp)
	require.False(t, leaveInflightForObserver(observer))

	// Adopting does not take a second increment, and a second adopt is a no-op.
	require.True(t, adoptInflightLease(observer, requestID))
}

func TestRequestIDFromProcessingRequest(t *testing.T) {
	st, err := structpb.NewStruct(map[string]any{"request.id": "abc-123"})
	require.NoError(t, err)
	req := &extProcPb.ProcessingRequest{
		Attributes: map[string]*structpb.Struct{
			extProcAttributeNamespace: st,
		},
		ObservabilityMode: true,
	}
	require.Equal(t, "abc-123", requestIDFromProcessingRequest(req))
}

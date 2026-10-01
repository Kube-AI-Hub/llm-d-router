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
	"sync"
	"time"

	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"google.golang.org/protobuf/types/known/structpb"

	reqcommon "github.com/llm-d/llm-d-router/pkg/common/request"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

// extProcAttributeNamespace is the ProcessingRequest.attributes key Envoy uses
// for ext_proc CEL attributes. See Filter::addAttributes in envoy 1.35.
const extProcAttributeNamespace = "envoy.filters.http.ext_proc"

// inflightLease is the running-request gauge ownership for one HTTP request.
// The synchronous request ext_proc stream increments the gauge and stores the
// lease. That stream ends before the model server generates, so it must not
// decrement. The observability response stream adopts the lease and decrements
// when the response finishes.
type inflightLease struct {
	incomingModel string
	targetModel   string
	fairnessID    string
	priority      int
	headers       map[string]string
	receivedAt    time.Time
}

var inflightLeases sync.Map // request ID -> *inflightLease

func publishInflightLease(reqCtx *RequestContext) {
	if reqCtx == nil || reqCtx.Request == nil {
		return
	}
	requestID := reqCtx.Request.Headers[reqcommon.RequestIDHeaderKey]
	if requestID == "" {
		return
	}
	fairnessID, _ := extractFairnessAndPriority(reqCtx)
	inflightLeases.Store(requestID, &inflightLease{
		incomingModel: reqCtx.IncomingModelName,
		targetModel:   reqCtx.TargetModelName,
		fairnessID:    fairnessID,
		priority:      reqCtx.Priority,
		headers:       copyHeaders(reqCtx.Request.Headers),
		receivedAt:    reqCtx.RequestReceivedTimestamp,
	})
	reqCtx.inflightLeaseID = requestID
}

// leaveInflightForObserver reports whether this stream incremented the gauge
// and is exiting before any response, so the response observer must decrement.
func leaveInflightForObserver(reqCtx *RequestContext) bool {
	return reqCtx != nil && reqCtx.requestRunning && reqCtx.inflightLeaseID != "" && !reqCtx.sawResponse
}

// adoptInflightLease makes the response stream the owner of an existing gauge
// increment. It does not increment again.
func adoptInflightLease(reqCtx *RequestContext, requestID string) bool {
	if reqCtx == nil || requestID == "" || reqCtx.requestRunning {
		return reqCtx != nil && reqCtx.requestRunning
	}
	raw, ok := inflightLeases.Load(requestID)
	if !ok {
		return false
	}
	lease := raw.(*inflightLease)
	reqCtx.IncomingModelName = lease.incomingModel
	reqCtx.TargetModelName = lease.targetModel
	reqCtx.Priority = lease.priority
	if reqCtx.RequestReceivedTimestamp.IsZero() {
		reqCtx.RequestReceivedTimestamp = lease.receivedAt
	}
	if reqCtx.SchedulingRequest == nil {
		reqCtx.SchedulingRequest = &fwksched.InferenceRequest{}
	}
	reqCtx.SchedulingRequest.FairnessID = lease.fairnessID
	reqCtx.SchedulingRequest.Headers = lease.headers
	if reqCtx.Request == nil {
		reqCtx.Request = &Request{Headers: map[string]string{}}
	}
	if reqCtx.Request.Headers == nil {
		reqCtx.Request.Headers = map[string]string{}
	}
	for k, v := range lease.headers {
		if _, exists := reqCtx.Request.Headers[k]; !exists {
			reqCtx.Request.Headers[k] = v
		}
	}
	reqCtx.Request.Headers[reqcommon.RequestIDHeaderKey] = requestID
	reqCtx.inflightLeaseID = requestID
	reqCtx.requestRunning = true
	reqCtx.sawResponse = true
	reqCtx.responseObserver = true
	return true
}

func releaseInflightLease(requestID string) {
	if requestID == "" {
		return
	}
	inflightLeases.Delete(requestID)
}

func copyHeaders(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// requestIDFromProcessingRequest reads request.id from the ext_proc attribute
// map Envoy attaches to response headers when response_attributes includes it.
func requestIDFromProcessingRequest(req *extProcPb.ProcessingRequest) string {
	if req == nil {
		return ""
	}
	attrs := req.GetAttributes()
	if len(attrs) == 0 {
		return ""
	}
	if id := requestIDFromAttributeStruct(attrs[extProcAttributeNamespace]); id != "" {
		return id
	}
	for _, st := range attrs {
		if id := requestIDFromAttributeStruct(st); id != "" {
			return id
		}
	}
	return ""
}

func requestIDFromAttributeStruct(st *structpb.Struct) string {
	if st == nil || st.Fields == nil {
		return ""
	}
	v := st.Fields["request.id"]
	if v == nil {
		return ""
	}
	return v.GetStringValue()
}

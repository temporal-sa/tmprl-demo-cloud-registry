package projectbootstrap

import (
	"context"
	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	"google.golang.org/grpc/credentials"
	"testing"
	"time"
)

func TestCredentialMethodPhaseAndDeadline(t *testing.T) {
	now := time.Now()
	c := &attemptCredentials{session: newSession(syntheticToken, now.Add(time.Hour)), now: func() time.Time { return now }, deadline: now.Add(time.Minute)}
	for _, method := range []string{cloudservice.CloudService_CreateProject_FullMethodName, cloudservice.CloudService_DeleteProject_FullMethodName, cloudservice.CloudService_GetUsers_FullMethodName} {
		ctx := credentials.NewContextWithRequestInfo(context.Background(), credentials.RequestInfo{Method: method})
		if _, err := c.GetAPIKey(ctx); err == nil {
			t.Fatal("unarmed/forbidden method exposed OAuth")
		}
	}
	c.armed.Store(true)
	ctx := credentials.NewContextWithRequestInfo(context.Background(), credentials.RequestInfo{Method: cloudservice.CloudService_GetNamespace_FullMethodName})
	if _, err := c.GetAPIKey(ctx); err == nil {
		t.Fatal("read phase reopened after dispatch")
	}
	c.session.private.expiry = now.Add(29 * time.Second)
	ctx = credentials.NewContextWithRequestInfo(context.Background(), credentials.RequestInfo{Method: cloudservice.CloudService_CreateProject_FullMethodName})
	if _, err := c.GetAPIKey(ctx); err == nil || c.used.Load() {
		t.Fatal("expired credential dispatched")
	}
	c.session.private.expiry = now.Add(time.Hour)
	c.deadline = now
	if _, err := c.GetAPIKey(ctx); err == nil || c.used.Load() {
		t.Fatal("approval deadline allowed credential emission")
	}
	c.deadline = now.Add(time.Hour)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := c.GetAPIKey(canceled); err == nil || c.used.Load() {
		t.Fatal("canceled dispatch allowed credential emission")
	}
}

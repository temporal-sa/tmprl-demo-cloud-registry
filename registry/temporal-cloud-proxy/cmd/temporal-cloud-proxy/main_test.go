package main

import (
	"testing"

	"go.temporal.io/api/workflowservice/v1"
)

func TestRewriteNamespaceReplacesDefaultAlias(t *testing.T) {
	req := &workflowservice.StartWorkflowExecutionRequest{Namespace: "default"}

	err := rewriteNamespace(req, "tmprl-dem-cld-demo.a2dd6", parseAliases("default"))

	if err != nil {
		t.Fatalf("rewriteNamespace returned error: %v", err)
	}
	if req.Namespace != "tmprl-dem-cld-demo.a2dd6" {
		t.Fatalf("namespace = %q, want target namespace", req.Namespace)
	}
}

func TestRewriteNamespaceRejectsUnexpectedNamespace(t *testing.T) {
	req := &workflowservice.StartWorkflowExecutionRequest{Namespace: "other"}

	err := rewriteNamespace(req, "tmprl-dem-cld-demo.a2dd6", parseAliases("default"))

	if err == nil {
		t.Fatal("rewriteNamespace returned nil error for unexpected namespace")
	}
}

func TestRewriteNamespaceIgnoresRequestsWithoutNamespace(t *testing.T) {
	req := &workflowservice.GetSystemInfoRequest{}

	err := rewriteNamespace(req, "tmprl-dem-cld-demo.a2dd6", parseAliases("default"))

	if err != nil {
		t.Fatalf("rewriteNamespace returned error: %v", err)
	}
}

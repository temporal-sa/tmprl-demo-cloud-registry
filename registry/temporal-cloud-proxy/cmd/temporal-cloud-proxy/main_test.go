package main

import (
	"testing"

	commandpb "go.temporal.io/api/command/v1"
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

// A RespondWorkflowTaskCompleted that starts a child workflow embeds the
// namespace inside the command attributes. If only the top-level namespace is
// rewritten, Temporal Cloud rejects the completion ("Namespace default is not
// found") and the workflow task times out forever — so the embedded namespace
// must be rewritten too. This is the exact failure that stopped the agentic
// repair child workflow from starting.
func TestRewriteNamespaceRewritesEmbeddedCommandNamespaces(t *testing.T) {
	target := "tmprl-dem-cld-demo.a2dd6"
	req := &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: "default",
		Commands: []*commandpb.Command{
			{
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
					StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
						Namespace:  "default",
						WorkflowId: "repair-ord-1",
					},
				},
			},
			{
				Attributes: &commandpb.Command_SignalExternalWorkflowExecutionCommandAttributes{
					SignalExternalWorkflowExecutionCommandAttributes: &commandpb.SignalExternalWorkflowExecutionCommandAttributes{
						Namespace: "default",
					},
				},
			},
		},
	}

	if err := rewriteNamespace(req, target, parseAliases("default")); err != nil {
		t.Fatalf("rewriteNamespace returned error: %v", err)
	}

	if req.Namespace != target {
		t.Fatalf("top-level namespace = %q, want %q", req.Namespace, target)
	}
	if got := req.Commands[0].GetStartChildWorkflowExecutionCommandAttributes().Namespace; got != target {
		t.Fatalf("child-workflow command namespace = %q, want %q", got, target)
	}
	if got := req.Commands[1].GetSignalExternalWorkflowExecutionCommandAttributes().Namespace; got != target {
		t.Fatalf("signal-external command namespace = %q, want %q", got, target)
	}
}

// An empty embedded namespace already means "the enclosing workflow's
// namespace", so it must be left untouched rather than forced to the target.
func TestRewriteNamespaceLeavesEmptyEmbeddedNamespace(t *testing.T) {
	target := "tmprl-dem-cld-demo.a2dd6"
	req := &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: "default",
		Commands: []*commandpb.Command{
			{
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
					StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
						WorkflowId: "child-1",
					},
				},
			},
		},
	}

	if err := rewriteNamespace(req, target, parseAliases("default")); err != nil {
		t.Fatalf("rewriteNamespace returned error: %v", err)
	}
	if got := req.Commands[0].GetStartChildWorkflowExecutionCommandAttributes().Namespace; got != "" {
		t.Fatalf("empty embedded namespace = %q, want empty", got)
	}
}

// A disallowed namespace embedded in a command is rejected just like a
// disallowed top-level namespace.
func TestRewriteNamespaceRejectsUnexpectedEmbeddedNamespace(t *testing.T) {
	req := &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: "default",
		Commands: []*commandpb.Command{
			{
				Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
					StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
						Namespace: "some-other-namespace",
					},
				},
			},
		},
	}

	if err := rewriteNamespace(req, "tmprl-dem-cld-demo.a2dd6", parseAliases("default")); err == nil {
		t.Fatal("rewriteNamespace returned nil error for unexpected embedded namespace")
	}
}

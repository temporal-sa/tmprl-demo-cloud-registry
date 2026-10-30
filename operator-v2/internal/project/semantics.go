package project

import (
	"fmt"
	"sort"
	"strings"
)

type issueSet []Issue

func (s *issueSet) add(path, code, message string) {
	*s = append(*s, Issue{Pointer: path, Code: code, Message: message})
}
func truth(value *bool) bool           { return value != nil && *value }
func explicitlyFalse(value *bool) bool { return value != nil && !*value }
func text[T ~string](value *T, fallback string) string {
	if value == nil {
		return fallback
	}
	return string(*value)
}
func number(value *int, fallback int) int {
	if value == nil {
		return fallback
	}
	return *value
}
func workerEnabled(c Component) bool { return c.Worker != nil && c.Worker.Enabled }
func versioned(c Component) bool {
	return workerEnabled(c) && c.Worker.Versioning != nil && c.Worker.Versioning.Enabled
}
func proxy(c Component) bool     { return c.TemporalProxy != nil && c.TemporalProxy.Enabled }
func lambda(c Component) bool    { return c.Serverless != nil && c.Serverless.Provider == "aws-lambda" }
func mtlsFiles(c Component) bool { return c.TemporalMtls != nil && c.TemporalMtls.CertFiles != nil }
func authMode(d Document) string {
	if d.Spec.Temporal == nil || d.Spec.Temporal.Auth == nil {
		return "apiKey"
	}
	return text(d.Spec.Temporal.Auth.Mode, "apiKey")
}
func componentPath(name string) string { return "/spec/components/" + escapePointer(name) }
func envPath(name string, index int) string {
	return fmt.Sprintf("%s/env/%d/valueFrom/inputRef", componentPath(name), index)
}

// ValidateSemantics applies authoring policy, never cloud authorization. It must
// run before defaulting because explicit fields can be mutually exclusive even
// when their values equal a normal runtime default.
func ValidateSemantics(document Document) []Issue {
	issues := issueSet(ValidateReferences(document))
	validateEnvironment(document, &issues)
	validateRuntime(document, &issues)
	validateCatalogMetadata(document, &issues)
	validateNamespaceUsage(document, &issues)
	sort.Slice(issues, func(i, j int) bool {
		if issues[i].Pointer == issues[j].Pointer {
			return issues[i].Code < issues[j].Code
		}
		return issues[i].Pointer < issues[j].Pointer
	})
	return issues
}

func needsTemporal(c Component) bool {
	if workerEnabled(c) || truth(c.TemporalAccess) || mtlsFiles(c) && !explicitlyFalse(c.TemporalMtls.CertFiles.Enabled) {
		return true
	}
	for _, env := range c.Env {
		if env.ValueFrom != nil && strings.HasPrefix(env.ValueFrom.InputRef, "temporal.") {
			return true
		}
	}
	return false
}

// TaskQueue resolves the same explicit authoring sources used by autoscaling.
// It cannot discover a queue from a running workflow or a provider.
func TaskQueue(d Document, c Component) string { return taskQueue(d.Spec, c) }

// TaskQueue reads only effective configuration, without re-entering authoring validation.
func (d Effective) TaskQueue(c Component) string { return taskQueue(d.Spec, c) }

func taskQueue(spec Spec, c Component) string {
	if c.Autoscaling != nil && strings.TrimSpace(text(c.Autoscaling.TaskQueue, "")) != "" {
		return strings.TrimSpace(*c.Autoscaling.TaskQueue)
	}
	values := map[string]string{}
	for _, env := range c.Env {
		if env.Value != nil && strings.TrimSpace(*env.Value) != "" {
			values[string(env.Name)] = strings.TrimSpace(*env.Value)
		}
	}
	if values["TEMPORAL_TASK_QUEUE"] != "" {
		return values["TEMPORAL_TASK_QUEUE"]
	}
	if spec.Registry != nil && spec.Registry.Crashable != nil {
		crash := spec.Registry.Crashable
		if queue := values[text(crash.TaskQueueEnv, "")]; queue != "" {
			return queue
		}
		return strings.TrimSpace(text(crash.BaseTaskQueue, ""))
	}
	return ""
}

package simulation

import (
	"context"
	"errors"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/engine"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
)

type Output struct {
	LocalSimulation   bool                            `json:"localSimulation"`
	FakeProviders     bool                            `json:"fakeProviders"`
	Scenario          Scenario                        `json:"scenario"`
	Steps             int                             `json:"steps"`
	CandidateRetained bool                            `json:"candidateRetained"`
	Trace             []string                        `json:"trace"`
	Diagnostics       diagnostics.OperationProjection `json:"diagnostics"`
}

// Run is deliberately finite and uses no ambient configuration or credentials.
func Run(ctx context.Context, scenario Scenario) (Output, error) {
	w, err := New(scenario, true)
	if err != nil {
		return Output{}, err
	}
	executor, err := engine.New(w.Context(), w.Writer(), w.Capabilities())
	if err != nil {
		return Output{}, err
	}
	request := Request()
	out := Output{LocalSimulation: true, FakeProviders: true, Scenario: scenario}
	for i := 0; i < 24; i++ {
		result, stepErr := executor.Step(ctx, request)
		out.Steps = i + 1
		out.Diagnostics = result.Projection
		out.Trace = w.Trace()
		out.CandidateRetained = result.Plan.Phase == rollout.Retained && w.CandidateExists()
		if stepErr != nil {
			return out, stepErr
		}
		if result.Terminal {
			return out, nil
		}
		request.Projection = &result.Projection
	}
	return out, errors.New("local simulation exceeded its bounded step limit")
}

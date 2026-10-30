package rollout

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

var (
	ErrInvalidIntent      = errors.New("invalid rollout intent")
	ErrForeignObservation = errors.New("observation belongs to another owner")
	ErrStaleIntent        = errors.New("desired generation is older than observed generation")
	ErrInvalidObservation = errors.New("invalid rollout observation")
	immutableImage        = regexp.MustCompile(`^[a-z0-9][a-z0-9./:_-]*(?:@sha256:[a-f0-9]{64}|:git-[a-f0-9]{7,40})$`)
)

// Compute returns at most one next action so the executor must observe state
// again after a provider operation. It never presumes an action succeeded.
func Compute(desired Desired, observed Observed) (Plan, error) {
	if err := validate(desired, observed); err != nil {
		return Plan{}, err
	}
	plan := Plan{Subject: desired.Subject, ReconcileKey: desired.ReconcileKey, Revision: desired.Revision}
	if observed.Active != nil {
		plan.ActiveRevision = observed.Active.ID
		plan.ActiveRouteConfirmed = observed.Route != nil && observed.Route.Revision == observed.Active.ID && !observed.Route.ConfirmedAt.IsZero()
	}
	settled := observed.SettledFailure != nil && observed.SettledFailure.Revision == desired.Revision && observed.SettledFailure.ReconcileKey == desired.ReconcileKey
	if outcome := observed.SettledFailure; settled && (observed.Candidate == nil || outcome.CandidateRetained) {
		plan.Phase, plan.Code = Failed, outcome.Code
		if outcome.CandidateRetained {
			plan.Phase = Retained
		}
		return plan, nil
	}
	for _, build := range desired.RequiredBuilds {
		if desired.Images[build] == "" {
			plan.MissingBuilds = append(plan.MissingBuilds, build)
		}
	}
	if len(plan.MissingBuilds) > 0 {
		sort.Strings(plan.MissingBuilds)
		plan.Phase, plan.Code = WaitingForImages, "ImagesNotResolved"
		return plan, nil
	}
	if len(observed.ExternalBlockers) > 0 {
		plan.Phase, plan.Code = WaitingForExternal, "ExternalResourcesNotReady"
		plan.Blockers = append([]string(nil), observed.ExternalBlockers...)
		sort.Strings(plan.Blockers)
		return plan, nil
	}
	if !settled && observed.Active != nil && observed.Active.ID == desired.Revision && observed.Active.ReconcileKey == desired.ReconcileKey && plan.ActiveRouteConfirmed {
		plan.Phase = Succeeded
		return plan, nil
	}
	candidate := observed.Candidate
	if candidate != nil && (candidate.ID != desired.Revision || candidate.ReconcileKey != desired.ReconcileKey) {
		return Plan{}, ErrInvalidObservation
	}
	if candidate == nil {
		return withAction(plan, desired, observed, Deploying, EnsureCandidate), nil
	}
	if settled || candidate.FailureCode != "" || (candidate.Smoke != nil && !candidate.Smoke.Passed) {
		plan.Phase, plan.Code = Failed, candidate.FailureCode
		if settled {
			plan.Code = observed.SettledFailure.Code
		}
		if plan.Code == "" {
			plan.Code = candidate.Smoke.FailureCode
		}
		if plan.Code == "" {
			plan.Code = "SmokeCheckFailed"
		}
		if candidate.CleanupFailure {
			plan.Phase = Retained
			return plan, nil
		}
		switch candidate.Evidence {
		case EvidenceCaptured:
			if !settled {
				plan = withAction(plan, desired, observed, Failed, RecordFailureOutcome)
				plan.Next.FailureOutcome = &FailureOutcome{Revision: desired.Revision, ReconcileKey: desired.ReconcileKey, Code: plan.Code}
				return plan, nil
			}
			return withAction(plan, desired, observed, Failed, DeleteCandidate), nil
		case EvidenceFailed:
			plan.Phase = Retained
			return plan, nil
		default:
			return withAction(plan, desired, observed, Failed, CaptureEvidence), nil
		}
	}
	if !candidate.Ready {
		return withAction(plan, desired, observed, WaitingForReadiness, ObserveReadiness), nil
	}
	if desired.RequireSmoke && candidate.Smoke == nil {
		return withAction(plan, desired, observed, CheckingSmoke, CheckSmoke), nil
	}
	return withAction(plan, desired, observed, Promoting, PromoteCandidate), nil
}

func validate(desired Desired, observed Observed) error {
	subject := desired.Subject
	if subject.Environment == "" || subject.Installation == "" || subject.ProjectUID == "" || desired.Generation < 1 || desired.ReconcileKey == "" || desired.Revision == "" {
		return fmt.Errorf("%w: owner, generation, reconcile key and revision are required", ErrInvalidIntent)
	}
	if observed.Subject != subject {
		return ErrForeignObservation
	}
	if observed.Generation > desired.Generation {
		return ErrStaleIntent
	}
	if observed.ObservationToken == "" {
		return fmt.Errorf("%w: adapter observation token is required", ErrInvalidObservation)
	}
	if route := observed.Route; route != nil {
		if route.Subject != subject {
			return ErrForeignObservation
		}
		if route.ResourceID == "" || route.ResourceVersion == "" {
			return fmt.Errorf("%w: route requires canonical identity and conditional version", ErrInvalidObservation)
		}
	}
	seen := map[string]bool{}
	for _, build := range desired.RequiredBuilds {
		if strings.TrimSpace(build) == "" || seen[build] {
			return fmt.Errorf("%w: build names must be nonempty and unique", ErrInvalidIntent)
		}
		seen[build] = true
	}
	builds := make([]string, 0, len(desired.Images))
	for build := range desired.Images {
		builds = append(builds, build)
	}
	sort.Strings(builds)
	for _, build := range builds {
		image := desired.Images[build]
		if image == "" || !immutableImage.MatchString(image) || strings.ContainsAny(image, " \t\r\n") {
			return fmt.Errorf("%w: build %q requires a digest or git commit image reference", ErrInvalidIntent, build)
		}
	}
	for _, revision := range []*Revision{observed.Active, candidateRevision(observed.Candidate)} {
		if revision != nil && (revision.ID == "" || revision.ReconcileKey == "" || revision.ResourceID == "" || revision.ResourceVersion == "") {
			return fmt.Errorf("%w: revisions require intent and canonical resource identity/version", ErrInvalidObservation)
		}
	}
	if candidate := observed.Candidate; candidate != nil {
		switch candidate.Evidence {
		case "", EvidencePending, EvidenceCaptured, EvidenceFailed:
		default:
			return fmt.Errorf("%w: unknown evidence state", ErrInvalidObservation)
		}
	}
	if outcome := observed.SettledFailure; outcome != nil && (outcome.Revision == "" || outcome.ReconcileKey == "" || outcome.Code == "") {
		return fmt.Errorf("%w: settled failure requires revision, reconcile key and code", ErrInvalidObservation)
	}
	return nil
}

func candidateRevision(candidate *Candidate) *Revision {
	if candidate == nil {
		return nil
	}
	return &candidate.Revision
}

func withAction(plan Plan, desired Desired, observed Observed, phase Phase, kind ActionKind) Plan {
	preconditions := Preconditions{ObservationToken: observed.ObservationToken, ActiveRevision: plan.ActiveRevision, RouteMustBeAbsent: observed.Route == nil}
	if observed.Route != nil {
		preconditions.RouteRevision = observed.Route.Revision
		preconditions.RouteResourceID = observed.Route.ResourceID
		preconditions.RouteResourceVersion = observed.Route.ResourceVersion
	}
	if observed.Candidate != nil {
		preconditions.CandidateResourceID = observed.Candidate.ResourceID
		preconditions.CandidateResourceVersion = observed.Candidate.ResourceVersion
	}
	// JSON encodes fields unambiguously; concatenation could collide when IDs
	// contain delimiters. The observed resource version is a precondition, not
	// part of the durable retry identity for this operation.
	identity, _ := json.Marshal(struct {
		Subject      Subject
		ReconcileKey string
		Revision     string
		Action       ActionKind
	}{desired.Subject, desired.ReconcileKey, desired.Revision, kind})
	digest := sha256.Sum256(identity)
	plan.Phase = phase
	plan.Next = &Action{Kind: kind, Revision: desired.Revision, IdempotencyKey: hex.EncodeToString(digest[:]), Preconditions: preconditions}
	return plan
}

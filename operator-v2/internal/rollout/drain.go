package rollout

// ComputeDrain never creates, checks readiness/smoke or changes traffic. The
// executor must independently fence/drain all writes before using these actions.
func ComputeDrain(desired Desired, observed Observed) (Plan, error) {
	// Project desired generation can advance while this old attempt drains.
	// Cleanup still pins its exact revision/key/UID and never deploys/promotes.
	// The executing supersession guard must exclude unresolved old/new writes.
	validation := observed
	if validation.Generation > desired.Generation {
		validation.Generation = desired.Generation
	}
	if err := validate(desired, validation); err != nil {
		return Plan{}, err
	}
	p := Plan{Subject: desired.Subject, ReconcileKey: desired.ReconcileKey, Revision: desired.Revision, Phase: Failed, Code: "RolloutSuperseded"}
	if observed.Active != nil {
		p.ActiveRevision = observed.Active.ID
		p.ActiveRouteConfirmed = observed.Route != nil && observed.Route.Revision == observed.Active.ID && !observed.Route.ConfirmedAt.IsZero()
	}
	c := observed.Candidate
	if c == nil {
		return p, nil
	}
	if c.ID != desired.Revision || c.ReconcileKey != desired.ReconcileKey {
		return Plan{}, ErrInvalidObservation
	}
	// Even an unconfirmed reference can be serving. Never delete a potentially
	// active candidate based on absent/stale planner metadata.
	if observed.Route != nil && observed.Route.Revision == c.ID || observed.Active != nil && observed.Active.ID == c.ID {
		if !p.ActiveRouteConfirmed || observed.Active.ResourceID != c.ResourceID {
			return Plan{}, ErrInvalidObservation
		}
		p.Phase, p.Code = Succeeded, "SupersededActiveRetained"
		return p, nil
	}
	if c.FailureCode != "" {
		p.Code = c.FailureCode
	} else if c.Smoke != nil && !c.Smoke.Passed {
		p.Code = c.Smoke.FailureCode
		if p.Code == "" {
			p.Code = "SmokeCheckFailed"
		}
	}
	settled := observed.SettledFailure != nil && observed.SettledFailure.Revision == desired.Revision && observed.SettledFailure.ReconcileKey == desired.ReconcileKey
	if settled {
		p.Code = observed.SettledFailure.Code
	}
	if c.CleanupFailure || c.Evidence == EvidenceFailed || settled && observed.SettledFailure.CandidateRetained {
		p.Phase = Retained
		return p, nil
	}
	if c.Evidence != EvidenceCaptured {
		return withAction(p, desired, observed, Failed, CaptureEvidence), nil
	}
	if !settled {
		p = withAction(p, desired, observed, Failed, RecordFailureOutcome)
		p.Next.FailureOutcome = &FailureOutcome{Revision: desired.Revision, ReconcileKey: desired.ReconcileKey, Code: p.Code}
		return p, nil
	}
	return withAction(p, desired, observed, Failed, DeleteCandidate), nil
}

package litellm

import (
	"encoding/json"
	"math"
	"testing"
)

func TestBuildRequestBodyIncludesWorkshopIdentityAndOverrides(t *testing.T) {
	t.Setenv("INSTRUQT_LITELLM_TRACK_ID", "track-a")
	t.Setenv("INSTRUQT_PARTICIPANT_ID", "participant-a")
	t.Setenv("INSTRUQT_USER_EMAIL", "learner@example.com")
	t.Setenv("INSTRUQT_TRACK_SLUG", "slug-a")
	t.Setenv("INSTRUQT_SANDBOX_ID", "sandbox-a")
	t.Setenv("INSTRUQT_TRACK_ID", "track-id-a")
	t.Setenv("INSTRUQT_USER_ID", "user-a")

	rpmLimit := 60
	tpmLimit := 10000
	maxParallelRequests := 4
	body, trackID, err := buildRequestBody(Options{
		Duration:            "1d",
		Budget:              5,
		Models:              []string{"anthropic/*", "openai/*"},
		DefaultModel:        "anthropic/claude-sonnet-4-5",
		BudgetDuration:      "1d",
		RPMLimit:            &rpmLimit,
		TPMLimit:            &tpmLimit,
		MaxParallelRequests: &maxParallelRequests,
	})
	if err != nil {
		t.Fatal(err)
	}
	if trackID != "track-a" {
		t.Fatalf("trackID = %q, want %q", trackID, "track-a")
	}

	var got tokenRequest
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != "track-a" {
		t.Errorf("TrackID = %q, want %q", got.TrackID, "track-a")
	}
	if got.ParticipantID != "participant-a" {
		t.Errorf("ParticipantID = %q, want %q", got.ParticipantID, "participant-a")
	}
	if got.ParticipantEmail == nil || *got.ParticipantEmail != "learner@example.com" {
		t.Fatalf("ParticipantEmail = %v, want learner@example.com", got.ParticipantEmail)
	}
	if got.Duration != "1d" {
		t.Errorf("Duration = %q, want %q", got.Duration, "1d")
	}
	if got.MaxBudget != 5.0 {
		t.Fatalf("MaxBudget = %v, want 5.0", got.MaxBudget)
	}
	if len(got.Models) != 2 || got.Models[0] != "anthropic/*" || got.Models[1] != "openai/*" {
		t.Fatalf("Models = %v, want anthropic/* and openai/*", got.Models)
	}
	if got.DefaultModel != "anthropic/claude-sonnet-4-5" {
		t.Errorf("DefaultModel = %q, want anthropic/claude-sonnet-4-5", got.DefaultModel)
	}
	if got.BudgetDuration != "1d" {
		t.Errorf("BudgetDuration = %q, want 1d", got.BudgetDuration)
	}
	if got.RPMLimit == nil || *got.RPMLimit != 60 {
		t.Errorf("RPMLimit = %v, want 60", got.RPMLimit)
	}
	if got.TPMLimit == nil || *got.TPMLimit != 10000 {
		t.Errorf("TPMLimit = %v, want 10000", got.TPMLimit)
	}
	if got.MaxParallelRequests == nil || *got.MaxParallelRequests != 4 {
		t.Errorf("MaxParallelRequests = %v, want 4", got.MaxParallelRequests)
	}
	if got.Metadata["sandbox_id"] == nil || *got.Metadata["sandbox_id"] != "sandbox-a" {
		t.Fatalf("metadata sandbox_id = %v, want sandbox-a", got.Metadata["sandbox_id"])
	}
}

func TestBuildRequestBodyRequiresTrackIdentity(t *testing.T) {
	t.Setenv("INSTRUQT_LITELLM_TRACK_ID", "")
	t.Setenv("INSTRUQT_TRACK_SLUG", "")

	if _, _, err := buildRequestBody(Options{Duration: "1d", Budget: 5}); err == nil {
		t.Fatal("buildRequestBody() error = nil, want missing track identity error")
	}
}

func TestBuildRequestBodyOmitsOptionalPolicy(t *testing.T) {
	t.Setenv("INSTRUQT_LITELLM_TRACK_ID", "track-a")
	t.Setenv("INSTRUQT_PARTICIPANT_ID", "participant-a")

	body, _, err := buildRequestBody(Options{Duration: "2h", Budget: 5})
	if err != nil {
		t.Fatal(err)
	}

	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"models",
		"default_model",
		"budget_duration",
		"rpm_limit",
		"tpm_limit",
		"max_parallel_requests",
	} {
		if _, exists := got[name]; exists {
			t.Errorf("request unexpectedly included %s", name)
		}
	}
}

func TestValidateOptionsRejectsInvalidPolicy(t *testing.T) {
	zero := 0
	tests := []struct {
		name string
		opts Options
	}{
		{name: "missing duration", opts: Options{Budget: 5}},
		{name: "invalid duration", opts: Options{Duration: "2hours", Budget: 5}},
		{name: "zero budget", opts: Options{Duration: "2h"}},
		{name: "infinite budget", opts: Options{Duration: "2h", Budget: math.Inf(1)}},
		{name: "empty models", opts: Options{Duration: "2h", Budget: 5, Models: []string{}}},
		{name: "empty model", opts: Options{Duration: "2h", Budget: 5, Models: []string{""}}},
		{name: "invalid budget duration", opts: Options{Duration: "2h", Budget: 5, BudgetDuration: "monthly"}},
		{name: "zero rpm limit", opts: Options{Duration: "2h", Budget: 5, RPMLimit: &zero}},
		{name: "zero tpm limit", opts: Options{Duration: "2h", Budget: 5, TPMLimit: &zero}},
		{name: "zero parallel limit", opts: Options{Duration: "2h", Budget: 5, MaxParallelRequests: &zero}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateOptions(test.opts); err == nil {
				t.Fatal("validateOptions() error = nil, want invalid policy error")
			}
		})
	}
}

func TestSignMatchesBrokerCanonicalization(t *testing.T) {
	body := []byte(`{"track_id":"track"}`)
	got := sign("secret", "1710000000", "abc123", body)
	want := "sha256=4eed46e6c602335434a6cf63c603246d419d095249586306d2d7e16fea6bf020"
	if got != want {
		t.Fatalf("signature = %q, want %q", got, want)
	}
}

func TestEnvValuesUsesBrokerBundle(t *testing.T) {
	t.Setenv("OPENAI_MODEL", "")

	values, err := envValues(tokenResponse{
		LiteLLMBaseURL: "https://litellm.example.com/",
		APIKey:         "sk-test",
		DefaultModel:   "openai/gpt-4o-mini",
	})
	if err != nil {
		t.Fatal(err)
	}

	if values["OPENAI_API_KEY"] != "sk-test" {
		t.Errorf("OPENAI_API_KEY = %q, want sk-test", values["OPENAI_API_KEY"])
	}
	if values["OPENAI_BASE_URL"] != "https://litellm.example.com" {
		t.Errorf("OPENAI_BASE_URL = %q, want trimmed base URL", values["OPENAI_BASE_URL"])
	}
	if values["OPENAI_MODEL"] != "gpt-4o-mini" {
		t.Errorf("OPENAI_MODEL = %q, want gpt-4o-mini", values["OPENAI_MODEL"])
	}
}

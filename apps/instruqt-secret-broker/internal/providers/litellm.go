package litellm

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"
)

const (
	defaultBrokerURL = "https://litellm-broker-instruqt.tmprl-demo.cloud"
	defaultKeyID     = "v1"
	defaultModel     = "gpt-4o"

	litellmEnvFile     = "/root/.litellm-env"
	etcEnvironment     = "/etc/environment"
	rootBashrc         = "/root/.bashrc"
	maxErrorBodyLength = 4096
)

var durationPattern = regexp.MustCompile(`^[1-9][0-9]*[smhd]$`)

type Options struct {
	Duration            string
	Budget              float64
	Models              []string
	DefaultModel        string
	BudgetDuration      string
	RPMLimit            *int
	TPMLimit            *int
	MaxParallelRequests *int
}

type tokenRequest struct {
	TrackID             string             `json:"track_id"`
	ParticipantID       string             `json:"participant_id"`
	ParticipantEmail    *string            `json:"participant_email"`
	TrackSlug           *string            `json:"track_slug"`
	Metadata            map[string]*string `json:"metadata"`
	Duration            string             `json:"duration"`
	MaxBudget           float64            `json:"max_budget"`
	Models              []string           `json:"models,omitempty"`
	DefaultModel        string             `json:"default_model,omitempty"`
	BudgetDuration      string             `json:"budget_duration,omitempty"`
	RPMLimit            *int               `json:"rpm_limit,omitempty"`
	TPMLimit            *int               `json:"tpm_limit,omitempty"`
	MaxParallelRequests *int               `json:"max_parallel_requests,omitempty"`
}

type tokenResponse struct {
	LiteLLMBaseURL string `json:"litellm_base_url"`
	APIKey         string `json:"api_key"`
	DefaultModel   string `json:"default_model"`
}

func MintAndPersist(opts Options) error {
	if err := validateOptions(opts); err != nil {
		return err
	}

	secret := strings.TrimSpace(os.Getenv("TEMPORAL_LITELLM_BROKER_SECRET"))
	if secret == "" {
		return fmt.Errorf("TEMPORAL_LITELLM_BROKER_SECRET is not set. Configure the Instruqt secret before starting the track")
	}

	requestBody, trackID, err := buildRequestBody(opts)
	if err != nil {
		return err
	}

	response, err := createToken(requestBody, secret)
	if err != nil {
		return err
	}

	values, err := envValues(response)
	if err != nil {
		return err
	}
	for name, value := range values {
		if err := os.Setenv(name, value); err != nil {
			return fmt.Errorf("set %s: %w", name, err)
		}
	}
	if err := writeEnvFiles(values); err != nil {
		return err
	}

	fmt.Printf("LiteLLM learner key minted for track %s.\n", trackID)
	return nil
}

func buildRequestBody(opts Options) ([]byte, string, error) {
	if err := validateOptions(opts); err != nil {
		return nil, "", err
	}

	trackID := envOr("INSTRUQT_LITELLM_TRACK_ID", "")
	if trackID == "" {
		trackID = envOr("INSTRUQT_TRACK_SLUG", "")
	}
	if trackID == "" {
		return nil, "", fmt.Errorf("track identity is not set: set INSTRUQT_LITELLM_TRACK_ID or INSTRUQT_TRACK_SLUG")
	}
	participantID := envOr("INSTRUQT_PARTICIPANT_ID", "")
	if participantID == "" {
		participantID = envOr("INSTRUQT_SANDBOX_ID", "")
	}
	if participantID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, "", fmt.Errorf("determine participant id: %w", err)
		}
		participantID = hostname
	}

	request := tokenRequest{
		TrackID:       trackID,
		ParticipantID: participantID,
		ParticipantEmail: optionalString(firstNonEmpty(
			os.Getenv("INSTRUQT_USER_EMAIL"),
			os.Getenv("INSTRUQT_PARTICIPANT_EMAIL"),
		)),
		TrackSlug: optionalString(os.Getenv("INSTRUQT_TRACK_SLUG")),
		Metadata: map[string]*string{
			"sandbox_id":        optionalString(os.Getenv("INSTRUQT_SANDBOX_ID")),
			"instruqt_track_id": optionalString(os.Getenv("INSTRUQT_TRACK_ID")),
			"track_slug":        optionalString(os.Getenv("INSTRUQT_TRACK_SLUG")),
			"user_id":           optionalString(os.Getenv("INSTRUQT_USER_ID")),
		},
		Duration:            strings.TrimSpace(opts.Duration),
		MaxBudget:           opts.Budget,
		Models:              normalizedModels(opts.Models),
		DefaultModel:        strings.TrimSpace(opts.DefaultModel),
		BudgetDuration:      strings.TrimSpace(opts.BudgetDuration),
		RPMLimit:            opts.RPMLimit,
		TPMLimit:            opts.TPMLimit,
		MaxParallelRequests: opts.MaxParallelRequests,
	}

	body, err := json.Marshal(request)
	if err != nil {
		return nil, "", fmt.Errorf("encode token request: %w", err)
	}
	return body, trackID, nil
}

func validateOptions(opts Options) error {
	duration := strings.TrimSpace(opts.Duration)
	if !durationPattern.MatchString(duration) {
		return fmt.Errorf("duration must be a positive integer followed by s, m, h, or d")
	}
	if opts.Budget <= 0 || math.IsNaN(opts.Budget) || math.IsInf(opts.Budget, 0) {
		return fmt.Errorf("budget must be a positive finite number")
	}
	if opts.Models != nil {
		if len(opts.Models) == 0 {
			return fmt.Errorf("models must be omitted or contain at least one model")
		}
		for _, model := range opts.Models {
			if strings.TrimSpace(model) == "" {
				return fmt.Errorf("models must not contain an empty model")
			}
		}
	}
	if budgetDuration := strings.TrimSpace(opts.BudgetDuration); budgetDuration != "" && !durationPattern.MatchString(budgetDuration) {
		return fmt.Errorf("budget duration must be a positive integer followed by s, m, h, or d")
	}
	if opts.RPMLimit != nil && *opts.RPMLimit <= 0 {
		return fmt.Errorf("rpm limit must be positive")
	}
	if opts.TPMLimit != nil && *opts.TPMLimit <= 0 {
		return fmt.Errorf("tpm limit must be positive")
	}
	if opts.MaxParallelRequests != nil && *opts.MaxParallelRequests <= 0 {
		return fmt.Errorf("max parallel requests must be positive")
	}
	return nil
}

func normalizedModels(models []string) []string {
	if models == nil {
		return nil
	}
	normalized := make([]string, len(models))
	for index, model := range models {
		normalized[index] = strings.TrimSpace(model)
	}
	return normalized
}

func createToken(body []byte, secret string) (tokenResponse, error) {
	timestamp := fmt.Sprintf("%d", time.Now().Unix())
	nonce, err := randomNonce()
	if err != nil {
		return tokenResponse{}, err
	}
	signature := sign(secret, timestamp, nonce, body)

	url := strings.TrimRight(envOr("INSTRUQT_BROKER_URL", defaultBrokerURL), "/") + "/v1/token"
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return tokenResponse{}, fmt.Errorf("create broker request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Instruqt-Key-Id", envOr("INSTRUQT_BROKER_KEY_ID", defaultKeyID))
	req.Header.Set("X-Instruqt-Timestamp", timestamp)
	req.Header.Set("X-Instruqt-Nonce", nonce)
	req.Header.Set("X-Instruqt-Signature", signature)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return tokenResponse{}, fmt.Errorf("post token request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		errorBody, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyLength))
		return tokenResponse{}, fmt.Errorf("broker returned %s: %s", resp.Status, strings.TrimSpace(string(errorBody)))
	}

	var decoded tokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return tokenResponse{}, fmt.Errorf("decode broker response: %w", err)
	}
	return decoded, nil
}

func envValues(response tokenResponse) (map[string]string, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(response.LiteLLMBaseURL), "/")
	if baseURL == "" {
		return nil, fmt.Errorf("broker response did not include litellm_base_url")
	}
	apiKey := strings.TrimSpace(response.APIKey)
	if apiKey == "" {
		return nil, fmt.Errorf("broker response did not include api_key")
	}

	model := strings.TrimSpace(os.Getenv("OPENAI_MODEL"))
	if model == "" {
		model = strings.TrimSpace(response.DefaultModel)
		if afterSlash, ok := strings.CutPrefix(model, "openai/"); ok {
			model = afterSlash
		} else if strings.Contains(model, "/") {
			parts := strings.SplitN(model, "/", 2)
			model = parts[1]
		}
	}
	if model == "" {
		model = defaultModel
	}

	return map[string]string{
		"LITELLM_BASE_URL":               baseURL,
		"OPENAI_BASE_URL":                baseURL,
		"OPENAI_API_KEY":                 apiKey,
		"OPENAI_MODEL":                   model,
		"OPENAI_AGENTS_DISABLE_TRACING":  "1",
		"SPRING_AI_OPENAI_BASE_URL":      baseURL,
		"SPRING_AI_OPENAI_CHAT_BASE_URL": baseURL,
	}, nil
}

func writeEnvFiles(values map[string]string) error {
	envBlock := exportBlock(values)
	if err := os.WriteFile(litellmEnvFile, []byte(envBlock), 0o600); err != nil {
		return fmt.Errorf("write %s: %w", litellmEnvFile, err)
	}
	if err := os.Chmod(litellmEnvFile, 0o600); err != nil {
		return fmt.Errorf("chmod %s: %w", litellmEnvFile, err)
	}
	if err := updateEtcEnvironment(values); err != nil {
		return err
	}
	if err := ensureBashrcSourcesEnvFile(); err != nil {
		return err
	}
	return nil
}

func updateEtcEnvironment(values map[string]string) error {
	existingBytes, err := os.ReadFile(etcEnvironment)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("read %s: %w", etcEnvironment, err)
	}

	lines := strings.Split(strings.ReplaceAll(string(existingBytes), "\r\n", "\n"), "\n")
	filtered := make([]string, 0, len(lines)+len(envOrder))
	for _, line := range lines {
		if line == "" || hasEnvPrefix(line) {
			continue
		}
		filtered = append(filtered, line)
	}
	for _, name := range envOrder {
		filtered = append(filtered, fmt.Sprintf("%s=%s", name, values[name]))
	}

	if err := os.WriteFile(etcEnvironment, []byte(strings.Join(filtered, "\n")+"\n"), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", etcEnvironment, err)
	}
	return nil
}

func ensureBashrcSourcesEnvFile() error {
	existing, err := os.ReadFile(rootBashrc)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("read %s: %w", rootBashrc, err)
	}
	if strings.Contains(string(existing), litellmEnvFile) {
		return nil
	}

	block := fmt.Sprintf(`

if [ -f %s ]; then
    . %s
fi
`, litellmEnvFile, litellmEnvFile)

	file, err := os.OpenFile(rootBashrc, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open %s: %w", rootBashrc, err)
	}
	defer file.Close()
	if _, err := file.WriteString(block); err != nil {
		return fmt.Errorf("append %s: %w", rootBashrc, err)
	}
	return nil
}

var envOrder = []string{
	"LITELLM_BASE_URL",
	"OPENAI_BASE_URL",
	"OPENAI_API_KEY",
	"OPENAI_MODEL",
	"OPENAI_AGENTS_DISABLE_TRACING",
	"SPRING_AI_OPENAI_BASE_URL",
	"SPRING_AI_OPENAI_CHAT_BASE_URL",
}

func exportBlock(values map[string]string) string {
	var builder strings.Builder
	for _, name := range envOrder {
		builder.WriteString("export ")
		builder.WriteString(name)
		builder.WriteByte('=')
		builder.WriteString(shellQuote(values[name]))
		builder.WriteByte('\n')
	}
	return builder.String()
}

func hasEnvPrefix(line string) bool {
	for _, name := range envOrder {
		if strings.HasPrefix(line, name+"=") {
			return true
		}
	}
	return false
}

func shellQuote(value string) string {
	if value == "" {
		return "''"
	}
	return "'" + strings.ReplaceAll(value, "'", `'"'"'`) + "'"
}

func sign(secret, timestamp, nonce string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp))
	mac.Write([]byte("."))
	mac.Write([]byte(nonce))
	mac.Write([]byte("."))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func randomNonce() (string, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", fmt.Errorf("generate nonce: %w", err)
	}
	return hex.EncodeToString(nonce[:]), nil
}

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func optionalString(value string) *string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return &value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

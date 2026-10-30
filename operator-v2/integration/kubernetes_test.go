//go:build integration

package integration

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
)

type object = map[string]any
type connection struct {
	Server              string `json:"server"`
	NodeIP              string `json:"nodeIp"`
	CA                  string `json:"ca"`
	Certificate         string `json:"certificate"`
	Key                 string `json:"key"`
	RunID               string `json:"runId"`
	NamespaceUID        string `json:"namespaceUid"`
	Image               string `json:"image"`
	ImageDigest         string `json:"imageDigest"`
	ImageManifestDigest string `json:"imageManifestDigest"`
	ImageConfigDigest   string `json:"imageConfigDigest"`
	HostGateway         string `json:"hostGateway"`
}

var local connection
var client *http.Client
var boundaryVerified bool

func TestMain(m *testing.M) {
	file, err := os.Open(os.Getenv("OPERATOR_LOCAL_KUBE_CONNECTION"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "Use scripts/run_operator_kube_tests.py; explicit local connection required")
		os.Exit(2)
	}
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&local); err != nil {
		panic(err)
	}
	_ = file.Close()
	if local.RunID != os.Getenv("OPERATOR_LOCAL_CLUSTER_RUN") || !strings.HasPrefix(local.RunID, "operator-local-") || net.ParseIP(local.NodeIP) == nil || local.Server != "https://"+local.NodeIP+":6443" || local.NamespaceUID == "" {
		panic("unproven local cluster connection")
	}
	digest := regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
	if local.Image != "operator-local-fixture:git-"+strings.TrimPrefix(local.RunID, "operator-local-") || !digest.MatchString(local.ImageManifestDigest) || !digest.MatchString(local.ImageConfigDigest) || local.ImageManifestDigest == local.ImageConfigDigest || local.ImageDigest != "docker.io/library/operator-local-fixture@"+local.ImageManifestDigest {
		panic("unproven immutable fixture manifest/config binding")
	}
	decode := func(value string) []byte {
		result, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			panic(err)
		}
		return result
	}
	cert, err := tls.X509KeyPair(decode(local.Certificate), decode(local.Key))
	if err != nil {
		panic(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(decode(local.CA)) {
		panic("invalid local CA")
	}
	client = &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, Certificates: []tls.Certificate{cert}}}}
	os.Exit(m.Run())
}

func request(t *testing.T, method, path string, value any, expected int) object {
	t.Helper()
	if method != "GET" && !boundaryVerified {
		t.Fatal("refusing API mutation before owned-cluster/network proof")
	}
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(method, local.Server+path, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != expected {
		t.Fatalf("%s %s: expected %d, got %d: %s", method, path, expected, response.StatusCode, body)
	}
	var result object
	if len(body) > 0 {
		if err := json.Unmarshal(body, &result); err != nil {
			t.Fatal(err)
		}
	}
	return result
}
func field(value object, key string) object { result, _ := value[key].(map[string]any); return result }
func text(value object, key string) string  { result, _ := value[key].(string); return result }
func metadata(value object) object          { return field(value, "metadata") }
func copyObject(value object) object {
	data, _ := json.Marshal(value)
	var result object
	_ = json.Unmarshal(data, &result)
	return result
}
func wait(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("local controller condition timed out")
		}
		time.Sleep(200 * time.Millisecond)
	}
}
func namespace(t *testing.T, suffix string) string {
	t.Helper()
	name := local.RunID + "-" + suffix
	request(t, "POST", "/api/v1/namespaces", object{"apiVersion": "v1", "kind": "Namespace", "metadata": object{"name": name, "labels": object{"operator.local/run-id": local.RunID}}}, 201)
	return name
}

func TestOwnedAPIAndNetworkBoundary(t *testing.T) {
	ns := request(t, "GET", "/api/v1/namespaces/kube-system", nil, 200)
	if text(metadata(ns), "uid") != local.NamespaceUID {
		t.Fatal("API namespace identity differs from owned node provenance")
	}
	proof := request(t, "GET", "/api/v1/namespaces/kube-system/configmaps/operator-local-proof", nil, 200)
	if text(field(proof, "data"), "run") != local.RunID {
		t.Fatal("owned API run proof does not match")
	}
	routes, err := os.ReadFile("/proc/net/route")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(routes), "\n") {
		columns := strings.Fields(line)
		if len(columns) > 1 && columns[1] == "00000000" {
			t.Fatal("test container has a default network route")
		}
	}
	for _, address := range []string{net.JoinHostPort(local.HostGateway, "9"), "192.0.2.1:443", "[2001:db8::1]:443"} {
		connection, err := net.DialTimeout("tcp", address, time.Second)
		if err == nil {
			_ = connection.Close()
			t.Fatalf("unapproved destination reachable: %s", address)
		}
		if !errors.Is(err, syscall.ENETUNREACH) && !errors.Is(err, syscall.EHOSTUNREACH) && !errors.Is(err, syscall.EAFNOSUPPORT) {
			t.Fatalf("kernel route denial not proven for %s: %v", address, err)
		}
	}
	for _, path := range []string{"/var/run/docker.sock", "/root/.aws/credentials", "/root/.kube/config"} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("unexpected host credential/control path: %s", path)
		}
	}
	boundaryVerified = true
}

func ensureCRD(t *testing.T) {
	t.Helper()
	definition := object{"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition", "metadata": object{"name": "rollouts.operator-test.local"}, "spec": object{"group": "operator-test.local", "scope": "Namespaced", "names": object{"plural": "rollouts", "singular": "rollout", "kind": "LocalRollout"}, "versions": []any{object{"name": "v1", "served": true, "storage": true, "subresources": object{"status": object{}}, "schema": object{"openAPIV3Schema": object{"type": "object", "properties": object{"spec": object{"type": "object", "required": []string{"revision"}, "properties": object{"revision": object{"type": "string", "minLength": 1}, "replicas": object{"type": "integer", "minimum": 0}}}, "status": object{"type": "object", "x-kubernetes-preserve-unknown-fields": true}}}}}}}}
	// These tests run serially in a fresh owned cluster.
	check, err := client.Get(local.Server + "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/rollouts.operator-test.local")
	if err != nil {
		t.Fatal(err)
	}
	_ = check.Body.Close()
	if check.StatusCode == 404 {
		request(t, "POST", "/apis/apiextensions.k8s.io/v1/customresourcedefinitions", definition, 201)
	} else if check.StatusCode != 200 {
		t.Fatal(check.StatusCode)
	}
	wait(t, func() bool {
		result := request(t, "GET", "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/rollouts.operator-test.local", nil, 200)
		conditions, _ := field(result, "status")["conditions"].([]any)
		for _, item := range conditions {
			condition := item.(map[string]any)
			if text(condition, "type") == "Established" && text(condition, "status") == "True" {
				return true
			}
		}
		return false
	})
}
func crPath(ns string) string { return "/apis/operator-test.local/v1/namespaces/" + ns + "/rollouts" }
func newCR(ns, name, revision string) object {
	return object{"apiVersion": "operator-test.local/v1", "kind": "LocalRollout", "metadata": object{"name": name, "namespace": ns, "finalizers": []string{"operator-test.local/cleanup"}}, "spec": object{"revision": revision, "replicas": 1}, "status": object{"phase": "MustBeIgnored"}}
}

func TestSchemaStatusGenerationAndFinalizerSemantics(t *testing.T) {
	ensureCRD(t)
	ns := namespace(t, "api")
	path := crPath(ns)
	invalid := newCR(ns, "invalid", "one")
	field(invalid, "spec")["replicas"] = -1
	request(t, "POST", path, invalid, 422)
	created := request(t, "POST", path, newCR(ns, "contract", "one"), 201)
	path += "/contract"
	if len(field(created, "status")) != 0 {
		t.Fatal("normal create accepted status-subresource content")
	}
	generation := metadata(created)["generation"]
	status := copyObject(created)
	status["status"] = object{"phase": "Ready"}
	written := request(t, "PUT", path+"/status", status, 200)
	if metadata(written)["generation"] != generation {
		t.Fatal("status update changed desired generation")
	}
	request(t, "PUT", path, created, 409)
	update := copyObject(written)
	field(update, "spec")["revision"] = "two"
	update["status"] = object{"phase": "MustBeIgnored"}
	updated := request(t, "PUT", path, update, 200)
	if metadata(updated)["generation"] == generation || text(field(updated, "status"), "phase") != "Ready" {
		t.Fatal("spec/status subresource separation is incorrect")
	}
	request(t, "DELETE", path, object{"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": object{"uid": text(metadata(updated), "uid")}}, 200)
	deleting := request(t, "GET", path, nil, 200)
	if text(metadata(deleting), "deletionTimestamp") == "" {
		t.Fatal("finalizer did not hold deletion")
	}
	metadata(deleting)["finalizers"] = []string{}
	request(t, "PUT", path, deleting, 200)
	wait(t, func() bool {
		response, err := client.Get(local.Server + path)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		return response.StatusCode == 404
	})
}

func deployment(t *testing.T, ns, revision string, ready bool) object {
	labels := object{"app": "operator-fixture", "revision": revision}
	token := false
	return request(t, "POST", "/apis/apps/v1/namespaces/"+ns+"/deployments", object{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": object{"name": revision, "namespace": ns}, "spec": object{"replicas": 1, "progressDeadlineSeconds": 10, "selector": object{"matchLabels": labels}, "template": object{"metadata": object{"labels": labels}, "spec": object{"automountServiceAccountToken": token, "securityContext": object{"runAsNonRoot": true, "runAsUser": 65532}, "containers": []any{object{"name": "web", "image": local.Image, "imagePullPolicy": "Never", "args": []string{"--revision=" + revision, fmt.Sprint("--ready=", ready)}, "securityContext": object{"allowPrivilegeEscalation": false, "capabilities": object{"drop": []string{"ALL"}}}, "readinessProbe": object{"httpGet": object{"path": "/ready", "port": 8080}, "periodSeconds": 1, "failureThreshold": 1}}}}}}}, 201)
}
func waitDeployment(t *testing.T, ns, revision string, ready bool) object {
	var result object
	wait(t, func() bool {
		result = request(t, "GET", "/apis/apps/v1/namespaces/"+ns+"/deployments/"+revision, nil, 200)
		status := field(result, "status")
		if ready {
			available, _ := status["availableReplicas"].(float64)
			return available >= 1
		}
		conditions, _ := status["conditions"].([]any)
		for _, item := range conditions {
			condition := item.(map[string]any)
			if text(condition, "reason") == "ProgressDeadlineExceeded" {
				return true
			}
		}
		return false
	})
	return result
}
func service(t *testing.T, ns, name, revision string) object {
	return request(t, "POST", "/api/v1/namespaces/"+ns+"/services", object{"apiVersion": "v1", "kind": "Service", "metadata": object{"name": name, "namespace": ns}, "spec": object{"selector": object{"app": "operator-fixture", "revision": revision}, "ports": []any{object{"port": 8080, "targetPort": 8080}}}}, 201)
}

var probeIndex int

func probe(t *testing.T, ns, serviceName, expected string) {
	t.Helper()
	probeIndex++
	name := fmt.Sprintf("probe-%d", probeIndex)
	request(t, "POST", "/api/v1/namespaces/"+ns+"/pods", object{"apiVersion": "v1", "kind": "Pod", "metadata": object{"name": name}, "spec": object{"restartPolicy": "Never", "automountServiceAccountToken": false, "containers": []any{object{"name": "probe", "image": local.Image, "imagePullPolicy": "Never", "args": []string{"--probe=http://" + serviceName + "." + ns + ".svc.cluster.local:8080/", "--expect=" + expected}}}}}, 201)
	wait(t, func() bool {
		pod := request(t, "GET", "/api/v1/namespaces/"+ns+"/pods/"+name, nil, 200)
		phase := text(field(pod, "status"), "phase")
		if phase == "Failed" {
			response, err := client.Get(local.Server + "/api/v1/namespaces/" + ns + "/pods/" + name + "/log")
			if err != nil {
				t.Fatal("Service probe failed; logs unavailable:", err)
			}
			defer response.Body.Close()
			logs, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
			t.Fatalf("in-cluster Service probe failed: %s", logs)
		}
		return phase == "Succeeded"
	})
	response, err := client.Get(local.Server + "/api/v1/namespaces/" + ns + "/pods/" + name + "/log")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(response.Body)
	if string(data) != expected {
		t.Fatalf("Service %s routed to %q, expected %q", serviceName, data, expected)
	}
}
func next(t *testing.T, desired rollout.Desired, observed rollout.Observed, expected rollout.ActionKind) rollout.Plan {
	t.Helper()
	plan, err := rollout.Compute(desired, observed)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Next == nil || plan.Next.Kind != expected {
		t.Fatalf("expected %s, got %+v", expected, plan)
	}
	return plan
}

func TestFailedCandidateRetentionAndPromotionCAS(t *testing.T) {
	ensureCRD(t)
	ns := namespace(t, "rollout")
	project := request(t, "POST", crPath(ns), newCR(ns, "project", "bad"), 201)
	deployment(t, ns, "active", true)
	active := waitDeployment(t, ns, "active", true)
	route := service(t, ns, "stable", "active")
	probe(t, ns, "stable", "active")
	subject := rollout.Subject{Environment: "local", Installation: local.RunID, ProjectUID: text(metadata(project), "uid")}
	observed := rollout.Observed{Subject: subject, Generation: 1, ObservationToken: "api-observation", Active: &rollout.Revision{ID: "active", ReconcileKey: "intent-active", ResourceID: text(metadata(active), "uid"), ResourceVersion: text(metadata(active), "resourceVersion")}, Route: &rollout.Route{Subject: subject, ResourceID: text(metadata(route), "uid"), ResourceVersion: text(metadata(route), "resourceVersion"), Revision: "active", ConfirmedAt: time.Now()}}
	desired := rollout.Desired{Subject: subject, Generation: 1, Revision: "bad", ReconcileKey: "intent-bad", RequiredBuilds: []string{"web"}, Images: map[string]string{"web": local.Image}, RequireSmoke: true}
	next(t, desired, observed, rollout.EnsureCandidate)
	deployment(t, ns, "bad", false)
	bad := waitDeployment(t, ns, "bad", false)
	observed.Candidate = &rollout.Candidate{Revision: rollout.Revision{ID: "bad", ReconcileKey: "intent-bad", ResourceID: text(metadata(bad), "uid"), ResourceVersion: text(metadata(bad), "resourceVersion")}, FailureCode: "CandidateNotReady"}
	next(t, desired, observed, rollout.CaptureEvidence)
	// Persist real controller/pod evidence before recording the terminal latch.
	pods := request(t, "GET", "/api/v1/namespaces/"+ns+"/pods?labelSelector=revision%3Dbad", nil, 200)
	project = request(t, "GET", crPath(ns)+"/project", nil, 200)
	project["status"] = object{"evidence": object{"deployment": field(bad, "status"), "pods": pods["items"]}}
	project = request(t, "PUT", crPath(ns)+"/project/status", project, 200)
	observed.Candidate.Evidence = rollout.EvidenceCaptured
	plan := next(t, desired, observed, rollout.RecordFailureOutcome)
	stale := copyObject(project)
	field(stale, "status")["failureOutcome"] = plan.Next.FailureOutcome
	// Simulate a concurrent status writer; a rejected latch cannot authorize delete.
	field(project, "status")["publisher"] = "concurrent"
	project = request(t, "PUT", crPath(ns)+"/project/status", project, 200)
	request(t, "PUT", crPath(ns)+"/project/status", stale, 409)
	next(t, desired, observed, rollout.RecordFailureOutcome)
	request(t, "GET", "/apis/apps/v1/namespaces/"+ns+"/deployments/bad", nil, 200)
	field(project, "status")["failureOutcome"] = plan.Next.FailureOutcome
	request(t, "PUT", crPath(ns)+"/project/status", project, 200)
	persisted := request(t, "GET", crPath(ns)+"/project", nil, 200)
	data, _ := json.Marshal(field(persisted, "status")["failureOutcome"])
	var outcome rollout.FailureOutcome
	_ = json.Unmarshal(data, &outcome)
	observed.SettledFailure = &outcome
	plan = next(t, desired, observed, rollout.DeleteCandidate)
	request(t, "DELETE", "/apis/apps/v1/namespaces/"+ns+"/deployments/bad", object{"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": object{"uid": plan.Next.Preconditions.CandidateResourceID, "resourceVersion": plan.Next.Preconditions.CandidateResourceVersion}}, 200)
	observed.Candidate = nil
	settled, err := rollout.Compute(desired, observed)
	if err != nil || settled.Next != nil || settled.Phase != rollout.Failed {
		t.Fatalf("failed intent recreated after cleanup: %+v %v", settled, err)
	}
	probe(t, ns, "stable", "active")
	deployment(t, ns, "good", true)
	good := waitDeployment(t, ns, "good", true)
	service(t, ns, "candidate", "good")
	probe(t, ns, "candidate", "good")
	desired.Revision, desired.ReconcileKey = "good", "intent-good"
	observed.Candidate = &rollout.Candidate{Revision: rollout.Revision{ID: "good", ReconcileKey: "intent-good", ResourceID: text(metadata(good), "uid"), ResourceVersion: text(metadata(good), "resourceVersion")}, Ready: true, Smoke: &rollout.SmokeResult{Passed: true}}
	plan = next(t, desired, observed, rollout.PromoteCandidate)
	if plan.Next.Preconditions.RouteResourceID != text(metadata(route), "uid") || plan.Next.Preconditions.RouteResourceVersion != text(metadata(route), "resourceVersion") {
		t.Fatal("planner omitted route CAS preconditions")
	}
	routePath := "/api/v1/namespaces/" + ns + "/services/stable"
	staleRoute := copyObject(route)
	metadata(route)["annotations"] = object{"operator.local/concurrent": "true"}
	route = request(t, "PUT", routePath, route, 200)
	field(staleRoute, "spec")["selector"] = object{"app": "operator-fixture", "revision": "good"}
	request(t, "PUT", routePath, staleRoute, 409)
	probe(t, ns, "stable", "active")
	observed.Route.ResourceVersion = text(metadata(route), "resourceVersion")
	next(t, desired, observed, rollout.PromoteCandidate)
	field(route, "spec")["selector"] = object{"app": "operator-fixture", "revision": "good"}
	route = request(t, "PUT", routePath, route, 200)
	probe(t, ns, "stable", "good")
	observed.Active = &observed.Candidate.Revision
	observed.Route.Revision = "good"
	observed.Route.ResourceVersion = text(metadata(route), "resourceVersion")
	success, err := rollout.Compute(desired, observed)
	if err != nil || success.Phase != rollout.Succeeded || success.Next != nil {
		t.Fatalf("successful route did not settle: %+v %v", success, err)
	}
}

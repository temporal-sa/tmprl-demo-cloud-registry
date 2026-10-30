package project

import (
	"fmt"
	"path"
	"regexp"
	"strings"
)

var envNamePattern = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)
var dnsNamePattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
var keyInputs = map[string]bool{"temporal.address": true, "temporal.namespace": true, "temporal.apiKey": true, "temporal.tls": true}
var mtlsInputs = map[string]bool{"temporal.caCert": true, "temporal.clientCert": true, "temporal.clientKey": true}

func validateEnvironment(d Document, issues *issueSet) {
	referenced := map[string]bool{}
	mode := authMode(d)
	for name, c := range d.Spec.Components {
		base := componentPath(name)
		seenSecrets := map[DNSLabel]bool{}
		for index, secret := range c.Secrets {
			referenced[string(secret)] = true
			if seenSecrets[secret] {
				issues.add(fmt.Sprintf("%s/secrets/%d", base, index), "DuplicateSecret", "secret references must be unique")
			}
			seenSecrets[secret] = true
		}
		for index, env := range c.Env {
			if env.ValueFrom == nil {
				continue
			}
			ref := env.ValueFrom.InputRef
			location := envPath(name, index)
			switch {
			case ref == "platform.rolloutVersion":
			case keyInputs[ref]:
				if proxy(c) {
					issues.add(location, "ProxyCredentialConflict", "proxy components cannot receive direct Temporal credentials")
				}
				if mode == "mtls" {
					issues.add(location, "AuthModeConflict", "API-key inputs require apiKey authentication")
				}
			case mtlsInputs[ref]:
				if proxy(c) {
					issues.add(location, "ProxyCredentialConflict", "proxy components cannot receive direct Temporal credentials")
				}
				if mode != "mtls" {
					issues.add(location, "AuthModeConflict", "certificate inputs require mtls authentication")
				}
			default:
				parts := strings.Split(ref, ".")
				if len(parts) != 3 || parts[0] != "secrets" || !dnsNamePattern.MatchString(parts[1]) || !envNamePattern.MatchString(parts[2]) {
					issues.add(location, "InvalidInputReference", "input must reference a supported platform/Temporal value or named secret property")
					continue
				}
				referenced[parts[1]] = true
				if _, ok := d.Spec.Secrets[parts[1]]; !ok {
					issues.add(location, "UnknownSecret", "input references an undeclared project secret")
				}
			}
		}
		if proxy(c) && !needsTemporal(c) {
			issues.add(base+"/temporalProxy", "TemporalAccessRequired", "Temporal proxy requires a worker or explicit Temporal input/access")
		}
		if mode == "mtls" {
			if proxy(c) {
				issues.add(base+"/temporalProxy", "AuthModeConflict", "Temporal proxy does not support mtls authentication")
			}
			if versioned(c) {
				issues.add(base+"/worker/versioning", "AuthModeConflict", "worker versioning requires apiKey authentication")
			}
		}
		if mtlsFiles(c) {
			if mode != "mtls" {
				issues.add(base+"/temporalMtls/certFiles", "AuthModeConflict", "certificate files require mtls authentication")
			}
			if certMountRoot(c) == "/" {
				issues.add(base+"/temporalMtls/certFiles/paths", "UnsafeCertificateMount", "certificate paths must share a non-root directory")
			}
		}
		if truth(c.IsolatedExecutor) {
			for index, env := range c.Env {
				key := string(env.Name)
				if strings.HasPrefix(key, "AWS_") || strings.HasPrefix(key, "TEMPORAL_") || strings.HasSuffix(key, "API_KEY") {
					issues.add(fmt.Sprintf("%s/env/%d/name", base, index), "ExecutorCredentialConflict", "isolated executor cannot receive credential environment variables")
				}
			}
		}
	}
	for name := range d.Spec.Secrets {
		if !referenced[name] {
			issues.add("/spec/secrets/"+escapePointer(name), "UnusedSecret", "project secret must be referenced by at least one component")
		}
	}
}

func certMountRoot(c Component) string {
	files := c.TemporalMtls.CertFiles
	mount := text(files.MountPath, "/var/run/secrets/tmprl-demo.cloud/temporal")
	paths := []string{path.Join(mount, "ca.crt"), path.Join(mount, "tls.crt"), path.Join(mount, "tls.key")}
	if files.Paths != nil {
		for index, value := range []*string{files.Paths.CACert, files.Paths.ClientCert, files.Paths.ClientKey} {
			if value != nil {
				paths[index] = path.Clean(*value)
			}
		}
	}
	common := strings.Split(strings.Trim(paths[0], "/"), "/")
	for _, value := range paths[1:] {
		parts := strings.Split(strings.Trim(value, "/"), "/")
		n := 0
		for n < len(common) && n < len(parts) && common[n] == parts[n] {
			n++
		}
		common = common[:n]
	}
	if len(common) == 0 {
		return "/"
	}
	return "/" + strings.Join(common, "/")
}

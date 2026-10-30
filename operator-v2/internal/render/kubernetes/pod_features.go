package kubernetes

import (
	"path"
	"strings"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func overwriteEnv(values []corev1.EnvVar, value corev1.EnvVar) []corev1.EnvVar {
	for i := range values {
		if values[i].Name == value.Name {
			values[i] = value
			return values
		}
	}
	return append(values, value)
}

func decoratePod(document project.Effective, name string, c project.Component, pod *corev1.PodSpec, b Bindings, requirements map[string]SecretRequirement) error {
	main := &pod.Containers[0]
	if c.Worker != nil && c.Worker.LivenessProbe {
		main.LivenessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"sh", "-c", `test "$(( $(date +%s) - $(stat -c %Y /tmp/worker-alive 2>/dev/null || echo 0) ))" -lt 120`}}}, InitialDelaySeconds: 60, PeriodSeconds: 30, TimeoutSeconds: 5, FailureThreshold: 2}
	}
	label := "primary"
	if c.TemporalNamespace != nil {
		label = string(*c.TemporalNamespace)
	}
	temporal := b.Temporal[label]
	if c.TemporalProxy != nil && c.TemporalProxy.Enabled {
		if b.ProxyImage == "" || !immutableImage.MatchString(b.ProxyImage) {
			return renderError("ProxyImageBindingRequired", "temporalProxy", "A Temporal proxy requires an explicit immutable trusted sidecar image.")
		}
		if temporal.Mode != "apiKey" {
			return renderError("TemporalBindingRequired", "temporalProxy", "Temporal proxy requires an explicit API-key credential binding.")
		}
		for _, input := range []string{"temporal.address", "temporal.namespace", "temporal.apiKey", "temporal.tls"} {
			if temporal.Secret.Keys[input] != StandardTemporalKeys()[input] {
				return renderError("DefaultTemporalSecretShape", "temporalProxy", "Proxy credentials require the standard local Secret key contract.")
			}
			if err := requireSecret(temporal.Secret, temporal.Secret.Keys[input], b, requirements); err != nil {
				return err
			}
		}
		names := []project.EnvName{"TEMPORAL_ADDRESS", "TEMPORAL_HOST"}
		if c.TemporalProxy.AddressEnv != nil {
			names = c.TemporalProxy.AddressEnv
		}
		for _, key := range names {
			main.Env = overwriteEnv(main.Env, corev1.EnvVar{Name: string(key), Value: "127.0.0.1:7233"})
		}
		main.Env = overwriteEnv(main.Env, corev1.EnvVar{Name: "TEMPORAL_NAMESPACE", Value: "default"})
		main.Env = overwriteEnv(main.Env, corev1.EnvVar{Name: "TEMPORAL_TLS", Value: "false"})
		pod.Containers = append(pod.Containers, corev1.Container{Name: "temporal-cloud-proxy", Image: b.ProxyImage, ImagePullPolicy: corev1.PullIfNotPresent, EnvFrom: []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: temporal.Secret.Name}}}}, Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("25m"), corev1.ResourceMemory: resource.MustParse("32Mi")}, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("128Mi")}}})
		// Append may relocate the backing slice.
		main = &pod.Containers[0]
	}
	if c.TemporalMtls != nil && c.TemporalMtls.CertFiles != nil && (c.TemporalMtls.CertFiles.Enabled == nil || *c.TemporalMtls.CertFiles.Enabled) {
		if temporal.Mode != "mtls" {
			return renderError("TemporalBindingRequired", "temporalMtls", "Certificate files require an explicit mTLS credential binding.")
		}
		files := c.TemporalMtls.CertFiles
		mount := "/var/run/secrets/tmprl-demo.cloud/temporal"
		if files.MountPath != nil {
			mount = path.Clean(*files.MountPath)
		}
		paths := []string{path.Join(mount, "ca.crt"), path.Join(mount, "tls.crt"), path.Join(mount, "tls.key")}
		if files.Paths != nil {
			for i, value := range []*string{files.Paths.CACert, files.Paths.ClientCert, files.Paths.ClientKey} {
				if value != nil {
					paths[i] = path.Clean(*value)
				}
			}
		}
		seen := map[string]bool{}
		for _, value := range paths {
			if seen[value] {
				return renderError("DuplicateCertificatePath", "temporalMtls.certFiles", "Each certificate/key projection requires a distinct file path.")
			}
			seen[value] = true
		}
		root := path.Dir(paths[0])
		for _, value := range paths[1:] {
			for root != "/" && !strings.HasPrefix(value, root+"/") {
				root = path.Dir(root)
			}
		}
		if root == "/" {
			return renderError("UnsafeCertificateMount", "temporalMtls.certFiles", "Certificate paths must share a non-root directory.")
		}
		items := []corev1.KeyToPath{}
		for i, input := range []string{"temporal.caCert", "temporal.clientCert", "temporal.clientKey"} {
			key, ok := temporal.Secret.Keys[input]
			if !ok {
				return renderError("SecretPropertyBindingRequired", "temporalMtls.certFiles", "A certificate field lacks a local Secret key binding.")
			}
			if err := requireSecret(temporal.Secret, key, b, requirements); err != nil {
				return err
			}
			items = append(items, corev1.KeyToPath{Key: key, Path: strings.TrimPrefix(paths[i], root+"/")})
		}
		pod.Volumes = append(pod.Volumes, corev1.Volume{Name: "temporal-mtls-certs", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: temporal.Secret.Name, Items: items}}})
		main.VolumeMounts = append(main.VolumeMounts, corev1.VolumeMount{Name: "temporal-mtls-certs", MountPath: root, ReadOnly: true})
		envNames := []string{"TEMPORAL_MTLS_CERT_DIR", "TEMPORAL_MTLS_CA_CERT_PATH", "TEMPORAL_MTLS_CLIENT_CERT_PATH", "TEMPORAL_MTLS_CLIENT_KEY_PATH"}
		if files.PathEnv != nil {
			for i, value := range []*project.EnvName{files.PathEnv.CertDir, files.PathEnv.CACert, files.PathEnv.ClientCert, files.PathEnv.ClientKey} {
				if value != nil {
					envNames[i] = string(*value)
				}
			}
		}
		values := append([]string{root}, paths...)
		for i, key := range envNames {
			main.Env = overwriteEnv(main.Env, corev1.EnvVar{Name: key, Value: values[i]})
		}
	}
	return nil
}

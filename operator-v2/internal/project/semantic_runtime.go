package project

import (
	"fmt"
	"strings"
)

func validateRuntime(d Document, issues *issueSet) {
	initialCurrent := map[string]bool{}
	for name, c := range d.Spec.Components {
		base := componentPath(name)
		if c.TemporalNamespace != nil && (lambda(c) || proxy(c) || versioned(c)) {
			issues.add(base+"/temporalNamespace", "UnsupportedNamespaceBinding", "additional namespace binding cannot be combined with Lambda, proxy or worker versioning")
		}
		if autoscale := c.Autoscaling; autoscale != nil && !explicitlyFalse(autoscale.Enabled) && !lambda(c) {
			if !workerEnabled(c) {
				issues.add(base+"/autoscaling", "WorkerRequired", "autoscaling requires a Temporal worker")
			}
			if c.Replicas != nil {
				issues.add(base+"/autoscaling", "FixedReplicaConflict", "autoscaling cannot be combined with explicit replicas")
			}
			if proxy(c) || authMode(d) == "mtls" {
				issues.add(base+"/autoscaling", "UnsupportedWorkerAuth", "autoscaling requires direct apiKey authentication")
			}
			if TaskQueue(d, c) == "" {
				issues.add(base+"/autoscaling/taskQueue", "TaskQueueRequired", "autoscaling requires an explicit resolvable task queue")
			}
			if number(autoscale.MinReplicas, 1) > number(autoscale.MaxReplicas, 10) {
				issues.add(base+"/autoscaling/minReplicas", "InvalidReplicaRange", "minimum replicas cannot exceed maximum replicas")
			}
		}
		if !lambda(c) {
			continue
		}
		serverless := c.Serverless
		if text(serverless.PackageType, "image") != "image" {
			issues.add(base+"/serverless/packageType", "UnsupportedLambdaPackage", "Lambda runtime requires an image package")
		}
		if build, found := d.Spec.Builds[string(c.Build)]; found && text(build.Strategy, "dockerfile") != "dockerfile" {
			issues.add(base+"/build", "UnsupportedLambdaBuild", "Lambda requires a Dockerfile build")
		}
		if c.Worker != nil {
			issues.add(base+"/worker", "UnsupportedLambdaWorker", "configure Lambda worker versioning under serverless.workerDeployment")
		}
		// Check field presence, not truthiness: even replicas:0 or access:false
		// has no supported meaning for a Lambda runtime.
		present := map[string]bool{
			"ports": c.Ports != nil, "serviceName": c.ServiceName != nil, "replicas": c.Replicas != nil,
			"autoscaling": c.Autoscaling != nil, "resources": c.Resources != nil,
			"automountServiceAccountToken": c.AutomountServiceAccountToken != nil, "awsIamRoleArn": c.AWSIAMRoleARN != nil,
			"isolatedExecutor": c.IsolatedExecutor != nil, "terminationGracePeriodSeconds": c.TerminationGracePeriodSeconds != nil,
			"command": c.Command != nil, "args": c.Args != nil, "temporalProxy": c.TemporalProxy != nil,
			"temporalAccess": c.TemporalAccess != nil, "secrets": c.Secrets != nil,
		}
		for field, found := range present {
			if found {
				issues.add(base+"/"+field, "UnsupportedLambdaField", "Kubernetes runtime field is unsupported for Lambda components")
			}
		}
		if mtlsFiles(c) {
			files := c.TemporalMtls.CertFiles
			if files.MountPath != nil || files.Paths != nil {
				issues.add(base+"/temporalMtls/certFiles", "UnsupportedLambdaMount", "Lambda certificate files are materialized under its temporary directory")
			}
		}
		if deployment := serverless.WorkerDeployment; deployment != nil {
			if env := deployment.Env; env != nil {
				seen := map[EnvName]bool{}
				for _, key := range env.DeploymentName {
					seen[key] = true
				}
				for _, key := range env.BuildID {
					if seen[key] {
						issues.add(base+"/serverless/workerDeployment/env", "DuplicateWorkerIdentityEnv", "deployment and build identifiers require distinct environment names")
					}
				}
			}
			if truth(deployment.InitialCurrent) {
				group := text(deployment.DeploymentName, name)
				if initialCurrent[group] {
					issues.add(base+"/serverless/workerDeployment/initialCurrent", "MultipleInitialCurrent", "only one component per worker deployment can select the initial current version")
				}
				initialCurrent[group] = true
			}
		}
	}
	if registry := d.Spec.Registry; registry != nil && registry.IsolatedLambda != nil && truth(registry.IsolatedLambda.Enabled) {
		if registry.Crashable != nil && truth(registry.Crashable.Enabled) {
			issues.add("/spec/registry/isolatedLambda", "WorkspaceModeConflict", "isolated Lambda and crashable workspace modes are mutually exclusive")
		}
		if strings.TrimSpace(text(registry.IsolatedLambda.TaskQueueEnv, "")) == "" {
			issues.add("/spec/registry/isolatedLambda/taskQueueEnv", "TaskQueueEnvRequired", "isolated Lambda mode requires an application task queue environment name")
		}
		found := false
		for _, component := range d.Spec.Components {
			found = found || lambda(component)
		}
		if !found {
			issues.add("/spec/registry/isolatedLambda", "LambdaRequired", "isolated Lambda mode requires a Lambda component")
		}
	}
}

func validateNamespaceUsage(d Document, issues *issueSet) {
	if d.Spec.Temporal == nil {
		return
	}
	referenced := map[string]bool{"primary": true}
	for _, c := range d.Spec.Components {
		if c.TemporalNamespace != nil {
			referenced[string(*c.TemporalNamespace)] = true
		}
	}
	for name, endpoint := range d.Spec.Temporal.NexusEndpoints {
		base := "/spec/temporal/nexusEndpoints/" + escapePointer(name)
		referenced[text(endpoint.TargetNamespace, "primary")] = true
		if strings.TrimSpace(endpoint.TargetTaskQueue) == "" {
			issues.add(base+"/targetTaskQueue", "TaskQueueRequired", "Nexus endpoint target task queue must be nonempty")
		}
		if len(endpoint.AllowedCallerNamespaces)+len(endpoint.AllowedCallerProjects) == 0 {
			issues.add(base, "NexusCallerRequired", "Nexus endpoint must explicitly allow at least one caller")
		}
		for _, label := range endpoint.AllowedCallerNamespaces {
			referenced[string(label)] = true
		}
		for index, caller := range endpoint.AllowedCallerProjects {
			if string(caller) == d.Metadata.Name || strings.HasPrefix(string(caller), "tmprl-dem-cld-") {
				issues.add(fmt.Sprintf("%s/allowedCallerProjects/%d", base, index), "InvalidNexusCallerProject", "caller must be another project name, not this project or a resolved namespace")
			}
		}
	}
	for label := range d.Spec.Temporal.Namespaces {
		if !referenced[label] {
			issues.add("/spec/temporal/namespaces/"+escapePointer(label), "UnusedTemporalNamespace", "additional namespace must be referenced by a component or endpoint")
		}
	}
}

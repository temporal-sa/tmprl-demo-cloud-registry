// Package integration contains public synthetic local-test fixture data.
// Actual API contracts are separately build-tagged and require the owned runner.
package integration

const runtimeDeploymentPrivateMarker = "PRIVATE_RUNTIME_INPUT_FIXTURE_ONLY"
const runtimeDeploymentDocument = `apiVersion: registry.tmprl-demo.cloud/v1alpha1
kind: DemoProject
metadata:
  name: runtime-example
spec:
  source:
    repo: https://github.com/temporal-sa/fixture
  builds:
    app:
      context: .
      dockerfile: Dockerfile
  components:
    app:
      build: app
      args: [-revision=runtime-deployment]
      env:
        - name: APP_LITERAL
          value: PRIVATE_RUNTIME_INPUT_FIXTURE_ONLY
      ports:
        - containerPort: 8080
`

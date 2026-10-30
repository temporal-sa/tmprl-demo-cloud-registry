.DEFAULT_GOAL := help
.NOTPARALLEL:
.PHONY: help tools test test-unit test-go test-ui test-helpers test-guards test-local test-kubernetes test-temporal

help:
	@printf '%s\n' 'tools            Acquire pinned public test dependencies and images' 'test-unit        Run isolated Go, UI and helper contracts' 'test-local       Run test-unit and disposable Kubernetes contracts' 'test-temporal    Run owned Go Temporal histories (macOS prerequisites)' 'Individual tiers: test-go, test-ui, test-helpers, test-guards, test-kubernetes'

# Acquisition is explicit. Test targets never install missing dependencies.
tools:
	python3 ../scripts/prepare_operator_kube_tests.py
	python3 ../scripts/prepare_operator_go_compiler.py --acquire
	python3 ../scripts/prepare_operator_python_tests.py
	python3 ../scripts/prepare_operator_node_tests.py --acquire

test: test-unit

test-unit: test-guards test-go test-ui test-helpers

test-guards:
	python3 -m unittest discover -s ../scripts/tests -p test_go_compiler_contract.py
	python3 -m unittest discover -s ../scripts/tests -p test_go_unit_runner_contract.py
	python3 -m unittest discover -s ../scripts/tests -p test_browser_boundary_contract.py
	python3 -m unittest discover -s ../scripts/tests -p test_browser_kube_contract.py
	python3 -m unittest discover -s ../scripts/tests -p test_preparation_test_contract.py

test-go:
	python3 ../scripts/run_operator_go_unit_tests.py
	python3 ../scripts/run_operator_go_unit_tests.py --metadata-process
	python3 ../scripts/run_operator_go_unit_tests.py --credential-observation-wire
	python3 ../scripts/run_operator_go_unit_tests.py --credential-staging-observation-wire
	python3 ../scripts/run_operator_go_unit_tests.py --aws-permission-invocation-process

test-ui:
	python3 ../scripts/verify_diagnostic_ui_contract.py
	python3 ../scripts/tests/test_node_runner_contract.py
	python3 ../scripts/run_operator_node_tests.py

test-helpers:
	python3 ../scripts/run_operator_python_tests.py --self-check

test-local: test-unit test-kubernetes

test-kubernetes:
	python3 ../scripts/run_operator_kube_tests.py

# Existing trusted Temporal CLI and the prepared Python environment are needed.
# The runner requires macOS sandbox-exec; it never uses Temporal Cloud.
test-temporal:
	python3 ../scripts/run_operator_temporal_tests.py --go --timeout 150

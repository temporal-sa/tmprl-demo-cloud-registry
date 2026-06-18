import dataclasses
import inspect
from types import ModuleType
from typing import get_type_hints

from demo_registry_operator.activities import canary
from demo_registry_operator.activities import force_reconcile
from demo_registry_operator.activities import instruqt
from demo_registry_operator.activities import reconcile
from demo_registry_operator.activities.builds import planning as build_planning
from demo_registry_operator.activities.builds.codebuild import execution as codebuild_execution
from demo_registry_operator.activities.external_resources import ecr
from demo_registry_operator.activities.external_resources import models as external_models
from demo_registry_operator.activities.external_resources import secrets
from demo_registry_operator.activities.external_resources import temporal
from demo_registry_operator.workflows import messages as workflow_messages


ACTIVITY_BOUNDARY_MODULES = (
    canary,
    codebuild_execution,
    ecr,
    external_models,
    force_reconcile,
    instruqt,
    reconcile,
    secrets,
    temporal,
    build_planning,
)

WORKFLOW_BOUNDARY_MODULES = (workflow_messages,)


def test_activity_boundary_dataclasses_use_concrete_annotations():
    for module in ACTIVITY_BOUNDARY_MODULES:
        for payload_type in module_dataclasses(module):
            hints = get_type_hints(payload_type)
            assert hints
            assert all(
                not isinstance(annotation, str)
                for annotation in payload_type.__annotations__.values()
            ), payload_type


def test_activity_functions_use_concrete_annotations():
    for module in ACTIVITY_BOUNDARY_MODULES:
        for function in module_functions(module):
            get_type_hints(function)
            assert all(
                not isinstance(annotation, str)
                for annotation in function.__annotations__.values()
            ), function


def test_workflow_boundary_dataclasses_use_concrete_annotations():
    for module in WORKFLOW_BOUNDARY_MODULES:
        for payload_type in module_dataclasses(module):
            hints = get_type_hints(payload_type)
            assert hints
            assert all(
                not isinstance(annotation, str)
                for annotation in payload_type.__annotations__.values()
            ), payload_type


def module_dataclasses(module: ModuleType) -> list[type]:
    return [
        value
        for value in vars(module).values()
        if isinstance(value, type)
        and dataclasses.is_dataclass(value)
        and value.__module__ == module.__name__
    ]


def module_functions(module: ModuleType) -> list:
    return [
        value
        for value in vars(module).values()
        if inspect.isfunction(value)
        and value.__module__ == module.__name__
        and value.__annotations__
    ]

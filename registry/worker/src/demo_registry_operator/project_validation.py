"""Access to scripts/validate_projects.py from the operator.

The operator image copies the validator to /app/src/validate_projects.py and the
schemas to /app/schemas (see registry/worker/Dockerfile), so it imports
directly. In a source checkout (tests, local runs) it is loaded from scripts/.
"""

from __future__ import annotations

import importlib.util
import sys
from functools import cache
from pathlib import Path
from types import ModuleType

SOURCE_CHECKOUT_VALIDATOR = (
    Path(__file__).resolve().parents[4] / "scripts" / "validate_projects.py"
)


@cache
def validator_module() -> ModuleType:
    try:
        import validate_projects
    except ModuleNotFoundError:
        spec = importlib.util.spec_from_file_location(
            "validate_projects", SOURCE_CHECKOUT_VALIDATOR
        )
        if spec is None or spec.loader is None:
            raise
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        return module
    return validate_projects


@cache
def default_validators() -> dict[str, object]:
    return validator_module().load_default_validators()

"""Pure negative guards; these mocks make no Docker/network isolation claim."""
from __future__ import annotations

import copy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from operator_node_test_contract import EXPECTED_CASES
from run_operator_node_tests import check_cases, check_container


class NodeRunnerContract(unittest.TestCase):
    def transcript(self, cases=EXPECTED_CASES):
        return "\n".join([*(f"ok {i} - {name}" for i, name in enumerate(cases, 1)), f"# tests {len(cases)}", f"# pass {len(cases)}", "# fail 0", "# cancelled 0", "# skipped 0", "# todo 0", "Verified Node boundary and diagnostic contracts completed in the same container."])

    def test_zero_todo_summary_is_not_a_directive(self):
        check_cases(self.transcript())

    def test_missing_extra_skipped_failed_or_unproven_cases_are_refused(self):
        valid = self.transcript()
        changes = (
            valid.replace("ok 1 - ", "not ok 1 - ", 1),
            "\n".join(valid.splitlines()[1:]),
            valid + f"\nok {len(EXPECTED_CASES) + 1} - unexpected case",
            self.transcript(EXPECTED_CASES[9:]),  # omit whole page suite
            self.transcript(EXPECTED_CASES[:9] + EXPECTED_CASES[25:]),  # omit component suite
            self.transcript(EXPECTED_CASES[:25]),  # omit all pure browser groups
            self.transcript(EXPECTED_CASES[:25] + EXPECTED_CASES[29:]),  # omit browser kernel validators
            self.transcript(EXPECTED_CASES[:29]),  # omit browser page protocol
            self.transcript(EXPECTED_CASES[:29] + EXPECTED_CASES[33:]),  # omit only page protocol
            self.transcript(EXPECTED_CASES[:33]),  # omit only source outage protocol
            valid.replace(EXPECTED_CASES[0], EXPECTED_CASES[0] + " # SKIP unavailable", 1),
            valid.replace("# skipped 0", "# skipped 1"),
            valid.replace("# todo 0", "# todo 1"),
            valid.replace("# fail 0", "# fail 1"),
            valid.replace("Verified Node boundary and diagnostic contracts completed in the same container.", ""),
        )
        for changed in changes:
            with self.subTest(changed=changed[-80:]):
                with self.assertRaises(RuntimeError):
                    check_cases(changed)

    def container(self):
        return {"Image": "pinned-image-id", "Config": {"Labels": {"operator.local/node-test-run": "owned-run"}, "User": "65532:65532", "Env": ["HOME=/tmp/home"]}, "Mounts": [], "HostConfig": {"NetworkMode": "none", "ReadonlyRootfs": True, "Binds": None, "Privileged": False, "Devices": [], "CapAdd": None, "CapDrop": ["ALL"], "SecurityOpt": ["no-new-privileges"], "Tmpfs": {"/tmp": "rw,noexec,nosuid,nodev,size=32m"}}}

    def test_changed_network_mount_privilege_credentials_or_ownership_are_refused(self):
        check_container(self.container(), "owned-run", "pinned-image-id")
        mutations = (
            lambda c: c["HostConfig"].update(NetworkMode="host"),
            lambda c: c["HostConfig"].update(Binds=["/host:/reviewed"]),
            lambda c: c["HostConfig"].update(ReadonlyRootfs=False),
            lambda c: c["HostConfig"].update(Privileged=True),
            lambda c: c["HostConfig"].update(CapAdd=["NET_ADMIN"]),
            lambda c: c["Config"].update(User="0:0"),
            lambda c: c["Config"].update(Env=["AWS_SECRET_ACCESS_KEY=synthetic"]),
            lambda c: c.update(Mounts=[{"Type": "bind", "Destination": "/tmp"}]),
            lambda c: c["Config"].update(Labels={}),
            lambda c: c.update(Image="foreign-image-id"),
        )
        for mutate in mutations:
            changed = copy.deepcopy(self.container())
            mutate(changed)
            with self.assertRaises(RuntimeError):
                check_container(changed, "owned-run", "pinned-image-id")


if __name__ == "__main__":
    unittest.main()

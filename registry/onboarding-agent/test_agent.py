"""Tests for the agent wrapper. Run with the agent's own dependencies:

    python -m pytest registry/onboarding-agent/test_agent.py
"""

from __future__ import annotations

import base64
import json
import subprocess
from pathlib import Path

import agent
from claude_agent_sdk import ToolUseBlock


def git(repo: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


def test_patch_includes_new_files_and_edits(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    git(repo, "init", "-q")
    (repo / "shared.go").write_text("const Queue = \"q\"\n")
    git(repo, "add", ".")
    git(repo, "-c", "user.email=a@b", "-c", "user.name=a", "commit", "-qm", "init")
    (repo / "shared.go").write_text("var Queue = os.Getenv(\"TASK_QUEUE\")\n")
    (repo / "Dockerfile.worker").write_text("FROM scratch\n")

    patch = agent.repo_patch(repo)

    assert "diff --git a/shared.go b/shared.go" in patch
    assert "diff --git a/Dockerfile.worker b/Dockerfile.worker" in patch
    assert "new file mode" in patch


def test_progress_text_hides_workspace_paths_and_cd_prefixes(monkeypatch):
    repo = Path("/workspace/repo")
    monkeypatch.setattr(agent, "WORKSPACE", Path("/workspace"))

    def text(name, **data):
        return agent.progress_text(ToolUseBlock(id="1", name=name, input=data), repo)

    assert text("Read", file_path="/workspace/repo/worker/main.go") == "Reading worker/main.go"
    assert text("Bash", command="cd /workspace/repo && go build ./...") == "go build ./..."
    assert text("Bash", command="cd /workspace/repo; ls", description="List files") == "List files"
    assert text("Bash", command="python3 /workspace/context/scripts/validate_projects.py /workspace/draft.yaml") == (
        "python3 context/scripts/validate_projects.py draft.yaml"
    )
    assert text("StructuredOutput") == "Writing up the findings"


def test_result_lines_reassemble(capsys):
    agent.emit_result({"summary": "x" * 20000})

    lines = capsys.readouterr().out.splitlines()
    parts = [line.split(" ", 3) for line in lines]
    assert {int(part[2]) for part in parts} == {len(lines)}
    data = json.loads(base64.b64decode("".join(part[3] for part in parts)))
    assert data["summary"] == "x" * 20000


def test_context_is_laid_out_like_the_registry_repo(tmp_path):
    request = {
        "context": {
            "schemas": {"project.schema.json": "{}", "instruqt-course.schema.json": "{}"},
            "validator": "print('v')",
            "projects_readme": "# Projects",
            "examples": {"canonical-ai-demo": "kind: DemoProject\n"},
            "taken_subdomains": ["expense-approval", "money-transfer"],
        }
    }

    agent.write_context(request, tmp_path)

    assert (tmp_path / "schemas" / "instruqt-course.schema.json").exists()
    assert (tmp_path / "scripts" / "validate_projects.py").exists()
    assert (tmp_path / "projects" / "demo" / "canonical-ai-demo.yaml").exists()
    assert (tmp_path / "buildkit" / "12-demo-catalog-readiness.md").exists()
    assert (tmp_path / "taken-subdomains.txt").read_text() == "expense-approval\nmoney-transfer\n"

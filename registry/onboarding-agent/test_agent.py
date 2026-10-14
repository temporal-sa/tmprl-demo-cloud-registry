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


def test_session_survives_the_round_trip_between_pods(tmp_path):
    first = tmp_path / "first"
    transcript = first / ".claude" / "projects" / "-workspace-repo" / "abc-123.jsonl"
    transcript.parent.mkdir(parents=True)
    transcript.write_text('{"type":"user"}\n')
    (transcript.parent / "abc-123" / "tool-results").mkdir(parents=True)

    mounted = tmp_path / "session"
    mounted.mkdir()
    (mounted / "session").write_text(agent.export_session(first))
    (mounted / "session_id").write_text("abc-123\n")
    second = tmp_path / "second"

    assert agent.restore_session(mounted, second) == "abc-123"
    restored = second / ".claude" / "projects" / "-workspace-repo"
    assert (restored / "abc-123.jsonl").read_text() == '{"type":"user"}\n'
    assert (restored / "abc-123" / "tool-results").is_dir()


def test_no_session_mounted_means_a_fresh_start(tmp_path):
    assert agent.restore_session(tmp_path / "missing", tmp_path / "home") is None
    assert agent.export_session(tmp_path / "home") == ""


def test_session_ids_must_look_like_ids(tmp_path):
    (tmp_path / "session").write_text(base64.b64encode(b"").decode())
    (tmp_path / "session_id").write_text("../../etc")

    assert agent.restore_session(tmp_path, tmp_path / "home") is None


REQUEST = {
    "mode": "chat",
    "repo": {"commit_sha": "c" * 40},
    "document": {"metadata": {"name": "demo"}},
    "errors": [],
    "question": "Use money-transfer as the subdomain.",
    "history": [
        {"role": "user", "text": "Draft the whole project from the repo."},
        {"role": "agent", "text": "Drafted it.", "outcome": "Done", "questions": ["Does the UI need a secret?"]},
        {"role": "user", "text": "Which port does the UI use?"},
        {"role": "agent", "text": "Out of budget.", "outcome": "Failed"},
    ],
}


def test_resumed_turn_gets_only_what_the_session_missed():
    prompt = agent.task_prompt(REQUEST, Path("/workspace/draft.yaml"), resumed=True)

    assert "never saw" in prompt
    assert "Drafted it." not in prompt
    assert "User: Which port does the UI use?" in prompt
    assert prompt.endswith("The user says:\nUse money-transfer as the subdomain.")


def test_fresh_turn_replays_the_whole_conversation():
    prompt = agent.task_prompt(
        REQUEST, Path("/workspace/draft.yaml"), resumed=False, patch_note="Edits reapplied."
    )

    assert "The conversation so far:" in prompt
    assert "You: Drafted it.\n  You asked: Does the UI need a secret?" in prompt
    assert "You (run did not finish: Failed): Out of budget." in prompt
    assert "Edits reapplied." in prompt


def test_earlier_edits_are_reapplied_or_reported(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    git(repo, "init", "-q")
    (repo / "main.go").write_text('const Queue = "q"\n')
    git(repo, "add", ".")
    git(repo, "-c", "user.email=a@b", "-c", "user.name=a", "commit", "-qm", "init")
    (repo / "main.go").write_text('var Queue = os.Getenv("TASK_QUEUE")\n')
    (repo / "health.go").write_text("package main\n")
    patch = agent.repo_patch(repo)
    agent.reset_checkout(repo)
    assert (repo / "main.go").read_text() == 'const Queue = "q"\n'

    assert "reapplied" in agent.reapply_patch(repo, patch)
    assert (repo / "health.go").exists()
    # Applying the conversation's patch again keeps it cumulative.
    assert agent.repo_patch(repo) == patch

    agent.reset_checkout(repo)
    (repo / "main.go").write_text("changed upstream\n")
    assert "no longer apply" in agent.reapply_patch(repo, patch)
    assert agent.reapply_patch(repo, "") == ""

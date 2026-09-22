from __future__ import annotations

import re
import subprocess
import unittest
from pathlib import Path

from scripts.eval_agent_skill import load_cases, validate_results


ROOT = Path(__file__).parents[1]
SKILL_DIR = ROOT / "skills" / "herdr"
SKILL = SKILL_DIR / "SKILL.md"
REFERENCES = {
    "references/agents.md",
    "references/automation.md",
    "references/external-workers.md",
    "references/remote-and-persistence.md",
}


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def package_entries(output: str) -> set[str]:
    return {line.replace("\\", "/") for line in output.splitlines()}


class AgentSkillTests(unittest.TestCase):
    def test_package_entries_normalize_windows_separators(self) -> None:
        self.assertEqual(
            package_entries("skills\\herdr\\SKILL.md\nskills/herdr/references/agents.md\n"),
            {"skills/herdr/SKILL.md", "skills/herdr/references/agents.md"},
        )

    def test_every_local_reference_exists(self) -> None:
        text = SKILL.read_text(encoding="utf-8")
        links = set(re.findall(r"\[[^]]+\]\(([^)]+\.md)\)", text))

        self.assertEqual(links, REFERENCES)
        for link in links:
            self.assertTrue((SKILL_DIR / link).is_file(), link)

    def test_skill_resources_are_in_the_source_package(self) -> None:
        expected = {"skills/herdr/SKILL.md", *(f"skills/herdr/{path}" for path in REFERENCES)}
        package = subprocess.run(
            ["cargo", "package", "--list", "--allow-dirty"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        packaged = package_entries(package.stdout)

        self.assertTrue(expected <= packaged, sorted(expected - packaged))
        self.assertIn('"skills/herdr/**/*"', read("Cargo.toml"))

    def test_docs_point_to_the_canonical_skill_folder(self) -> None:
        for relative in [
            "distribution/agent-guide.md",
            "docs/next/website/src/content/docs/agent-skill.mdx",
        ]:
            text = read(relative)
            self.assertIn(
                "https://github.com/f5-sales-demo/herdr/tree/build-xcsh/skills/herdr",
                text,
                relative,
            )
            self.assertIn("herdr --skill", text, relative)

    def test_public_skill_contains_no_private_values(self) -> None:
        text = "\n".join(path.read_text(encoding="utf-8") for path in SKILL_DIR.rglob("*.md"))
        for forbidden in [
            '"answer":',
            '"capability":',
            '"lease":',
            "HERDR_NATIVE_CAPABILITY",
            "context_capability",
            "native_capability",
        ]:
            self.assertNotIn(forbidden, text)
        self.assertNotRegex(text, r"(?i)(token|secret|lease)\s*[=:]\s*[A-Za-z0-9_-]{16,}")

    def test_self_contained_core_qualifies_prompt_admission(self) -> None:
        core = SKILL.read_text(encoding="utf-8")
        self.assertIn("When admission starts from a non-working state", core)
        self.assertIn("If the agent is already working", core)

    def test_forward_eval_matrix_covers_acceptance_scenarios(self) -> None:
        cases = load_cases()
        trigger_cases = {case["id"]: case["should_trigger"] for case in cases["trigger_cases"]}
        self.assertIn(True, trigger_cases.values())
        self.assertIn(False, trigger_cases.values())
        self.assertEqual(
            {case["id"] for case in cases["behavior_cases"]},
            {
                "direct-managed-pane",
                "unpaired-process",
                "pair-external-worker",
                "revoke-external-worker",
                "codex-kind",
                "claude-kind",
                "opencode-kind",
                "xcsh-kind",
                "unknown-kind",
                "blocked-question",
                "interaction-acknowledgement",
                "durable-semantic-completion",
                "moved-pane",
                "duplicate-machine-id",
                "missing-capability",
                "forbidden-context-discovery",
            },
        )

    def test_forward_eval_validation_rejects_wrong_results(self) -> None:
        cases = load_cases()
        results = {
            "trigger_results": [
                {"id": case["id"], "should_trigger": not case["should_trigger"]}
                for case in cases["trigger_cases"]
            ],
            "behavior_results": [
                {"id": case["id"], "outcome": "wrong"}
                for case in cases["behavior_cases"]
            ],
        }
        self.assertTrue(validate_results(cases, results))

    def test_forward_eval_validation_accepts_expected_results(self) -> None:
        cases = load_cases()
        results = {
            "trigger_results": [
                {"id": case["id"], "should_trigger": case["should_trigger"]}
                for case in cases["trigger_cases"]
            ],
            "behavior_results": [
                {"id": case["id"], "outcome": case["expected_outcome"]}
                for case in cases["behavior_cases"]
            ],
        }
        self.assertEqual(validate_results(cases, results), [])

        results["trigger_results"].append(results["trigger_results"][0])
        self.assertIn(
            "trigger results contain duplicate IDs",
            validate_results(cases, results),
        )


if __name__ == "__main__":
    unittest.main()

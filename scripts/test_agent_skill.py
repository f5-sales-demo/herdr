from __future__ import annotations

import re
import subprocess
import unittest
from pathlib import Path


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

    def test_scenario_contracts_remain_covered(self) -> None:
        core = SKILL.read_text(encoding="utf-8")
        agents = read("skills/herdr/references/agents.md")
        automation = read("skills/herdr/references/automation.md")
        external = read("skills/herdr/references/external-workers.md")
        remote = read("skills/herdr/references/remote-and-persistence.md")

        scenarios = {
            "direct managed pane": (core, 'test "${HERDR_ENV:-}" = 1'),
            "unpaired worker": (core, "this process is not paired with Herdr"),
            "pair and revoke": (external, "herdr context revoke"),
            "known and unknown harnesses": (agents, "Codex, Claude Code, OpenCode, xcsh, and an unknown harness"),
            "blocked question": (agents, "returns `agent_blocked` without answering"),
            "delivery acknowledgement": (automation, "acknowledgement can mark the receipt accepted"),
            "durable versus semantic completion": (automation, "output drainage (`output_complete` after EOF)"),
            "moved pane": (core, ".result.move_result.pane.pane_id"),
            "duplicate cross-machine IDs": (remote, "Two machines can both report `w1:p1`"),
            "older server": (automation, "Treat an absent name as unsupported"),
            "no global environment": (external, "export `HERDR_ENV` globally"),
            "no socket scanning": (external, "never scan config directories or sockets"),
        }
        for scenario, (text, expected) in scenarios.items():
            with self.subTest(scenario=scenario):
                self.assertIn(expected, text)


if __name__ == "__main__":
    unittest.main()

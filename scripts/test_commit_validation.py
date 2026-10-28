"""PR constituents, direct pushes, and authenticated merge provenance."""
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
import scripts.validate_merge_commits as validate_merge_commits
from scripts.conventional_commits import valid_subject

class CommitValidationTests(unittest.TestCase):
    def test_conventional_build_type_is_supported(self):
        self.assertTrue(valid_subject("build: update the native compiler"))

    def test_pr_and_push_ranges_preserve_conventional_enforcement(self):
        """PR and push ranges both reject invalid constituent commits."""
        validator = Path(__file__).with_name("conventional_commits.py")

        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory)

            def git(*args: str) -> str:
                return subprocess.check_output(["git", *args], cwd=repo, text=True).strip()

            def commit(subject: str, content: str) -> str:
                (repo / "history.txt").write_text(content, encoding="utf-8")
                git("add", "history.txt")
                git("commit", "-m", subject)
                return git("rev-parse", "HEAD")

            git("init", "--initial-branch=main")
            git("config", "user.name", "Herdr test")
            git("config", "user.email", "test@example.invalid")
            base = commit("chore: seed validation fixture", "base\n")
            git("switch", "--create", "feature")
            pr_head = commit("style: invalid PR constituent", "feature\n")

            def validate(*args: str) -> subprocess.CompletedProcess[str]:
                return subprocess.run(
                    [sys.executable, str(validator), *args],
                    cwd=repo,
                    text=True,
                    capture_output=True,
                    check=False,
                )

            pr_result = validate("--range", f"{base}..{pr_head}")
            self.assertNotEqual(pr_result.returncode, 0)
            self.assertIn("style: invalid PR constituent", pr_result.stdout)

            git("switch", "main")
            git("merge", "--no-ff", "feature", "-m", "Merge direct invalid feature")
            merge = git("rev-parse", "HEAD")
            self.assertNotEqual(validate("--range", f"{base}..{merge}").returncode, 0)
            self.assertEqual(
                validate_merge_commits.invalid_merge_subjects(
                    f"{base}..{merge}", "owner/repo", "main", "token", lambda *_: [], lambda *_: [], lambda *_: {}, repo
                ),
                [(merge, "Merge direct invalid feature")],
            )

            git("switch", "--create", "valid-feature")
            valid_head = commit("fix: preserve valid merge history", "valid feature\n")
            git("switch", "main")
            git("merge", "--no-ff", "valid-feature", "-m", "Merge invalid subject with valid side")
            invalid_subject_merge = git("rev-parse", "HEAD")
            self.assertEqual(validate("--range", f"{merge}..{invalid_subject_merge}").returncode, 0)
            self.assertEqual(
                validate_merge_commits.invalid_merge_subjects(
                    f"{merge}..{invalid_subject_merge}", "owner/repo", "main", "token", lambda *_: [], lambda *_: [], lambda *_: {}, repo
                ),
                [(invalid_subject_merge, "Merge invalid subject with valid side")],
            )

            git("switch", "--create", "valid-merge")
            valid_head = commit("fix: preserve valid merge history", "valid merge\n")
            git("switch", "main")
            git("merge", "--no-ff", "valid-merge", "-m", "fix: merge valid feature")
            valid_merge = git("rev-parse", "HEAD")
            self.assertEqual(validate("--range", f"{invalid_subject_merge}..{valid_merge}").returncode, 0)
            self.assertEqual(validate("--range", f"{merge}..{valid_head}").returncode, 0)
            self.assertEqual(
                validate_merge_commits.invalid_merge_subjects(
                    f"{invalid_subject_merge}..{valid_merge}", "owner/repo", "main", "token", lambda *_: [], lambda *_: [], lambda *_: {}, repo
                ),
                [],
            )

            git("switch", "--create", "valid-direct", valid_merge)
            direct_valid = commit("fix: validate direct push", "valid direct\n")
            self.assertEqual(validate("--range", f"{valid_merge}..{direct_valid}").returncode, 0)

            git("switch", "main")
            direct_head = commit("style: invalid direct push", "direct\n")
            direct_result = validate("--range", f"{valid_merge}..{direct_head}")
            self.assertNotEqual(direct_result.returncode, 0)
            self.assertIn("style: invalid direct push", direct_result.stdout)

    def test_github_merge_wrapper_requires_matching_conventional_pr_evidence(self):
        sha = "a" * 40
        subject = "Merge pull request #52 from owner/branch"
        recorded = [{
            "state": "closed",
            "merged_at": "2026-09-09T00:00:00Z",
            "merge_commit_sha": sha,
            "base": {"ref": "build-xcsh"},
            "head": {"sha": "b" * 40},
            "title": "fix(ci): preserve merge constituent validation",
        }]
        passing_checks = [{
            "name": "conventional-commits", "status": "completed", "conclusion": "success",
            "app": {"slug": "github-actions"},
            "details_url": "https://github.com/owner/repo/actions/runs/123/job/456",
        }]
        passing_run = {"name": "CI", "event": "pull_request", "status": "completed", "conclusion": "success", "head_sha": "b" * 40}
        self.assertTrue(
            validate_merge_commits.is_recorded_pr_merge(
                subject, sha, "owner/repo", "build-xcsh", "token", lambda *_: recorded, lambda *_: passing_checks, lambda *_: passing_run
            )
        )
        recorded[0]["title"] = "invalid merge title"
        self.assertFalse(
            validate_merge_commits.is_recorded_pr_merge(
                subject, sha, "owner/repo", "build-xcsh", "token", lambda *_: recorded, lambda *_: passing_checks, lambda *_: passing_run
            )
        )
        recorded[0]["title"] = "fix(ci): preserve merge constituent validation"
        passing_checks[0]["conclusion"] = "success"
        passing_checks[0]["app"] = {"slug": "unrelated-app"}
        self.assertFalse(validate_merge_commits.is_recorded_pr_merge(subject, sha, "owner/repo", "build-xcsh", "token", lambda *_: recorded, lambda *_: passing_checks, lambda *_: passing_run))
        passing_checks[0]["app"] = {"slug": "github-actions"}
        passing_run["head_sha"] = "c" * 40
        self.assertFalse(validate_merge_commits.is_recorded_pr_merge(subject, sha, "owner/repo", "build-xcsh", "token", lambda *_: recorded, lambda *_: passing_checks, lambda *_: passing_run))
        recorded[0]["title"] = "fix(ci): preserve merge constituent validation"
        passing_checks[0]["conclusion"] = "failure"
        self.assertFalse(
            validate_merge_commits.is_recorded_pr_merge(
                subject, sha, "owner/repo", "build-xcsh", "token", lambda *_: recorded, lambda *_: passing_checks, lambda *_: passing_run
            )
        )


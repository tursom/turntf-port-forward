import subprocess
import sys
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("release_metadata.py")


class ReleaseMetadataCLITest(unittest.TestCase):
    def run_metadata(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(SCRIPT), *arguments],
            check=False,
            capture_output=True,
            text=True,
        )

    def test_stable_version_outputs_all_release_tags(self) -> None:
        result = self.run_metadata("v1.2.3")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(),
            [
                "exact=1.2.3",
                "major_minor=1.2",
                "major=1",
                "latest=latest",
                "prerelease=false",
            ],
        )

    def test_prerelease_outputs_only_the_exact_version(self) -> None:
        result = self.run_metadata("v1.2.3-rc.1")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(),
            [
                "exact=1.2.3-rc.1",
                "prerelease=true",
            ],
        )

    def test_build_metadata_uses_an_oci_safe_exact_tag(self) -> None:
        result = self.run_metadata("v1.2.3+build.4")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(),
            [
                "exact=1.2.3_build.4",
                "major_minor=1.2",
                "major=1",
                "latest=latest",
                "prerelease=false",
            ],
        )

    def test_prerelease_with_build_metadata_has_no_stable_aliases(self) -> None:
        result = self.run_metadata("v1.2.3-rc.1+build.4")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(),
            [
                "exact=1.2.3-rc.1_build.4",
                "prerelease=true",
            ],
        )

    def test_invalid_semver_tags_are_rejected(self) -> None:
        for tag in ("1.2.3", "v1.2", "v01.2.3", "v1.2.3-01", "v1.2.3+"):
            with self.subTest(tag=tag):
                result = self.run_metadata(tag)

                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, "")
                self.assertIn("not a valid v-prefixed SemVer version", result.stderr)

    def test_exact_tag_longer_than_128_characters_is_rejected(self) -> None:
        result = self.run_metadata("v1.2.3+" + "a" * 123)

        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")
        self.assertIn("exceeds the OCI tag length limit", result.stderr)

    def test_master_outputs_moving_and_commit_tags(self) -> None:
        result = self.run_metadata(
            "--branch",
            "master",
            "0123456789abcdef0123456789abcdef01234567",
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(),
            [
                "exact=master",
                "commit=sha-0123456789ab",
                "prerelease=false",
            ],
        )


if __name__ == "__main__":
    unittest.main()

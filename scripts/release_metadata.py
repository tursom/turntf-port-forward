#!/usr/bin/env python3

import re
import sys


SEMVER = re.compile(
    r"^v(0|[1-9][0-9]*)\."
    r"(0|[1-9][0-9]*)\."
    r"(0|[1-9][0-9]*)"
    r"(?:-((?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)"
    r"(?:\.(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*))?"
    r"(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$"
)


def main() -> int:
    if len(sys.argv) == 4 and sys.argv[1] == "--branch":
        branch, commit = sys.argv[2:]
        if branch != "master" or re.fullmatch(r"[0-9a-fA-F]{40}", commit) is None:
            print("branch metadata requires master and a full commit SHA", file=sys.stderr)
            return 1
        print("exact=master")
        print(f"commit=sha-{commit[:12].lower()}")
        print("prerelease=false")
        return 0

    if len(sys.argv) != 2:
        print(
            f"usage: {sys.argv[0]} vMAJOR.MINOR.PATCH | --branch master COMMIT_SHA",
            file=sys.stderr,
        )
        return 2

    match = SEMVER.fullmatch(sys.argv[1])
    if match is None:
        print(f"tag {sys.argv[1]} is not a valid v-prefixed SemVer version", file=sys.stderr)
        return 1

    major, minor, patch, prerelease, build = match.groups()
    version = f"{major}.{minor}.{patch}"
    exact = version
    if prerelease is not None:
        exact += f"-{prerelease}"
    if build is not None:
        exact += f"_{build}"
    if len(exact) > 128:
        print(
            f"tag {sys.argv[1]} exceeds the OCI tag length limit after normalization",
            file=sys.stderr,
        )
        return 1

    if prerelease is not None:
        print(f"exact={exact}")
        print("prerelease=true")
        return 0

    print(f"exact={exact}")
    print(f"major_minor={major}.{minor}")
    print(f"major={major}")
    print("latest=latest")
    print("prerelease=false")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

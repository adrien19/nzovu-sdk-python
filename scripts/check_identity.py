"""Reject old import/product names in active tracked files and built distributions."""

import argparse
import re
import subprocess
import tarfile
import zipfile
from pathlib import Path

PATTERN = re.compile(rb"chrono" + rb"(?:queue|q(?:sdk)?)", re.IGNORECASE)
ALLOWED = {"docs/LEGACY_CHANGELOG.md"}
ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dist", type=Path, default=ROOT / "dist")
    args = parser.parse_args()
    violations = []
    paths = subprocess.check_output(["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=ROOT)
    for relative in set(paths.decode().split("\0")) - {""}:
        path = ROOT / relative
        if relative not in ALLOWED and path.is_file() and PATTERN.search(path.read_bytes()):
            violations.append(relative)
    for path in args.dist.glob("nzovu-*"):
        if path.suffix == ".whl":
            with zipfile.ZipFile(path) as archive:
                for name in archive.namelist():
                    if PATTERN.search(name.encode()) or PATTERN.search(archive.read(name)):
                        violations.append(f"{path.name}:{name}")
        elif path.name.endswith(".tar.gz"):
            with tarfile.open(path) as archive:
                for entry in archive:
                    if entry.isfile():
                        member = archive.extractfile(entry)
                        if member is not None and (
                            PATTERN.search(entry.name.encode()) or PATTERN.search(member.read())
                        ):
                            violations.append(f"{path.name}:{entry.name}")
    if violations:
        raise SystemExit("Legacy identifiers found:\n" + "\n".join(violations))
    print("Active source and artifact identity checks passed")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Vendor protocol sources from an explicitly selected local Nzovu checkout."""

import argparse
import hashlib
import io
import json
import re
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--ref", required=True, help="Full server commit SHA")
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.ref):
        parser.error("--ref must be a full commit SHA")
    commit = subprocess.check_output(
        ["git", "-C", str(args.source), "rev-parse", f"{args.ref}^{{commit}}"], text=True
    ).strip()
    archive = subprocess.check_output(["git", "-C", str(args.source), "archive", commit, "proto"])
    with tempfile.TemporaryDirectory(prefix="nzovu-proto-") as temporary:
        destination = Path(temporary)
        checksums = {}
        with tarfile.open(fileobj=io.BytesIO(archive)) as source:
            for member in source.getmembers():
                path = Path(member.name)
                if not member.isfile() or path.suffix != ".proto":
                    continue
                if path.is_absolute() or ".." in path.parts or path.parts[0] != "proto":
                    raise ValueError(f"Unexpected archive path: {path}")
                stream = source.extractfile(member)
                if stream is None:
                    raise ValueError(f"Unreadable archive file: {path}")
                with stream:
                    data = stream.read()
                output = destination / path
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_bytes(data)
                checksums[path.as_posix()] = hashlib.sha256(data).hexdigest()
        if "proto/queueservice/v1/service.proto" not in checksums:
            raise ValueError("Selected commit does not contain the Nzovu service protocol")
        manifest = {
            "repository": "https://github.com/adrien19/nzovu",
            "commit": commit,
            "files": dict(sorted(checksums.items())),
        }
        (destination / "proto/SOURCE.json").write_text(json.dumps(manifest, indent=2) + "\n")
        shutil.rmtree(ROOT / "proto")
        shutil.copytree(destination / "proto", ROOT / "proto")
    print(f"Vendored {len(checksums)} protocol files from {commit}")


if __name__ == "__main__":
    main()

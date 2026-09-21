#!/usr/bin/env python3
"""Generate Python modules while retaining canonical server descriptor paths."""

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import sys
import tempfile
from importlib.resources import files
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "nzovu/api"


def verify_sources():
    manifest = json.loads((ROOT / "proto/SOURCE.json").read_text())
    actual = {
        path.relative_to(ROOT).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted((ROOT / "proto").rglob("*.proto"))
    }
    if actual != manifest["files"]:
        raise ValueError("Protocol sources differ from SOURCE.json; use the explicit update-proto command")
    return sorted(actual)


def snapshot(directory):
    return {
        path.relative_to(directory).as_posix(): path.read_bytes()
        for path in directory.rglob("*")
        if path.is_file() and path.suffix in {".py", ".pyi"}
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    sources = verify_sources()
    with tempfile.TemporaryDirectory(prefix="nzovu-python-proto-") as temporary:
        destination = Path(temporary)
        subprocess.run(
            [
                sys.executable,
                "-m",
                "grpc_tools.protoc",
                f"-I{ROOT}",
                f"-I{files('grpc_tools') / '_proto'}",
                f"--python_out={destination}",
                f"--pyi_out={destination}",
                f"--grpc_python_out={destination}",
                *sources,
            ],
            cwd=ROOT,
            check=True,
        )
        generated = destination / "proto"
        for path in generated.rglob("*"):
            if not path.is_file():
                continue
            text = path.read_text()
            text = text.replace("from proto.", "from nzovu.api.")
            text = re.sub(
                r"(BuildTopDescriptorsAndMessages\(DESCRIPTOR, ['\"])proto\.",
                r"\1nzovu.api.",
                text,
            )
            path.write_text(text)
        for directory in [generated, *(path for path in generated.rglob("*") if path.is_dir())]:
            (directory / "__init__.py").touch()
        if args.check:
            if snapshot(generated) != snapshot(OUTPUT):
                raise SystemExit("Generated protocol files differ; run make gen-proto")
            print("Generated protocol files are current")
        else:
            if OUTPUT.exists():
                shutil.rmtree(OUTPUT)
            shutil.copytree(generated, OUTPUT)
            print(f"Generated {len(snapshot(OUTPUT))} Python protocol files")


if __name__ == "__main__":
    main()

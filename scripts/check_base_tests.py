"""Run the test suite against an installed wheel without the optional models extra."""

import argparse
import importlib.metadata
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True)
    args = parser.parse_args()
    wheel = ROOT / "dist" / f"nzovu-{args.version}-py3-none-any.whl"
    if not wheel.is_file():
        parser.error("Build the wheel before running base-install tests")
    environment = dict(os.environ)
    environment.pop("PYTHONPATH", None)
    environment.pop("PYTHONHOME", None)
    with tempfile.TemporaryDirectory(prefix="nzovu-base-tests-") as temporary:
        target = Path(temporary)
        subprocess.run([sys.executable, "-m", "venv", str(target / "venv")], check=True, env=environment)
        python = str(target / "venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python"))
        constraints = target / "constraints.txt"
        constraints.write_text(
            "\n".join(sorted(f"{item.metadata['Name']}=={item.version}" for item in importlib.metadata.distributions()))
        )
        subprocess.run(
            [
                python,
                "-m",
                "pip",
                "install",
                "--disable-pip-version-check",
                "-c",
                str(constraints),
                str(wheel),
                "pytest",
                "pytest-asyncio",
                "grpcio-tools",
            ],
            check=True,
            cwd=target,
            env=environment,
        )
        subprocess.run(
            [python, "-I", "-c", "import importlib.util; assert importlib.util.find_spec('pydantic') is None"],
            check=True,
            cwd=target,
            env=environment,
        )
        for directory in ("tests", "proto", "scripts"):
            shutil.copytree(ROOT / directory, target / directory, ignore=shutil.ignore_patterns("__pycache__"))
        shutil.copyfile(ROOT / "pyproject.toml", target / "pyproject.toml")
        subprocess.run([python, "-I", "-m", "pytest", "tests", "-q"], check=True, cwd=target, env=environment)
        subprocess.run([python, "-m", "pip", "check"], check=True, cwd=target, env=environment)


if __name__ == "__main__":
    main()

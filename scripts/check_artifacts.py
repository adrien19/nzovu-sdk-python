"""Validate installed distributions outside the checkout in isolated environments."""

import argparse
import hashlib
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

SMOKE = """
import importlib
import importlib.metadata
import importlib.util
import pickle
import pkgutil
import sys

import nzovu
import nzovu.api
from importlib.resources import files

assert files("nzovu").joinpath("py.typed").is_file()
from nzovu import AsyncNzovuClient, NzovuClient
from nzovu.api.queueservice.v1 import request_response_pb2, service_pb2

assert nzovu.__version__ == sys.argv[1], nzovu.__version__
assert (importlib.util.find_spec('pydantic') is not None) == (sys.argv[2] == 'True')
distribution = importlib.metadata.distribution('nzovu')
assert distribution.metadata['Name'] == 'nzovu'
for path in distribution.files:
    if path.suffix in {'.py', '.pyi'}:
        assert path.parts[0] == 'nzovu', path
service = service_pb2.DESCRIPTOR.services_by_name['QueueService']
assert service.full_name == 'nzovu.api.queueservice.v1.QueueService'
assert len(service.methods) == 31
for module in pkgutil.walk_packages(nzovu.api.__path__, nzovu.api.__name__ + '.'):
    importlib.import_module(module.name)
message = request_response_pb2.ListQueuesRequest(page_size=10, page_token='next')
assert pickle.loads(pickle.dumps(message)) == message
print('Installed namespace, version, generated imports and pickle identity passed')
"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dist", type=Path, default=Path("dist"))
    expected_version = re.search(
        r'^version = "([^"]+)"', (Path(__file__).resolve().parents[1] / "pyproject.toml").read_text(), re.M
    )
    if expected_version is None:
        raise ValueError("Package version missing")
    parser.add_argument("--version", default=expected_version.group(1))
    parser.add_argument("--profiles", nargs="+", choices=["minimum", "latest"], default=["latest"])
    parser.add_argument("--artifacts", nargs="+", choices=["wheel", "sdist"], default=["wheel", "sdist"])
    parser.add_argument("--extras", nargs="+", choices=["base", "pydantic"], default=["base", "pydantic"])
    parser.add_argument("--run-tests", action="store_true")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    artifacts = {
        "wheel": args.dist.resolve() / f"nzovu-{args.version}-py3-none-any.whl",
        "sdist": args.dist.resolve() / f"nzovu-{args.version}.tar.gz",
    }
    for kind in args.artifacts:
        if not artifacts[kind].is_file():
            parser.error(f"Missing distribution: {artifacts[kind]}")
    environment = dict(os.environ)
    environment.pop("PYTHONPATH", None)
    environment.pop("PYTHONHOME", None)
    records = []
    try:
        for profile in args.profiles:
            for kind in args.artifacts:
                for extra in args.extras:
                    artifact = artifacts[kind]
                    with tempfile.TemporaryDirectory(prefix="nzovu-install-") as temporary:
                        target = Path(temporary)
                        subprocess.run(
                            [sys.executable, "-m", "venv", str(target / "venv")], check=True, env=environment
                        )
                        python = str(target / "venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python"))
                        package = str(artifact) + ("[pydantic]" if extra == "pydantic" else "")
                        requirements = [package]
                        if profile == "minimum":
                            requirements += ["grpcio==1.76.0", "protobuf==5.29.6"]
                            if extra == "pydantic":
                                requirements += ["pydantic==2.12.5"]
                        if args.run_tests:
                            requirements += ["pytest>=9.0.3,<10", "pytest-asyncio>=1.2,<2"]
                        subprocess.run(
                            [python, "-m", "pip", "install", "--disable-pip-version-check", *requirements],
                            cwd=target,
                            check=True,
                            env=environment,
                        )
                        subprocess.run(
                            [python, "-I", "-c", SMOKE, args.version, str(extra == "pydantic")],
                            cwd=target,
                            check=True,
                            env=environment,
                        )
                        subprocess.run([python, "-m", "pip", "check"], cwd=target, check=True, env=environment)
                        versions = subprocess.check_output([python, "-m", "pip", "list", "--format=json"], text=True)
                        record = dict(
                            python=platform.python_version(),
                            platform=platform.platform(),
                            artifact=artifact.name,
                            profile=profile,
                            extra=extra,
                            packages=json.loads(versions),
                        )
                        record["sha256"] = hashlib.sha256(artifact.read_bytes()).hexdigest()
                        if environment.get("NZOVU_LIVE_CONFIG"):
                            fixture = json.loads(Path(environment["NZOVU_LIVE_CONFIG"]).read_text())
                            record["server_source"] = fixture["source"]["commit"]
                            record["server_build"] = fixture.get("server_build", "")
                            record["server_label"] = fixture.get("server_label", "pinned")
                        records.append(record)
                        if args.run_tests:
                            for directory in ("tests", "proto", "scripts"):
                                shutil.copytree(
                                    root / directory, target / directory, ignore=shutil.ignore_patterns("__pycache__")
                                )
                            shutil.copyfile(root / "pyproject.toml", target / "pyproject.toml")
                            # Source/descriptor regeneration runs once with the pinned compiler in the quality job.
                            subprocess.run(
                                [
                                    python,
                                    "-I",
                                    "-m",
                                    "pytest",
                                    "tests",
                                    "-v",
                                    "-k",
                                    "not test_generated_descriptors_match_pinned_sources",
                                ],
                                cwd=target,
                                check=True,
                                env=environment,
                            )
                        record["passed"] = True
                        print(f"{artifact.name}, {profile}, {extra}: PASS", flush=True)
    finally:
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(records, indent=2) + "\n")


if __name__ == "__main__":
    main()

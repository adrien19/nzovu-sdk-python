"""Validate installed distributions outside the checkout in isolated environments."""

import argparse
import os
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
    parser.add_argument("--version", required=True)
    args = parser.parse_args()
    artifacts = args.dist.resolve()
    wheel = artifacts / f"nzovu-{args.version}-py3-none-any.whl"
    sdist = artifacts / f"nzovu-{args.version}.tar.gz"
    for artifact in (wheel, sdist):
        if not artifact.is_file():
            parser.error(f"Missing distribution: {artifact}")
    environment = dict(os.environ)
    environment.pop("PYTHONPATH", None)
    environment.pop("PYTHONHOME", None)
    for artifact, extra in ((wheel, False), (wheel, True), (sdist, False)):
        with tempfile.TemporaryDirectory(prefix="nzovu-install-") as temporary:
            target = Path(temporary)
            subprocess.run([sys.executable, "-m", "venv", str(target / "venv")], check=True, env=environment)
            python = target / "venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
            package = str(artifact) + ("[pydantic]" if extra else "")
            subprocess.run(
                [str(python), "-m", "pip", "install", "--disable-pip-version-check", package],
                cwd=target,
                check=True,
                env=environment,
            )
            subprocess.run(
                [str(python), "-I", "-c", SMOKE, args.version, str(extra)],
                cwd=target,
                check=True,
                env=environment,
            )
            subprocess.run([str(python), "-m", "pip", "check"], cwd=target, check=True, env=environment)
            print(f"{artifact.name}, Pydantic={extra}: PASS", flush=True)


if __name__ == "__main__":
    main()

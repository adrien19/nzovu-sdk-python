"""Run SDK-PR3 gates against a local Nzovu binary (SQLite, TLS and mTLS)."""

import argparse
import json
import os
import socket
import subprocess
import tempfile
import time
from pathlib import Path


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def run(*args):
    subprocess.run(args, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)


def certificate(directory, name, ca=None, server=False):
    base = directory / name
    run(
        "openssl",
        "req",
        "-new",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        str(base) + ".key",
        "-out",
        str(base) + ".csr",
        "-subj",
        f"/CN={name}",
    )
    if ca is None:
        config = base.with_suffix(".cnf")
        config.write_text(
            "[req]\ndistinguished_name=dn\nx509_extensions=ca\n[dn]\n[ca]\n"
            "basicConstraints=critical,CA:TRUE\nkeyUsage=critical,keyCertSign,cRLSign,digitalSignature\n"
        )
        run(
            "openssl",
            "req",
            "-x509",
            "-config",
            str(config),
            "-new",
            "-key",
            str(base) + ".key",
            "-days",
            "1",
            "-out",
            str(base) + ".crt",
            "-subj",
            f"/CN={name}",
        )
    else:
        extensions = base.with_suffix(".ext")
        extensions.write_text(
            "basicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\n"
            + (
                "subjectAltName=DNS:localhost,DNS:host.docker.internal,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n"
                if server
                else "extendedKeyUsage=clientAuth\n"
            )
        )
        run(
            "openssl",
            "x509",
            "-req",
            "-in",
            str(base) + ".csr",
            "-CA",
            str(directory / ca) + ".crt",
            "-CAkey",
            str(directory / ca) + ".key",
            "-CAcreateserial",
            "-days",
            "1",
            "-out",
            str(base) + ".crt",
            "-extfile",
            str(extensions),
        )
    os.chmod(str(base) + ".key", 0o600)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-binary", required=True, type=Path)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--temp-root", type=Path)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if not args.command:
        parser.error("provide the pytest command after --")
    directory = Path(tempfile.mkdtemp(prefix="nzovu-sdk3-live-", dir=args.temp_root)).resolve()
    processes, logs = [], []
    try:
        certificate(directory, "ca")
        certificate(directory, "wrong-ca")
        certificate(directory, "server", "ca", server=True)
        certificate(directory, "client", "ca")
        certificate(directory, "wrong-client", "wrong-ca")
        config = {"host": args.host, "directory": str(directory), "api_key": "sdk-fixture-key"}
        for mode in ("tls", "mtls"):
            grpc_port, http_port = port(), port()
            config[mode] = grpc_port
            command = [
                str(args.server_binary.resolve()),
                "server",
                "--dev",
                "--database",
                str(directory / f"{mode}.db"),
                "--grpc-addr",
                f":{grpc_port}",
                "--http-addr",
                f"127.0.0.1:{http_port}",
                "--enable-tls",
                "--cert-file",
                str(directory / "server.crt"),
                "--key-file",
                str(directory / "server.key"),
                "--gateway-insecure",
                "--metrics-enabled=false",
                "--log-level",
                "error",
            ]
            if mode == "mtls":
                command += [
                    "--ca-cert-file",
                    str(directory / "ca.crt"),
                    "--gateway-client-cert",
                    str(directory / "client.crt"),
                    "--gateway-client-key",
                    str(directory / "client.key"),
                ]
            log = (directory / f"{mode}.log").open("w")
            logs.append(log)
            env = dict(os.environ, AUTH_ENABLED="true", API_KEYS=config["api_key"])
            process = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT)
            processes.append(process)
            deadline = time.monotonic() + 20
            while True:
                if process.poll() is not None:
                    raise RuntimeError(f"Nzovu {mode} exited; inspect {directory / (mode + '.log')}")
                try:
                    with socket.create_connection(("localhost", grpc_port), timeout=0.2):
                        break
                except OSError:
                    if time.monotonic() > deadline:
                        raise TimeoutError(f"Nzovu {mode} did not start")
                    time.sleep(0.1)
        config_file = directory / "config.json"
        config_file.write_text(json.dumps(config))
        command = args.command[1:] if args.command[0] == "--" else args.command
        print(f"Live fixture logs: {directory}", flush=True)
        subprocess.run(command, env=dict(os.environ, NZOVU_LIVE_CONFIG=str(config_file)), check=True)
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for log in logs:
            log.close()


if __name__ == "__main__":
    main()

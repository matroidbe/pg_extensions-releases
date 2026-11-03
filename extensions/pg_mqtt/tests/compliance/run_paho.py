#!/usr/bin/env python3
"""Run the Eclipse Paho MQTT interoperability suites against pg_mqtt and
check the result against a support profile.

The suites (eclipse-paho/paho.mqtt.testing: interoperability/client_test5.py
for MQTT 5, client_test.py for MQTT 3.1.1) are not vendored: clone the
repository at the commit in profile.toml and pass its `interoperability`
directory with --suite.

Each test runs in its own process with a time limit: a test of a feature the
broker lacks can otherwise hang the whole run (MQTT 5's test_flow_control2
publishes up to 65,536 QoS 2 messages and waits for a DISCONNECT).

The profile lists every test of each suite as `required` or `exempt` (with the
reason). The run fails when a required test fails, when a test is missing from
the profile, or when the profile names a test the suite does not have. An
exempt test that passes is reported, so its exemption can be dropped.

    python3 run_paho.py --suite ~/src/paho.mqtt.testing/interoperability --port 41883
"""

import argparse
import ast
import pathlib
import subprocess
import sys
import time
import tomllib

HERE = pathlib.Path(__file__).resolve().parent

# Runs one test of a suite script. The scripts read their options with getopt
# but then call unittest.main(), which reads the same argv and rejects them; so
# getopt gets the options and unittest only the test name. The host goes as
# --hostname=: the scripts test `o in ("--help")`, a substring test that -h
# matches, and then print usage and exit 0.
LAUNCHER = """
import runpy, sys, unittest
script, host, port, test = sys.argv[1:5]
sys.path.insert(0, ".")
real_main = unittest.main
unittest.main = lambda *a, **k: real_main(*a, argv=[script, test], **k)
sys.argv = [script, "--hostname=" + host, "-p", port]
runpy.run_path(script, run_name="__main__")
"""


def suite_tests(script: pathlib.Path) -> list[str]:
    """Names of the test methods of the script's Test class, in order"""
    tree = ast.parse(script.read_text())
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == "Test":
            return [
                f.name
                for f in node.body
                if isinstance(f, ast.FunctionDef) and f.name.startswith("test")
            ]
    raise SystemExit(f"{script.name} has no Test class")


def run_test(
    suite: pathlib.Path, script: str, host: str, port: int, name: str, timeout: float
) -> tuple[str, float, str]:
    """Run one test; returns (ok|fail|timeout, seconds, output tail)"""
    start = time.monotonic()
    try:
        proc = subprocess.run(
            [sys.executable, "-c", LAUNCHER, script, host, str(port), f"Test.{name}"],
            cwd=suite,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as e:
        err = e.stderr.decode(errors="replace") if isinstance(e.stderr, bytes) else (e.stderr or "")
        return "timeout", time.monotonic() - start, err[-600:]
    output = (proc.stdout or "") + (proc.stderr or "")
    # Only a test unittest actually ran and passed counts
    ran = "Ran 1 test" in output and "\nOK" in output
    status = "ok" if proc.returncode == 0 and ran else "fail"
    return status, time.monotonic() - start, output[-800:]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--suite", required=True, type=pathlib.Path,
                        help="the suites' interoperability directory")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--profile", type=pathlib.Path, default=HERE / "profile.toml")
    parser.add_argument("--timeout", type=float, default=60.0,
                        help="seconds per test (default 60)")
    parser.add_argument("--only-suite", help="run one suite of the profile (e.g. mqtt311)")
    parser.add_argument("--only", nargs="*", help="run only these tests")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="print the output of failing tests")
    args = parser.parse_args()

    suite_dir = args.suite.expanduser()
    profile = tomllib.loads(args.profile.read_text())
    problems = []
    promotable = []

    for suite_name, suite in profile["suites"].items():
        if args.only_suite and suite_name != args.only_suite:
            continue
        script = suite["script"]
        expected = suite["tests"]
        tests = suite_tests(suite_dir / script)
        print(f"== {suite_name} ({script})")

        unknown = sorted(set(tests) - set(expected))
        stale = sorted(set(expected) - set(tests))
        if unknown:
            problems.append(f"{suite_name}: not in the profile: {', '.join(unknown)}")
        if stale:
            problems.append(f"{suite_name}: in the profile but not in the suite: {', '.join(stale)}")

        width = max(len(t) for t in tests)
        for name in tests:
            if args.only and name not in args.only:
                continue
            entry = expected.get(name, {"status": "required"})
            status, seconds, tail = run_test(
                suite_dir, script, args.host, args.port, name, args.timeout
            )
            verdict = entry["status"]
            if verdict == "required" and status != "ok":
                mark = "FAIL"
                problems.append(f"{suite_name}: {name}: required, {status}")
            elif verdict == "exempt" and status == "ok":
                mark = "pass (exempt)"
                promotable.append(f"{suite_name}: {name}")
            elif verdict == "exempt":
                mark = f"exempt ({status})"
            else:
                mark = "pass"
            print(f"{name:<{width}}  {mark:<16} {seconds:5.1f}s", flush=True)
            if args.verbose and status != "ok":
                print("    " + tail.strip().replace("\n", "\n    "))
        print()

    if promotable:
        print(f"exempt but passing (drop the exemption?): {', '.join(promotable)}")
    if problems:
        print("NOT conforming to the profile:")
        for p in problems:
            print(f"  - {p}")
        return 1
    print(f"conforms to profile '{profile['profile']['name']}'")
    return 0


if __name__ == "__main__":
    sys.exit(main())

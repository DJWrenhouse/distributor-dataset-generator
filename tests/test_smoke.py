"""Smoke test coverage for streamed full-generation execution.

The goal is to confirm the CLI runs end-to-end with small inputs and produces
at least one expected output artifact.
"""

import subprocess
import sys


def test_smoke_streamed(tmp_path):
    """Run a tiny streamed generation job and validate output presence.

    Args:
        tmp_path: Pytest-provided temporary directory fixture.

    Asserts:
        The generator exits successfully and creates files related to orders.
    """
    out = tmp_path / "out"
    out.mkdir()
    inline = (
        "import sys; "
        "import data_generator.config as c; "
        "c.N_VENDORS=20; c.N_DCS=3; c.N_CUSTOMERS=100; c.N_ORDERS=100; "
        "import data_generator.generate_all as g; "
        "sys.argv=["
        "'generate_all',"
        "'--n-orders','100',"
        "'--seed','1',"
        "'--output-dir',r'" + str(out) + "',"
        "'--stream','--chunk-size','20',"
        "'--assume-yes','--no-confirm'"
        "]; "
        "g.main()"
    )
    cmd = [
        sys.executable,
        "-c",
        inline,
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    print(res.stdout)
    print(res.stderr)
    assert res.returncode == 0
    # basic output checks
    # either CSV files or parquet parts should exist
    files = list(out.iterdir())
    assert any(f.name.startswith("orders") for f in files)

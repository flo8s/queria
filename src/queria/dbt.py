"""dbt execution helpers."""

import os
from contextlib import contextmanager
from pathlib import Path

from dbt.cli.main import dbtRunner


@contextmanager
def working_directory(path: Path):
    """Context manager to temporarily change the current working directory.

    dbt must run with transform/ as the working directory because
    profiles.yml uses relative paths (ducklake:ducklake.duckdb).
    """
    prev = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(prev)


def run_dbt(transform_dir: Path, args: list[str]) -> None:
    """Run a dbt command inside the transform/ directory."""
    with working_directory(transform_dir):
        result = dbtRunner().invoke(args)
        if not result.success:
            raise RuntimeError(f"dbt {' '.join(args)} failed")

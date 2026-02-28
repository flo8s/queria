"""queria CLI entry point."""

from pathlib import Path
from typing import Optional

import typer
from typer.main import Typer

app = typer.Typer()


@app.callback()
def callback() -> None:
    """queria: dbt + DuckLake data pipeline CLI"""


@app.command()
def init(
    path: Path = typer.Argument(..., help="Path to the dataset directory to create"),
) -> None:
    """Scaffold a new dataset"""
    from queria.init import init_dataset

    init_dataset(path.resolve())


@app.command()
def fetch(
    path: Path = typer.Argument(..., help="Path to the dataset directory"),
    bucket: str = typer.Option(..., envvar="S3_BUCKET", help="S3 bucket name"),
) -> None:
    """Fetch ducklake.duckdb from S3"""
    from queria.fetch import fetch_datasource

    fetch_datasource(path.resolve(), bucket=bucket)


@app.command()
def ingest(
    path: Path = typer.Argument(..., help="Path to the dataset directory"),
) -> None:
    """Run dataset-specific ingestion script"""
    script = path.resolve() / "ingestion" / "__main__.py"
    if not script.exists():
        return
    import subprocess
    import sys

    subprocess.run(
        [sys.executable, str(script.parent)],
        check=True,
    )


@app.command()
def transform(
    path: Path = typer.Argument(..., help="Path to the dataset directory"),
    target: str = typer.Option("dev", help="dbt target (defined in profiles.yml)"),
    vars: Optional[str] = typer.Option(None, help="dbt vars (JSON string)"),
) -> None:
    """Transform a dataset"""
    script = path.resolve() / "transform" / "__main__.py"
    if script.exists():
        import subprocess
        import sys

        subprocess.run(
            [sys.executable, str(script.parent)],
            check=True,
        )
        return

    from queria.run import build_datasource

    build_datasource(path.resolve(), target, dbt_vars=vars)


@app.command()
def run(
    path: Path = typer.Argument(..., help="Path to the dataset directory"),
    target: str = typer.Option("dev", help="dbt target (defined in profiles.yml)"),
    vars: Optional[str] = typer.Option(None, help="dbt vars (JSON string)"),
) -> None:
    """Build a dataset (ingest + transform)"""
    ingest(path)
    transform(path, target, vars)


@app.command()
def freeze(
    path: Path = typer.Argument(..., help="Path to the dataset directory"),
    bucket: Optional[str] = typer.Option(
        None, envvar="S3_BUCKET", help="S3 bucket name"
    ),
    output_dir: Optional[Path] = typer.Option(None, help="Local output directory"),
) -> None:
    """Deploy build artifacts"""
    from queria.freeze import freeze_datasource

    if not bucket and not output_dir:
        typer.echo("Error: specify --bucket or --output-dir", err=True)
        raise typer.Exit(1)

    freeze_datasource(path.resolve(), bucket=bucket, output_dir=output_dir)


@app.command()
def gc(
    path: Path = typer.Argument(..., help="Path to the dataset directory"),
    bucket: str = typer.Option(..., envvar="S3_BUCKET", help="S3 bucket name"),
    force: bool = typer.Option(False, help="Skip confirmation prompt"),
    older_than_days: Optional[int] = typer.Option(
        None, help="Only delete files older than N days"
    ),
) -> None:
    """Clean up orphaned Parquet files on R2"""
    from queria.gc import gc_datasource

    gc_datasource(
        path.resolve(), bucket=bucket, force=force, older_than_days=older_than_days
    )


main: Typer = app

if __name__ == "__main__":
    main()

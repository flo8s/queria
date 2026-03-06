"""queria CLI entry point."""

from pathlib import Path
from typing import Optional

import typer
from typer.main import Typer

from queria.gc import gc_datasource
from queria.ingestion import has_ingestion_script, ingest_datasource
from queria.init import init_dataset
from queria.pull import pull_datasource
from queria.push import push_datasource
from queria.transform import build_datasource

app = typer.Typer()


@app.callback()
def callback() -> None:
    """queria: dbt + DuckLake data pipeline CLI"""


@app.command()
def new(
    path: Path = typer.Argument(..., help="Path to the dataset directory to create"),
) -> None:
    """Scaffold a new dataset"""
    init_dataset(path.resolve())


@app.command()
def pull(
    bucket: str = typer.Option(..., envvar="S3_BUCKET", help="S3 bucket name"),
) -> None:
    """Pull ducklake.duckdb from S3"""
    pull_datasource(Path.cwd(), bucket=bucket)


@app.command()
def ingest() -> None:
    """Run dataset-specific ingestion script"""
    ingest_datasource(Path.cwd())


@app.command()
def transform(
    target: str = typer.Option("dev", help="dbt target (defined in profiles.yml)"),
    vars: Optional[str] = typer.Option(None, help="dbt vars (JSON string)"),
) -> None:
    """Transform a dataset"""
    build_datasource(Path.cwd(), target, dbt_vars=vars)


@app.command()
def run(
    target: str = typer.Option("dev", help="dbt target (defined in profiles.yml)"),
    vars: Optional[str] = typer.Option(None, help="dbt vars (JSON string)"),
) -> None:
    """Build a dataset (ingest + transform)"""
    cwd = Path.cwd()
    if has_ingestion_script(cwd):
        ingest()
    transform(target, vars)


@app.command()
def push(
    bucket: Optional[str] = typer.Option(
        None, envvar="S3_BUCKET", help="S3 bucket name"
    ),
    output_dir: Optional[Path] = typer.Option(None, help="Local output directory"),
) -> None:
    """Push build artifacts to S3 or local directory"""
    if not bucket and not output_dir:
        typer.echo("Error: specify --bucket or --output-dir", err=True)
        raise typer.Exit(1)

    push_datasource(Path.cwd(), bucket=bucket, output_dir=output_dir)


@app.command()
def gc(
    bucket: str = typer.Option(..., envvar="S3_BUCKET", help="S3 bucket name"),
    force: bool = typer.Option(False, help="Skip confirmation prompt"),
    older_than_days: Optional[int] = typer.Option(
        None, help="Only delete files older than N days"
    ),
) -> None:
    """Clean up orphaned Parquet files on R2"""
    gc_datasource(
        Path.cwd(), bucket=bucket, force=force, older_than_days=older_than_days
    )


main: Typer = app

if __name__ == "__main__":
    main()

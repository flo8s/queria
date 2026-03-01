"""Sync catalog data from DuckLake (R2) to Cloudflare D1.

DuckDB で R2 上の catalog DuckLake に接続し、mart テーブルから
D1 用の SQL を生成して Cloudflare Python SDK で実行する。

Usage:
    uv run --with cloudflare python scripts/sync_catalog_d1.py
    uv run --with cloudflare python scripts/sync_catalog_d1.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import urllib.request

import duckdb

R2_PUBLIC_URL = "https://data.queria.io"
CATALOG_DUCKLAKE_URL = f"{R2_PUBLIC_URL}/catalog/ducklake.duckdb"
CATALOG_ALIAS = "catalog"

D1_TABLES = [
    "catalog_datasets",
    "catalog_schemas",
    "catalog_tables",
    "catalog_columns",
    "catalog_dependencies",
]


# --- SQL helpers ---


def esc_sql(s: str) -> str:
    return s.replace("'", "''")


def sql_val(v: object) -> str:
    if v is None:
        return "NULL"
    return f"'{esc_sql(str(v))}'"


def json_val(v: object) -> str:
    if v is None:
        return "NULL"
    return f"'{esc_sql(json.dumps(v, ensure_ascii=False))}'"


def bool_val(v: object) -> str:
    return "1" if v is True else "0"


def build_insert(table: str, row: dict[str, str]) -> str:
    cols = ", ".join(row.keys())
    vals = ", ".join(row.values())
    return f"INSERT OR REPLACE INTO {table} ({cols}) VALUES ({vals});"


# --- SQL generation ---


def generate_catalog_sql() -> str:
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL ducklake; LOAD ducklake;")
    conn.execute("SET http_retries = 10")
    conn.execute("SET http_retry_wait_ms = 1000")
    conn.execute("SET http_retry_backoff = 2.0")
    conn.execute(
        f"ATTACH 'ducklake:{CATALOG_DUCKLAKE_URL}' AS {CATALOG_ALIAS} (READ_ONLY)"
    )

    statements: list[str] = [f"DELETE FROM {t};" for t in D1_TABLES]

    # --- Datasets ---
    datasets = conn.execute(
        f"""
        SELECT datasource, title, description, cover, ducklake_url, tags_json
        FROM {CATALOG_ALIAS}.main.mart_datasets
        ORDER BY datasource
        """
    ).fetchall()
    ds_columns = ["datasource", "title", "description", "cover", "ducklake_url", "tags_json"]
    existing_datasources: set[str] = set()

    for row in datasets:
        r = dict(zip(ds_columns, row))
        existing_datasources.add(str(r["datasource"]))
        tags = json.loads(str(r["tags_json"])) if r["tags_json"] else None
        statements.append(
            build_insert(
                "catalog_datasets",
                {
                    "datasource": sql_val(r["datasource"]),
                    "title": sql_val(r["title"]),
                    "description": sql_val(r["description"]),
                    "cover": sql_val(r["cover"]),
                    "ducklake_url": sql_val(r["ducklake_url"]),
                    "tags": json_val(tags),
                },
            )
        )

    # --- Dependencies ---
    deps = conn.execute(
        f"""
        SELECT datasource, alias, ducklake_url
        FROM {CATALOG_ALIAS}.main.mart_dependencies
        ORDER BY datasource, alias
        """
    ).fetchall()
    for row in deps:
        datasource, alias, ducklake_url = row
        dep_id = f"{datasource}/{alias}"
        statements.append(
            build_insert(
                "catalog_dependencies",
                {
                    "id": sql_val(dep_id),
                    "datasource": sql_val(datasource),
                    "alias": sql_val(alias),
                    "ducklake_url": sql_val(ducklake_url),
                },
            )
        )

    # --- Schemas ---
    schemas = conn.execute(
        f"""
        SELECT datasource, schema_name, title
        FROM {CATALOG_ALIAS}.main.mart_schemas
        ORDER BY datasource, schema_name
        """
    ).fetchall()
    for row in schemas:
        datasource, schema_name, title = row
        schema_id = f"{datasource}/{schema_name}"
        statements.append(
            build_insert(
                "catalog_schemas",
                {
                    "id": sql_val(schema_id),
                    "datasource": sql_val(datasource),
                    "schema_name": sql_val(schema_name),
                    "title": sql_val(title or ""),
                },
            )
        )

    # --- Tables ---
    tables = conn.execute(
        f"""
        SELECT datasource, node_id, name, schema_name, description,
               materialized AS type, title, license, license_url,
               source_url, is_published, tags_json, sql
        FROM {CATALOG_ALIAS}.main.mart_tables
        ORDER BY datasource, node_index
        """
    ).fetchall()
    tbl_columns = [
        "datasource", "node_id", "name", "schema_name", "description",
        "type", "title", "license", "license_url", "source_url",
        "is_published", "tags_json", "sql",
    ]
    for row in tables:
        r = dict(zip(tbl_columns, row))
        table_id = f"{r['datasource']}/{r['schema_name']}/{r['name']}"
        tags = json.loads(str(r["tags_json"])) if r["tags_json"] else None
        statements.append(
            build_insert(
                "catalog_tables",
                {
                    "id": sql_val(table_id),
                    "datasource": sql_val(r["datasource"]),
                    "schema_name": sql_val(r["schema_name"]),
                    "name": sql_val(r["name"]),
                    "title": sql_val(r["title"] or ""),
                    "description": sql_val(r["description"] or ""),
                    "type": sql_val(r["type"]),
                    "license": sql_val(r["license"]),
                    "license_url": sql_val(r["license_url"]),
                    "source_url": sql_val(r["source_url"]),
                    "is_published": bool_val(r["is_published"]),
                    "tags": json_val(tags),
                    "sql": sql_val(r["sql"]),
                },
            )
        )

    # --- Columns ---
    columns = conn.execute(
        f"""
        SELECT t.datasource, t.schema_name, t.name AS table_name,
               c.node_id, c.column_name, c.title, c.description,
               c.data_type, c.column_index
        FROM {CATALOG_ALIAS}.main.mart_columns c
        JOIN {CATALOG_ALIAS}.main.mart_tables t
          ON c.node_id = t.node_id AND c.datasource = t.datasource
        ORDER BY c.datasource, c.column_index
        """
    ).fetchall()
    col_columns = [
        "datasource", "schema_name", "table_name", "node_id",
        "column_name", "title", "description", "data_type", "column_index",
    ]
    for row in columns:
        r = dict(zip(col_columns, row))
        col_id = f"{r['datasource']}/{r['schema_name']}/{r['table_name']}/{r['column_name']}"
        statements.append(
            build_insert(
                "catalog_columns",
                {
                    "id": sql_val(col_id),
                    "datasource": sql_val(r["datasource"]),
                    "schema_name": sql_val(r["schema_name"]),
                    "table_name": sql_val(r["table_name"]),
                    "column_name": sql_val(r["column_name"]),
                    "title": sql_val(r["title"]),
                    "description": sql_val(r["description"] or ""),
                    "data_type": sql_val(r["data_type"]),
                    "column_index": str(r["column_index"] or 0),
                },
            )
        )

    conn.close()

    # --- catalog 自体のメタデータ (R2 metadata.json) ---
    metadata_url = f"{R2_PUBLIC_URL}/catalog/metadata.json"
    req = urllib.request.Request(metadata_url, headers={"User-Agent": "queria-sync"})
    with urllib.request.urlopen(req) as resp:
        meta = json.loads(resp.read())

    if CATALOG_ALIAS not in existing_datasources:
        meta_tags = meta.get("tags") or None
        if meta_tags and len(meta_tags) == 0:
            meta_tags = None
        statements.append(
            build_insert(
                "catalog_datasets",
                {
                    "datasource": sql_val(CATALOG_ALIAS),
                    "title": sql_val(meta["title"]),
                    "description": sql_val(meta["description"]),
                    "cover": sql_val(meta.get("cover")),
                    "ducklake_url": sql_val(meta["ducklake_url"]),
                    "tags": json_val(meta_tags),
                },
            )
        )

        for schema_name, schema in meta.get("schemas", {}).items():
            schema_id = f"{CATALOG_ALIAS}/{schema_name}"
            statements.append(
                build_insert(
                    "catalog_schemas",
                    {
                        "id": sql_val(schema_id),
                        "datasource": sql_val(CATALOG_ALIAS),
                        "schema_name": sql_val(schema_name),
                        "title": sql_val(schema["title"]),
                    },
                )
            )

            for t in schema.get("tables", []):
                table_id = f"{CATALOG_ALIAS}/{schema_name}/{t['name']}"
                t_tags = t.get("tags") or None
                if t_tags and len(t_tags) == 0:
                    t_tags = None
                statements.append(
                    build_insert(
                        "catalog_tables",
                        {
                            "id": sql_val(table_id),
                            "datasource": sql_val(CATALOG_ALIAS),
                            "schema_name": sql_val(schema_name),
                            "name": sql_val(t["name"]),
                            "title": sql_val(t["title"]),
                            "description": sql_val(t["description"]),
                            "type": sql_val(t.get("materialized")),
                            "license": sql_val(t.get("license")),
                            "license_url": sql_val(t.get("license_url")),
                            "source_url": sql_val(t.get("source_url")),
                            "is_published": bool_val(t.get("published")),
                            "tags": json_val(t_tags),
                            "sql": sql_val(t.get("sql")),
                        },
                    )
                )

                for i, c in enumerate(t.get("columns", [])):
                    col_id = f"{CATALOG_ALIAS}/{schema_name}/{t['name']}/{c['name']}"
                    statements.append(
                        build_insert(
                            "catalog_columns",
                            {
                                "id": sql_val(col_id),
                                "datasource": sql_val(CATALOG_ALIAS),
                                "schema_name": sql_val(schema_name),
                                "table_name": sql_val(t["name"]),
                                "column_name": sql_val(c["name"]),
                                "title": sql_val(c.get("title")),
                                "description": sql_val(c.get("description", "")),
                                "data_type": sql_val(c.get("data_type")),
                                "column_index": str(i),
                            },
                        )
                    )

    return "\n".join(["-- Catalog data", *statements])


# --- D1 execution ---


def execute_d1(sql: str) -> None:
    from cloudflare import Cloudflare

    client = Cloudflare(api_token=os.environ["CF_API_TOKEN"])
    account_id = os.environ["CF_ACCOUNT_ID"]
    database_id = os.environ["CF_D1_DATABASE_ID"]

    stmts = [s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("--")]
    chunk_size = 200

    for i in range(0, len(stmts), chunk_size):
        chunk = stmts[i : i + chunk_size]
        batch_sql = "; ".join(chunk) + ";"
        client.d1.database.query(
            database_id=database_id,
            account_id=account_id,
            sql=batch_sql,
        )
        print(f"  executed {len(chunk)} statements ({i + 1}-{i + len(chunk)})")


# --- CLI ---


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync catalog to Cloudflare D1")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate SQL and print to stdout without executing",
    )
    args = parser.parse_args()

    print("Generating catalog SQL...")
    sql = generate_catalog_sql()

    if args.dry_run:
        print(sql)
        return

    print("Executing SQL on D1...")
    execute_d1(sql)
    print("Done.")


if __name__ == "__main__":
    main()

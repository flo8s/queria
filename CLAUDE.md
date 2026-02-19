# queria 開発ガイド

## Frozen DuckLake の DATA_PATH 設計

duckdb-wasm からは絶対 HTTP URL でしか Parquet ファイルを参照できない。
そのため DATA_PATH を空文字 ('') にしてはいけない。

フロー:
1. init_ducklake: dataset.yml の ducklake_url から公開 URL を導出し DATA_PATH に設定
   - 例: `https://pub-xxx.r2.dev/tsukuba/ducklake.duckdb.files/`
2. dbt run (prd target): 環境変数 OVERRIDE_DATA_PATH で S3 書き込み先に一時的に上書き
   - OVERRIDE_DATA_PATH はセッション限定でメタデータの data_path を変更しない

ducklake_url の正規ソースは各データセットの `dataset.yml` ファイル。

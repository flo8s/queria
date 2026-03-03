# e-Stat データパイプライン: Iceberg 検討と DuckLake カタログ変換の知見

調査日: 2026-03-03

## 概要

e_stat の ingestion パイプライン（dlt[ducklake]）について、dlt[iceberg] への移行を検討し不採用と判断した。
また DuckLake カタログの SQLite → DuckDB 変換について COPY FROM DATABASE の適用可否を検証し、
NOT NULL 制約の差異により使用不可であることを確認した。

## 1. Iceberg 採用検討

### 検討動機

現行の dlt[ducklake] には以下の課題がある:

- OVERRIDE_DATA_PATH のモンキーパッチ（ducklake_patch.py）が dlt バージョンアップで壊れるリスク
- DuckLake の SQLite カタログがファイルロックを長時間保持し、テーブルごとに subprocess 分離が必要

Iceberg はロック粒度が細かく（メタデータコミット時のみ）、dlt[iceberg] に移行すれば
モンキーパッチと subprocess 分離の両方を解消できる可能性があった。

### Iceberg のメタデータ構造

Iceberg テーブルは以下のファイルで構成される:

```
table/
  metadata/
    v1.metadata.json    # テーブル定義・スナップショット情報（不変）
    v2.metadata.json    # 書き込みのたびに新バージョンが作られる
    snap-*.avro         # スナップショットマニフェスト
  data/
    *.parquet           # 実データ
```

書き込み時、「現在のバージョンはどれか」のアトミックな更新が必要で、
これを担うのがカタログ（REST, SQL, Hadoop 等）。

### サーバーレス運用の可否

カタログサーバーなしで運用できるかを調査した:

| カタログ種別 | サーバー不要 | PyIceberg 対応 | 備考 |
|---|---|---|---|
| Hadoop catalog | ファイルシステムの atomic rename で管理 | 未実装・対応予定なし | apache/iceberg-python#17 が "not planned" でクローズ |
| SQL catalog (SQLite) | SQLite ファイルのみ | 対応 | 並行アクセス非対応 |
| REST catalog | サーバー必要 | 対応 | GitHub Actions CI で非現実的 |

dlt[iceberg] は PyIceberg に依存しており、PyIceberg は Hadoop catalog を実装していない。
メンテナーは「HDFS の atomic rename に依存する実装は他のカタログと異なるため避けたい」と明言しており、
今後も対応予定はない。

サーバーレスで使える最軽量のカタログは SQLite catalog のみ。

### DuckLake との比較

| 観点 | dlt[ducklake] (現行) | dlt[iceberg] (検討案) |
|---|---|---|
| カタログ | SQLite（ファイル） | SQLite（ファイル）← 結局同じ |
| ロック粒度 | データロード全体 | メタデータコミット時のみ |
| duckdb-wasm 配信 | 直接（DuckLake がそのまま配信フォーマット） | Iceberg → DuckLake 変換が必要 |
| データコピー回数 | 1回（API → DuckLake） | 2回（API → Iceberg → DuckLake） |
| モンキーパッチ | 必要（OVERRIDE_DATA_PATH） | 不要 |

### 不採用理由

Iceberg でもサーバーレス運用には SQLite catalog が必要で、カタログ管理の負担は DuckLake と同等。
その上で Iceberg → DuckLake のデータコピーが追加で必要になり、
ロック粒度の改善（22 テーブルの逐次実行では実用上問題なし）に対してコストが見合わない。

dlt[ducklake] のモンキーパッチは dlt バージョン固定 + テスト 7 ケースで管理可能であり、
DuckLake 1.0（2026 年前半目標）で OVERRIDE_DATA_PATH がネイティブサポートされれば解消する見込み。

## 2. DuckLake カタログ変換: COPY FROM DATABASE の適用可否

### 背景

dlt[ducklake] は SQLite 形式のカタログを作成する。
duckdb-wasm は DuckDB 形式のカタログが必要なため、SQLite → DuckDB の変換処理がある。

現行の変換処理は DuckLake の内部テーブル（22 個）をループしてコピーしており、
DuckDB の COPY FROM DATABASE で簡略化できないか検証した。

### 検証結果

COPY FROM DATABASE 自体は動作する。SQLite → DuckDB のスキーマコピーで
NOT NULL 制約や PRIMARY KEY も正しく転写されることを確認した。
変換後のファイルを DuckLake としてアタッチし、データの読み取りも成功した。

ただし、実際の DuckLake カタログでは使用不可:

- DuckLake は DuckDB 形式カタログに NOT NULL 制約を設定するが、SQLite 形式には設定しない
- COPY FROM DATABASE はソースのスキーマを忠実にコピーするため、
  NOT NULL がない SQLite からコピーすると DuckDB 側にも NOT NULL が設定されない
- duckdb-wasm は NOT NULL 制約がないカタログからは 0 行を返す

### 現行方式が必要な理由

```python
# 1. DuckDB 形式の空カタログを作成（NOT NULL 制約付きの正しいスキーマ）
ATTACH 'ducklake:{tmp_file}' AS dst (DATA_PATH '...')
DETACH dst

# 2. 両カタログを通常の DB として開いてデータのみコピー
ATTACH '{sqlite_file}' AS src (TYPE sqlite)
ATTACH '{tmp_file}' AS dst
for table in tables:
    DELETE FROM dst.main.{table}
    INSERT INTO dst.main.{table} SELECT * FROM src.main.{table}
```

DuckDB 形式の空カタログを先に作成してスキーマ（NOT NULL 制約含む）を確保し、
SQLite からはデータのみを INSERT する 2 段階方式が必要。
COPY FROM DATABASE はスキーマごとコピーするため、この要件を満たせない。

### information_schema の注意点

DuckDB の information_schema.columns は SQLite アタッチ時に
PRIMARY KEY の暗黙的 NOT NULL を `is_nullable='YES'` と誤報告する。
明示的な NOT NULL は正しく報告される。

COPY FROM DATABASE は information_schema とは別のコードパスで
SQLite スキーマを読んでおり、PRIMARY KEY の暗黙的 NOT NULL も正しくコピーする。
ただし上述の理由（DuckLake の SQLite カタログ自体に NOT NULL がない）により、
この問題は COPY FROM DATABASE では解決できない。

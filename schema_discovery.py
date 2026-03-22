# ─────────────────────────────────────────────────────────────────
# SCHEMA DISCOVERY — Parquet Source
# Scans all table folders, infers schema, and outputs a registry
# template + basic profile per table.
# ─────────────────────────────────────────────────────────────────

import json
from pyspark.sql import functions as F

# ── Config ────────────────────────────────────────────────────────
BASE_PATH    = "abfss://20hgsd2345-234sdfihert-45363dfbb-345@onelake.dfs.fabric.microsoft.com/7bf2345-234f34-234f-a34sd-adfsdf/Files/openbridge/parquet"
SOURCE_SYSTEM = "openbridge"

# Set to a specific table name to inspect just one, or None for all
TARGET_TABLE  = None   # e.g., "sp_inventory_ledger_summary"

# How many rows to sample for the profile (set to 0 to skip profiling)
SAMPLE_ROWS   = 1_000_000
# ──────────────────────────────────────────────────────────────────


def list_table_folders(base_path: str) -> list[str]:
    """Return subfolder names (i.e. table names) under base_path."""
    entries = mssparkutils.fs.ls(base_path)
    return [e.name.rstrip("/") for e in entries if e.isDir]


def read_parquet_sample(table_path: str, n_rows: int):
    """Read parquet files; limit rows if n_rows > 0."""
    df = spark.read.option("mergeSchema", "true").parquet(table_path)
    return df.limit(n_rows) if n_rows > 0 else df


def profile_dataframe(df, table_name: str) -> dict:
    """Compute null rates and distinct counts per column."""
    total = df.count()
    if total == 0:
        return {"row_count": 0, "columns": {}}

    agg_exprs = []
    for col in df.columns:
        agg_exprs += [
            F.count(F.when(F.col(f"`{col}`").isNull(), 1)).alias(f"{col}__nulls"),
            F.approx_count_distinct(F.col(f"`{col}`")).alias(f"{col}__distinct"),
        ]

    stats = df.agg(*agg_exprs).collect()[0].asDict()

    columns = {}
    for col in df.columns:
        null_count    = stats[f"{col}__nulls"]
        distinct_count = stats[f"{col}__distinct"]
        columns[col] = {
            "null_count":     null_count,
            "null_pct":       round(null_count / total * 100, 2),
            "distinct_count": distinct_count,
            "likely_pk_candidate": null_count == 0 and distinct_count == total,
        }

    return {"row_count": total, "columns": columns}


def schema_to_registry_entry(table_name: str, df, source_system: str) -> dict:
    """Produce a table_registry.json stub from an inferred schema."""
    fields = [
        {"name": f.name, "type": str(f.dataType), "nullable": f.nullable}
        for f in df.schema.fields
    ]
    return {
        "table_name":              table_name,
        "source_system":           source_system,
        "source_path_pattern":     f"{BASE_PATH}/{table_name}/*.parquet",
        "primary_keys":            [],          # ← fill after reviewing profile
        "watermark_column":        None,        # ← fill if incremental
        "load_type":               "full_snapshot",  # ← adjust per table
        "partition_columns":       [],
        "z_order_columns":         [],
        "schema_version":          1,
        "schema_enforcement_mode": "additive",
        "dq_rule_set":             f"{table_name}.json",
        "enabled":                 True,
        "priority":                1,
        "owner":                   "",
        "sla_minutes":             60,
        "inferred_schema":         fields,
    }


# ── Main ──────────────────────────────────────────────────────────
tables = (
    [TARGET_TABLE] if TARGET_TABLE
    else list_table_folders(BASE_PATH)
)

print(f"Found {len(tables)} table(s): {tables}\n")
print("=" * 70)

registry_stubs = []

for table_name in tables:
    table_path = f"{BASE_PATH}/{table_name}"
    print(f"\n>>> TABLE: {table_name}")
    print(f"    Path : {table_path}")

    try:
        df = read_parquet_sample(table_path, SAMPLE_ROWS)

        # 1. Schema
        print("\n  SCHEMA:")
        df.printSchema()

        # 2. File count
        files = mssparkutils.fs.ls(table_path)
        parquet_files = [f for f in files if f.name.endswith(".parquet")]
        print(f"  Parquet files : {len(parquet_files)}")
        print(f"  Total size    : {sum(f.size for f in parquet_files) / 1024**2:.1f} MB")

        # 3. Profile
        if SAMPLE_ROWS > 0:
            profile = profile_dataframe(df, table_name)
            print(f"\n  PROFILE (sampled up to {SAMPLE_ROWS:,} rows):")
            print(f"  Row count : {profile['row_count']:,}")
            print(f"  {'Column':<40} {'Null%':>7}  {'Distinct':>10}  {'PK Candidate':>13}")
            print(f"  {'-'*40} {'-'*7}  {'-'*10}  {'-'*13}")
            for col, stats in profile["columns"].items():
                pk_flag = "YES" if stats["likely_pk_candidate"] else ""
                print(f"  {col:<40} {stats['null_pct']:>6.1f}%  {stats['distinct_count']:>10,}  {pk_flag:>13}")

        # 4. Registry stub
        stub = schema_to_registry_entry(table_name, df, SOURCE_SYSTEM)
        registry_stubs.append(stub)

    except Exception as e:
        print(f"  ERROR reading {table_name}: {e}")

    print("-" * 70)


# ── Output registry template ──────────────────────────────────────
print("\n\n" + "=" * 70)
print("TABLE REGISTRY TEMPLATE (table_registry.json)")
print("Copy this, fill in primary_keys / watermark_column / load_type")
print("=" * 70)
print(json.dumps(registry_stubs, indent=2, default=str))

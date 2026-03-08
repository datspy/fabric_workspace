# ============================================================================
# Fabric Lakehouse - File Metadata & Schema Scanner
# ============================================================================
# Paste this into a Microsoft Fabric notebook attached to your Lakehouse.
# Update ROOT_PATH below to point at the folder you want to scan.
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType,
    BooleanType, TimestampType
)
from pyspark.sql import functions as F
from datetime import datetime, timezone
import json
import hashlib

# ── Configuration ───────────────────────────────────────────────────────────
ROOT_PATH       = "Files/"           # Root folder to scan (relative to Lakehouse)
RECURSIVE       = True               # Scan subfolders
SAMPLE_ROWS     = 5                  # Rows to sample for profiling
SUPPORTED_FMTS  = {".csv", ".tsv", ".parquet", ".json", ".delta", ".avro", ".orc"}
# ────────────────────────────────────────────────────────────────────────────

spark = SparkSession.builder.getOrCreate()


# ============================================================================
# 1. DISCOVER FILES
# ============================================================================
def list_files(path, recursive=True):
    """Recursively list all files under *path* using mssparkutils."""
    results = []
    try:
        items = mssparkutils.fs.ls(path)
    except Exception as e:
        print(f"⚠ Could not access: {path}  →  {e}")
        return results

    for item in items:
        if item.isDir and recursive:
            results.extend(list_files(item.path, recursive))
        elif item.isFile:
            results.append({
                "file_name":      item.name,
                "file_path":      item.path,
                "file_size_bytes": item.size,
                "is_directory":   False,
            })
    return results

print("🔍 Scanning files …")
all_files = list_files(ROOT_PATH, RECURSIVE)
print(f"   Found {len(all_files)} file(s).\n")


# ============================================================================
# 2. HELPER UTILITIES
# ============================================================================
def get_extension(name: str) -> str:
    """Return lower-cased extension including the dot, e.g. '.csv'."""
    idx = name.rfind(".")
    return name[idx:].lower() if idx != -1 else ""

def friendly_size(size_bytes: int) -> str:
    """Human-readable file size."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} PB"

def schema_fingerprint(columns: list) -> str:
    """SHA-256 hash of sorted column names → groups files with identical schemas."""
    key = "|".join(sorted(col.lower() for col in columns))
    return hashlib.sha256(key.encode()).hexdigest()[:12]

def safe_read(path: str, ext: str):
    """Try to read a file into a Spark DataFrame. Returns (df, format_used) or (None, None)."""
    readers = {
        ".csv":     lambda p: spark.read.option("header", "true").option("inferSchema", "true")
                                  .option("samplingRatio", "0.1").csv(p),
        ".tsv":     lambda p: spark.read.option("header", "true").option("inferSchema", "true")
                                  .option("sep", "\t").csv(p),
        ".parquet": lambda p: spark.read.parquet(p),
        ".json":    lambda p: spark.read.option("multiLine", "true").json(p),
        ".delta":   lambda p: spark.read.format("delta").load(p),
        ".avro":    lambda p: spark.read.format("avro").load(p),
        ".orc":     lambda p: spark.read.orc(p),
    }
    reader = readers.get(ext)
    if reader is None:
        return None, None
    try:
        df = reader(path)
        return df, ext.replace(".", "")
    except Exception as e:
        print(f"   ⚠ Could not read {path}: {e}")
        return None, None


# ============================================================================
# 3. EXTRACT METADATA FOR EVERY FILE
# ============================================================================
metadata_records = []

for idx, file_info in enumerate(all_files, 1):
    name = file_info["file_name"]
    path = file_info["file_path"]
    size = file_info["file_size_bytes"]
    ext  = get_extension(name)

    print(f"[{idx}/{len(all_files)}] {name}")

    # ── Derive folder path ──────────────────────────────────────────────
    folder = "/".join(path.replace("\\", "/").split("/")[:-1])

    record = {
        "file_name":          name,
        "file_path":          path,
        "folder":             folder,
        "file_extension":     ext,
        "file_size_bytes":    size,
        "file_size_friendly": friendly_size(size),
        "is_supported":       ext in SUPPORTED_FMTS,
        "format_read_as":     None,
        "row_count":          None,
        "column_count":       None,
        "column_names":       None,
        "column_types":       None,
        "schema_json":        None,
        "schema_fingerprint": None,
        "has_nulls_in_cols":  None,
        "null_pct_per_col":   None,
        "sample_values":      None,
        "partition_columns":  None,
        "scan_timestamp":     datetime.now(timezone.utc).isoformat(),
        "scan_status":        "skipped",
        "scan_error":         None,
    }

    if ext not in SUPPORTED_FMTS:
        record["scan_status"] = "unsupported_format"
        metadata_records.append(record)
        continue

    # ── Read file ───────────────────────────────────────────────────────
    df, fmt = safe_read(path, ext)
    if df is None:
        record["scan_status"] = "read_error"
        metadata_records.append(record)
        continue

    try:
        # Basic schema info
        columns      = [f.name for f in df.schema.fields]
        column_types = [f"{f.name}: {f.dataType.simpleString()}" for f in df.schema.fields]

        record["format_read_as"]     = fmt
        record["column_count"]       = len(columns)
        record["column_names"]       = columns
        record["column_types"]       = column_types
        record["schema_json"]        = df.schema.json()
        record["schema_fingerprint"] = schema_fingerprint(columns)

        # Row count (may be slow for huge files — comment out if needed)
        row_count = df.count()
        record["row_count"] = row_count

        # Null analysis per column
        if row_count > 0:
            null_counts = df.select(
                [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in columns]
            ).collect()[0]
            null_pct = {c: round((null_counts[c] or 0) / row_count * 100, 2) for c in columns}
            cols_with_nulls = [c for c, pct in null_pct.items() if pct > 0]
            record["has_nulls_in_cols"] = cols_with_nulls if cols_with_nulls else None
            record["null_pct_per_col"]  = null_pct

        # Sample first N rows (as list of dicts) for quick inspection
        sample_rows = [row.asDict() for row in df.limit(SAMPLE_ROWS).collect()]
        record["sample_values"] = sample_rows

        # Delta / partitioned table detection
        if ext == ".delta":
            try:
                detail = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").collect()[0]
                record["partition_columns"] = list(detail["partitionColumns"]) if detail["partitionColumns"] else None
            except Exception:
                pass

        record["scan_status"] = "success"

    except Exception as e:
        record["scan_status"] = "partial_error"
        record["scan_error"]  = str(e)

    metadata_records.append(record)


# ============================================================================
# 4. BUILD SUMMARY DATAFRAME
# ============================================================================
print("\n📊 Building summary DataFrame …")

summary_schema = StructType([
    StructField("file_name",          StringType(),  True),
    StructField("folder",             StringType(),  True),
    StructField("file_extension",     StringType(),  True),
    StructField("file_size_bytes",    LongType(),    True),
    StructField("file_size_friendly", StringType(),  True),
    StructField("format_read_as",     StringType(),  True),
    StructField("row_count",          LongType(),    True),
    StructField("column_count",       IntegerType(), True),
    StructField("column_names",       StringType(),  True),
    StructField("column_types",       StringType(),  True),
    StructField("schema_fingerprint", StringType(),  True),
    StructField("has_nulls_in_cols",  StringType(),  True),
    StructField("null_pct_per_col",   StringType(),  True),
    StructField("scan_status",        StringType(),  True),
    StructField("scan_error",         StringType(),  True),
    StructField("scan_timestamp",     StringType(),  True),
])

summary_rows = []
for r in metadata_records:
    summary_rows.append((
        r["file_name"],
        r["folder"],
        r["file_extension"],
        r["file_size_bytes"],
        r["file_size_friendly"],
        r["format_read_as"],
        r["row_count"],
        r["column_count"],
        json.dumps(r["column_names"])       if r["column_names"]       else None,
        json.dumps(r["column_types"])       if r["column_types"]       else None,
        r["schema_fingerprint"],
        json.dumps(r["has_nulls_in_cols"])  if r["has_nulls_in_cols"]  else None,
        json.dumps(r["null_pct_per_col"])   if r["null_pct_per_col"]   else None,
        r["scan_status"],
        r["scan_error"],
        r["scan_timestamp"],
    ))

df_summary = spark.createDataFrame(summary_rows, schema=summary_schema)
df_summary.createOrReplaceTempView("file_metadata")

print("✅ View 'file_metadata' ready. Run:  spark.sql('SELECT * FROM file_metadata').show()")


# ============================================================================
# 5. SCHEMA GROUPING  — files that share the same column structure
# ============================================================================
print("\n📁 Schema Groups (files with identical column names):\n")

schema_groups = {}
for r in metadata_records:
    fp = r["schema_fingerprint"]
    if fp is None:
        continue
    schema_groups.setdefault(fp, {
        "columns":  r["column_names"],
        "types":    r["column_types"],
        "files":    [],
        "folders":  set(),
    })
    schema_groups[fp]["files"].append(r["file_name"])
    schema_groups[fp]["folders"].add(r["folder"])

for fp, grp in schema_groups.items():
    print(f"  Schema [{fp}]  —  {len(grp['files'])} file(s)")
    print(f"    Columns : {grp['columns']}")
    print(f"    Types   : {grp['types']}")
    print(f"    Folders : {list(grp['folders'])}")
    print(f"    Files   : {grp['files']}\n")


# ============================================================================
# 6. HIGH-LEVEL STATISTICS
# ============================================================================
total_size   = sum(r["file_size_bytes"] for r in metadata_records)
total_rows   = sum(r["row_count"] or 0  for r in metadata_records)
ext_counts   = {}
status_counts = {}
for r in metadata_records:
    ext_counts[r["file_extension"]]  = ext_counts.get(r["file_extension"], 0) + 1
    status_counts[r["scan_status"]]  = status_counts.get(r["scan_status"], 0) + 1

print("=" * 60)
print("  LAKEHOUSE FILE SCAN — SUMMARY")
print("=" * 60)
print(f"  Total files scanned   : {len(metadata_records)}")
print(f"  Total size            : {friendly_size(total_size)}")
print(f"  Total rows (all files): {total_rows:,}")
print(f"  Unique schemas        : {len(schema_groups)}")
print(f"  File types            : {json.dumps(ext_counts, indent=2)}")
print(f"  Scan statuses         : {json.dumps(status_counts, indent=2)}")
print("=" * 60)


# ============================================================================
# 7. (OPTIONAL) SAVE METADATA TO LAKEHOUSE TABLE
# ============================================================================
# Uncomment the lines below to persist the metadata as a Delta table
# so you can query it later or build reports on top of it.
#
# df_summary.write.mode("overwrite").format("delta").saveAsTable("lakehouse_file_metadata")
# print("💾 Saved to table: lakehouse_file_metadata")


# ============================================================================
# 8. HANDY SQL QUERIES (run in a new notebook cell)
# ============================================================================
# -- All files ordered by size
# SELECT * FROM file_metadata ORDER BY file_size_bytes DESC
#
# -- Files with nulls
# SELECT file_name, has_nulls_in_cols, null_pct_per_col
# FROM file_metadata WHERE has_nulls_in_cols IS NOT NULL
#
# -- Schema mismatch detection: folders with >1 unique schema
# SELECT folder, COUNT(DISTINCT schema_fingerprint) AS schema_count
# FROM file_metadata
# WHERE schema_fingerprint IS NOT NULL
# GROUP BY folder HAVING schema_count > 1
#
# -- Largest files
# SELECT file_name, file_size_friendly, row_count
# FROM file_metadata ORDER BY file_size_bytes DESC LIMIT 10

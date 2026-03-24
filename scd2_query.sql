
**Cell 1 — Create history table**
```sql
%%sql
CREATE TABLE IF NOT EXISTS sku_scd2_history (
    surrogate_key BIGINT,
    sku           STRING,
    ean           STRING,
    asin          STRING,
    ean_qty_per_sku INT,
    valid_from    TIMESTAMP,
    valid_to      TIMESTAMP,
    is_current    BOOLEAN
)
USING DELTA;
```

---

**Cell 2 — Phase 1: Expire rows where key matches but qty changed**
```sql
%%sql
MERGE INTO sku_scd2_history AS target
USING (
    SELECT h.sku, h.ean, h.asin
    FROM   source_table s                          -- replace with your actual source table
    JOIN   sku_scd2_history h
        ON  s.sku  = h.sku
        AND s.ean  = h.ean
        AND s.asin = h.asin
        AND h.is_current        = true
        AND s.ean_qty_per_sku  <> h.ean_qty_per_sku
) AS to_expire
ON  target.sku       = to_expire.sku
AND target.ean       = to_expire.ean
AND target.asin      = to_expire.asin
AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    target.is_current = false,
    target.valid_to   = current_timestamp();
```

---

**Cell 3 — Phase 2: Insert all new rows**
```sql
%%sql
INSERT INTO sku_scd2_history
SELECT
    monotonically_increasing_id()              AS surrogate_key,
    s.sku,
    s.ean,
    s.asin,
    s.ean_qty_per_sku,
    current_timestamp()                        AS valid_from,
    CAST('9999-12-31 00:00:00' AS TIMESTAMP)   AS valid_to,
    true                                       AS is_current
FROM source_table s                            -- replace with your actual source table
WHERE NOT EXISTS (
    SELECT 1
    FROM   sku_scd2_history h
    WHERE  h.sku             = s.sku
    AND    h.ean             = s.ean
    AND    h.asin            = s.asin
    AND    h.ean_qty_per_sku = s.ean_qty_per_sku
    AND    h.is_current      = true
);
```

DROP TABLE IF EXISTS flights_task32;

CREATE EXTERNAL TABLE flights_task32 (
    month INT,
    op_unique_carrier STRING,
    op_carrier_fl_num INT,
    origin STRING,
    dest STRING,
    dep_delay DOUBLE,
    arr_delay DOUBLE,
    cancelled INT,
    cancellation_code STRING
)
STORED AS PARQUET
LOCATION '${hivevar:INPUT_PATH}';

-- Query Finale con CTE (Common Table Expression) per aggirare la mancanza di MAX_BY
WITH aggregato_base AS (
    SELECT 
        origin,
        month,
        COUNT(CASE WHEN cancelled = 0 AND dep_delay < 15 THEN 1 END) AS voli_basso,
        ROUND(COALESCE(AVG(CASE WHEN cancelled = 0 AND dep_delay > 0 AND dep_delay < 15 THEN dep_delay END), 0.0), 2) AS avg_dep_basso,
        ROUND(COALESCE(AVG(CASE WHEN cancelled = 0 AND dep_delay < 15 AND arr_delay > 0 THEN arr_delay END), 0.0), 2) AS avg_arr_basso,
        COUNT(CASE WHEN cancelled = 0 AND dep_delay >= 15 AND dep_delay <= 60 THEN 1 END) AS voli_medio,
        ROUND(COALESCE(AVG(CASE WHEN cancelled = 0 AND dep_delay >= 15 AND dep_delay <= 60 THEN dep_delay END), 0.0), 2) AS avg_dep_medio,
        ROUND(COALESCE(AVG(CASE WHEN cancelled = 0 AND dep_delay >= 15 AND dep_delay <= 60 AND arr_delay > 0 THEN arr_delay END), 0.0), 2) AS avg_arr_medio,
        COUNT(CASE WHEN cancelled = 0 AND dep_delay > 60 THEN 1 END) AS voli_alto,
        ROUND(COALESCE(AVG(CASE WHEN cancelled = 0 AND dep_delay > 60 THEN dep_delay END), 0.0), 2) AS avg_dep_alto,
        ROUND(COALESCE(AVG(CASE WHEN cancelled = 0 AND dep_delay > 60 AND arr_delay > 0 THEN arr_delay END), 0.0), 2) AS avg_arr_alto
    FROM 
        flights_task32
    GROUP BY 
        origin, month
),
conteggio_cause AS (
    SELECT 
        origin,
        month,
        cancellation_code,
        COUNT(*) as r_count,
        ROW_NUMBER() OVER (PARTITION BY origin, month ORDER BY COUNT(*) DESC) as rank
    FROM 
        flights_task32
    WHERE 
        cancelled = 1 AND cancellation_code IS NOT NULL AND cancellation_code != ''
    GROUP BY 
        origin, month, cancellation_code
)
SELECT 
    b.origin,
    b.month,
    b.voli_basso, b.avg_dep_basso, b.avg_arr_basso,
    b.voli_medio, b.avg_dep_medio, b.avg_arr_medio,
    b.voli_alto, b.avg_dep_alto, b.avg_arr_alto,
    CASE 
        WHEN c.cancellation_code IS NULL OR c.cancellation_code = '' THEN 'N/A' 
        ELSE c.cancellation_code 
    END AS causa_canc_piu_frequente
FROM 
    aggregato_base b
LEFT JOIN 
    conteggio_cause c 
ON 
    b.origin = c.origin AND b.month = c.month AND c.rank = 1;
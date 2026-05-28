#!/usr/bin/env python3
import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Inizializza la sessione Spark
    spark = SparkSession.builder \
        .appName("Flight Task 3.2 - Spark SQL") \
        .getOrCreate()

    # Recupera il percorso di input dagli argomenti (passato dall'orchestratore o da terminale)
    input_path = sys.argv[1] if len(sys.argv) > 1 else "/home/sofia/flights_sample_cleaned_parquet"
    output_path = "/home/sofia/Big_Data_Second_Project/output/risultato_spark_task32_temp"

    # 1. Legge i dati dal formato Parquet locale o distribuito
    df = spark.read.parquet(input_path)

    # 2. Crea una vista temporanea per poter usare l'SQL standard
    df.createOrReplaceTempView("flights")

    # 3. Esegue la query con l'aggregazione condizionale (Pivoting) e arrotondamenti a 2 decimali
    query = """
    SELECT 
        origin,
        month,
        
        -- FASCIA BASSA
        COUNT(CASE WHEN cancelled = 0 AND dep_delay < 15 THEN 1 END) AS voli_basso,
        ROUND(COALESCE(AVG(CASE WHEN cancelled = 0 AND dep_delay > 0 AND dep_delay < 15 THEN dep_delay END), 0.0), 2) AS avg_dep_basso,
        ROUND(COALESCE(AVG(CASE WHEN cancelled = 0 AND dep_delay < 15 AND arr_delay > 0 THEN arr_delay END), 0.0), 2) AS avg_arr_basso,
        
        -- FASCIA MEDIA
        COUNT(CASE WHEN cancelled = 0 AND dep_delay >= 15 AND dep_delay <= 60 THEN 1 END) AS voli_medio,
        ROUND(COALESCE(AVG(CASE WHEN cancelled = 0 AND dep_delay >= 15 AND dep_delay <= 60 THEN dep_delay END), 0.0), 2) AS avg_dep_medio,
        ROUND(COALESCE(AVG(CASE WHEN cancelled = 0 AND dep_delay >= 15 AND dep_delay <= 60 AND arr_delay > 0 THEN arr_delay END), 0.0), 2) AS avg_arr_medio,
        
        -- FASCIA ALTA
        COUNT(CASE WHEN cancelled = 0 AND dep_delay > 60 THEN 1 END) AS voli_alto,
        ROUND(COALESCE(AVG(CASE WHEN cancelled = 0 AND dep_delay > 60 THEN dep_delay END), 0.0), 2) AS avg_dep_alto,
        ROUND(COALESCE(AVG(CASE WHEN cancelled = 0 AND dep_delay > 60 AND arr_delay > 0 THEN arr_delay END), 0.0), 2) AS avg_arr_alto,
        
        -- PUNTO C: CAUSA CANCELLAZIONE PIÙ FREQUENTE
        COALESCE(
            MAX_BY(cancellation_code, CASE WHEN cancelled = 1 AND cancellation_code IS NOT NULL AND cancellation_code != '' THEN 1 ELSE 0 END), 
            'N/A'
        ) AS causa_canc_piu_frequente
    FROM 
        flights
    GROUP BY 
        origin, 
        month
    ORDER BY 
        origin, 
        month
    """

    result_df = spark.sql(query)

    # 4. Salva il risultato in un unico file CSV con l'intestazione inclusa!
    result_df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .option("delimiter", "\t") \
        .csv(output_path)

    spark.stop()
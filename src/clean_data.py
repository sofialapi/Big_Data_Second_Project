#!/usr/bin/env python3
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    # Verifica che l'utente abbia fornito i percorsi di input e output
    if len(sys.argv) < 3:
        print("Uso: spark-submit clean_data.py <path_input_csv> <path_output_parquet>")
        sys.exit(-1)
        
    input_path = sys.argv[1]
    output_path = sys.argv[2]

    # Inizializzazione della SparkSession
    # L'app name apparirà nella dashboard di Spark/YARN per il monitoraggio
    spark = SparkSession.builder \
        .appName("FlightDelay-DataCleaning") \
        .getOrCreate()
    
    print(f"[*] Inizio lettura del dataset da: {input_path}")

    # 1. Lettura del file CSV
    # header=True mantiene i nomi delle colonne
    # inferSchema=True identifica automaticamente i tipi (es. interi per month, float per i ritardi)
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Elenco delle colonne necessarie per le interrogazioni 3.1 e 3.2
    columns_to_keep = [
        "month", 
        "op_unique_carrier", 
        "op_carrier_fl_num", 
        "origin", 
        "dest", 
        "dep_delay", 
        "arr_delay", 
        "cancelled", 
        "cancellation_code"
    ]

    print("[*] Selezione delle colonne e filtraggio dei record nulli...")

    # 2. Selezione delle sole colonne desiderate e pulizia dei valori nulli per le colonne opportune (vedi sotto)
    # Nota: Per 'cancellation_code', il valore nullo è semanticamente corretto 
    # se il volo NON è stato cancellato. Di conseguenza, escludiamo dal filtro dei nulli 
    # questa specifica colonna per evitare di perdere i voli regolari.
    df_cleaned = df.select(*columns_to_keep) \
        .filter(
            col("month").isNotNull() &
            col("op_unique_carrier").isNotNull() &
            col("op_carrier_fl_num").isNotNull() &
            col("origin").isNotNull() &
            col("dest").isNotNull() &
            col("dep_delay").isNotNull() &
            col("arr_delay").isNotNull() &
            col("cancelled").isNotNull()
        )

    print(f"[*] Scrittura del dataset pulito...")

    # 1. Salvataggio in formato PARQUET (per Task 3.2 - Hive e Spark SQL)
    # Crea una cartella con i file .parquet ottimizzati e compressi
    df_cleaned.write.mode("overwrite").parquet(output_path + "_parquet")

    # 2. Salvataggio in formato CSV (per Task 3.1 - MapReduce)
    # Crea una cartella con i file .csv puliti, che verranno poi letti riga per riga da sys.stdin
    # header=False risulta consigliato per MapReduce per non dover gestire la riga dei titoli nel Mapper
    df_cleaned.write.mode("overwrite").csv(output_path + "_csv", header=False)

    print("[+] Pulizia completata! Generati sia i file CSV che i file Parquet.")
    spark.stop()

if __name__ == "__main__":
    main()
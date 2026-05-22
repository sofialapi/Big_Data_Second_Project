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

    # Elenco delle colonne richieste dalla tua specifica
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

    # 2. Selezione delle sole colonne desiderate e pulizia dei valori nulli
    # Nota progettuale: Per 'cancellation_code', il valore nullo è semanticamente corretto 
    # se il volo NON è stato cancellato. Di conseguenza, escludiamo dal filtro dei nulli 
    # questa specifica colonna per evitare di perdere il 99% dei voli regolari.
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

    print(f"[*] Scrittura del dataset pulito in formato Parquet su: {output_path}")

    # 3. Salvataggio in formato Parquet
    # 'overwrite' assicura che se riesegui lo script, la cartella precedente viene sovrascritta senza errori
    df_cleaned.write.mode("overwrite").parquet(output_path)

    print("[+] Processo di ottimizzazione e pulizia completato con successo!")
    
    # Chiusura della sessione Spark
    spark.stop()

if __name__ == "__main__":
    main()
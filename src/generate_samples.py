#!/usr/bin/env python3
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    # Verifica gli argomenti passati da riga di comando
    if len(sys.argv) < 2:
        print("Errore! Uso corretto: python3 generate_samples.py <percentuale>")
        print("Esempio per il 30%: python3 generate_samples.py 30")
        sys.exit(-1)

    # Recupera la percentuale (es. 10, 30, 50)
    percentage_str = sys.argv[1]
    if percentage_str not in ["10", "30", "50"]:
        print("Errore! Percentuali supportate: 10, 30, 50")
        sys.exit(-1)
        
    # Converte la percentuale in frazione per Spark (es. 30 -> 0.3)
    fraction = float(percentage_str) / 100.0

    # Definiamo i percorsi fissi locali della tua VM
    input_csv_path = "/home/sofia/flight_data_2024.csv"
    base_output_dir = "/home/sofia/dataset_samples"

    # Inizializzazione della sessione Spark
    spark = SparkSession.builder \
        .appName(f"FlightDelay-GenerateSample-{percentage_str}%") \
        .getOrCreate()

    print(f"[*] Caricamento del dataset integrale da: {input_csv_path}")

    # 1. Lettura del file CSV originale con inferenza dello schema automatica
    df_raw = spark.read.csv(input_csv_path, header=True, inferSchema=True)

    print(f"[*] Campionamento del {percentage_str}% dei record (frazione: {fraction})...")
    # Usa il campionamento casuale senza reinserimento (withReplacement=False)
    # Impostiamo un seed fisso (es. 42) così se rilanci lo script genererà sempre lo stesso identico campione
    df_sampled = df_raw.sample(withReplacement=False, fraction=fraction, seed=42)

    # Elenco delle colonne richieste dalla specifica di progetto
    columns_to_keep = [
        "month", "op_unique_carrier", "op_carrier_fl_num", 
        "origin", "dest", "dep_delay", "arr_delay", 
        "cancelled", "cancellation_code"
    ]

    print("[*] Esecuzione della pipeline di pulizia dati (Data Cleaning)...")
    # Applichiamo gli stessi identici filtri di pulizia validati ieri
    df_cleaned = df_sampled.select(*columns_to_keep) \
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

    # Costruiamo i percorsi di output per riflettere esattamente le richieste di run_all.sh
    output_parquet_path = f"{base_output_dir}/flights_sample_{percentage_str}_parquet"
    output_csv_path = f"{base_output_dir}/flights_sample_{percentage_str}_csv"

    print(f"[*] Scrittura del campione in formato PARQUET -> {output_parquet_path}")
    df_cleaned.write.mode("overwrite").parquet(output_parquet_path)

    print(f"[*] Scrittura del campione in formato CSV (no header) -> {output_csv_path}")
    df_cleaned.write.mode("overwrite").csv(output_csv_path, header=False)

    print(f"[+] Campione {percentage_str}% generato con successo in entrambe le modalità!")
    spark.stop()

if __name__ == "__main__":
    main()
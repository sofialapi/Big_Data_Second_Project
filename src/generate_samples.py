#!/usr/bin/env python3
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    # Verifica gli argomenti passati da riga di comando
    if len(sys.argv) < 2:
        print("Errore! Uso corretto: python3 generate_samples.py <percentuale>")
        print("Esempio per lo stress test 200%: python3 generate_samples.py 200")
        sys.exit(-1)

    # Recupera la percentuale (es. 10, 30, 50, 100, 200, 300)
    percentage_str = sys.argv[1]
    supported_percentages = ["10", "30", "50", "100", "200", "300"]
    
    if percentage_str not in supported_percentages:
        print(f"Errore! Percentuali supportate: {', '.join(supported_percentages)}")
        sys.exit(-1)
        
    percentage = int(percentage_str)

    # Definisce i percorsi fissi locali della VM
    input_csv_path = "/home/sofia/flight_data_2024.csv"
    base_output_dir = "/home/sofia/dataset_samples"

    # Inizializzazione della sessione Spark
    spark = SparkSession.builder \
        .appName(f"FlightDelay-GenerateSample-{percentage_str}%") \
        .getOrCreate()

    print(f"[*] Caricamento del dataset integrale da: {input_csv_path}")

    # Lettura del file CSV originale con inferenza dello schema automatica
    df_raw = spark.read.csv(input_csv_path, header=True, inferSchema=True)

    # Elenco delle colonne richieste dalla specifica di progetto
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

    # Applica la pipeline di pulizia dati
    print("[*] Esecuzione della pipeline di pulizia dati (Data Cleaning)...")
    df_base_cleaned = df_raw.select(*columns_to_keep).filter(
        col("month").isNotNull() &
        col("op_unique_carrier").isNotNull() &
        col("op_carrier_fl_num").isNotNull() &
        col("origin").isNotNull() &
        col("dest").isNotNull() &
        col("dep_delay").isNotNull() &
        col("arr_delay").isNotNull() &
        col("cancelled").isNotNull()
    )

    # Gestione del campionamento o dell'incremento artificiale della volumetria
    if percentage <= 100:
        fraction = float(percentage) / 100.0
        print(f"[*] Campionamento del {percentage_str}% dei record (frazione: {fraction})...")
        df_final = df_base_cleaned.sample(withReplacement=False, fraction=fraction, seed=42)
    else:
        multiplier = percentage // 100
        print(f"[*] Stress Test: Generazione di una volumetria pari al {percentage_str}% tramite concatenazione logica (Moltiplicatore: {multiplier}x)...")
        
        # Duplicazione verticale del dataset pulito tramite l'operatore distribuito .union()
        df_final = df_base_cleaned
        for _ in range(multiplier - 1):
            df_final = df_final.union(df_base_cleaned)

    # Costruisce i percorsi di output per riflettere le richieste di run_all.sh
    output_parquet_path = f"{base_output_dir}/flights_sample_{percentage_str}_parquet"
    output_csv_path = f"{base_output_dir}/flights_sample_{percentage_str}_csv"

    print(f"[*] Scrittura del dataset in formato PARQUET -> {output_parquet_path}")
    df_final.write.mode("overwrite").parquet(output_parquet_path)

    #print(f"[*] Scrittura del dataset in formato CSV (no header, delimitatore Tab) -> {output_csv_path}")
    #df_final.write.mode("overwrite").csv(output_csv_path, header=False, sep="\t")
    print(f"[*] Scrittura del dataset in formato CSV (no header) -> {output_csv_path}")
    # Rimosso sep="\t" per salvare in CSV standard separato da virgole
    df_final.write.mode("overwrite").csv(output_csv_path, header=False)

    print(f"[+] Dataset al {percentage_str}% generato e pulito con successo in entrambe le modalità!")
    spark.stop()

if __name__ == "__main__":
    main()
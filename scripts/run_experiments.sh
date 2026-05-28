#!/bin/bash
# run_experiments.sh

# ==========================================
# CONFIGURAZIONE DEI PARAMETRI DI TEST
# Modifica queste tre variabili prima di lanciare lo script
# ==========================================
TECNOLOGIA="spark"       # Opzioni: "hive" o "spark"
DATASET_SIZE="10"        # Opzioni: "10", "30", "50", "100"
AMBIENTE="local"         # Opzioni: "local" (VM cartella) o "hdfs" (Hadoop/AWS)

# ==========================================
# DETERMINAZIONE AUTOMATICA DEI PERCORSI
# ==========================================
if [ "$AMBIENTE" = "local" ]; then
    BASE_PATH="/home/sofia"
else
    BASE_PATH="hdfs:///user/hadoop/input"
fi

INPUT_DIR="${BASE_PATH}/flights_sample_${DATASET_SIZE}.parquet"
OUTPUT_DIR="/home/sofia/Big_Data_Second_Project/output/res_${TECNOLOGIA}_size${DATASET_SIZE}"

echo "=========================================================="
echo "AVVIO ESPERIMENTO: $TECNOLOGIA su dataset al $DATASET_SIZE% ($AMBIENTE)"
echo "Input target: $INPUT_DIR"
echo "=========================================================="

# Cronometro inizio job
START_TIME=$(date +%s)

# ==========================================
# ESECUZIONE DEL JOB COERENTE
# ==========================================
if [ "$TECNOLOGIA" = "hive" ]; then
    # Avvio query Hive con iniezione del percorso
    hive -hivevar INPUT_DIR="$INPUT_DIR" -f src/query_task32.sql > "${OUTPUT_DIR}.txt"
    
elif [ "$TECNOLOGIA" = "spark" ]; then
    # Avvio script PySpark con argomenti posizionali
    spark-submit src/analysis_task32_spark.py "$INPUT_DIR" "$OUTPUT_DIR"
else
    echo "Errore: Tecnologia non riconosciuta."
    exit 1
fi

# Cronometro fine job e calcolo performance
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

echo "=========================================================="
echo "FINE JOB. Tempo impiegato: $ELAPSED secondi."
echo "=========================================================="
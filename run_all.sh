#!/bin/bash

# Controllo argomenti minimi
if [ $# -lt 3 ]; then
    echo "Errore! Uso corretto: ./run_all.sh <tecnologia> <percentuale> <ambiente>"
    echo "  - tecnologia: mapreduce | hive | spark"
    echo "  - percentuale: 10 | 30 | 50 | 100 | 200 | 300"
    echo "  - ambiente:    local | hdfs | aws"
    exit 1
fi

TECNOLOGIA=$1
SIZE=$2
AMBIENTE=$3

# --- BLOCCO DI SICUREZZA PER STRESS TEST SU AWS ---
if { [ "$SIZE" = "200" ] || [ "$SIZE" = "300" ]; } && [ "$AMBIENTE" != "aws" ]; then
    echo "================================================================="
    echo " ERROR: Rilevato Stress Test con dimensionamento al ${SIZE}%."
    echo " I test al 200% e 300% possono essere eseguiti SOLO su cluster AWS."
    echo " L'esecuzione su singola macchina virtuale è stata bloccata."
    echo "================================================================="
    exit 1
fi

# Definizione del file storico delle metriche
METRICS_FILE="/home/sofia/Big_Data_Second_Project/output/metrics_history.txt" [cite: 32]

# Configurazione dinamica dei percorsi in base all'ambiente scelto
if [ "$AMBIENTE" = "local" ]; then
    BASE_INPUT_DIR="/home/sofia/dataset_samples"
    BASE_OUTPUT_DIR="/home/sofia/Big_Data_Second_Project/output"
    
    INPUT_MAPREDUCE="${BASE_INPUT_DIR}/flights_sample_${SIZE}_csv"
    INPUT_PARQUET="file://${BASE_INPUT_DIR}/flights_sample_${SIZE}_parquet"
elif [ "$AMBIENTE" = "hdfs" ]; then
    BASE_INPUT_DIR="/user/sofia/dataset_samples"
    BASE_OUTPUT_DIR="/user/sofia/output"
    
    if [ "$SIZE" = "100" ]; then
        INPUT_MAPREDUCE="/user/sofia/flight_data_2024_csv"
        INPUT_PARQUET="hdfs://localhost:9000/user/sofia/flight_data_2024_parquet"
    else
        INPUT_MAPREDUCE="${BASE_INPUT_DIR}/flights_sample_${SIZE}_csv"
        INPUT_PARQUET="hdfs://localhost:9000${BASE_INPUT_DIR}/flights_sample_${SIZE}_parquet"
    fi
else
    # --- CONFIGURAZIONE PERCORSI STORAGE AWS S3 ---
    BUCKET_NAME="sofia-bigdata-project-task31and32"
    BASE_OUTPUT_DIR="s3://${BUCKET_NAME}/output"
    
    INPUT_MAPREDUCE="s3://${BUCKET_NAME}/input/flights_sample_${SIZE}_csv"
    INPUT_PARQUET="s3://${BUCKET_NAME}/input/flights_sample_${SIZE}_parquet"
fi

# --- LOGICA DINAMICA PER I TASK E NUOVO FORMATO NOMI ---
if [ "$TECNOLOGIA" = "mapreduce" ]; then
    TASK="task31"
else
    TASK="task32"
fi

FINAL_LOCAL_OUTPUT="/home/sofia/Big_Data_Second_Project/output/risultato_${TASK}_${TECNOLOGIA}_${SIZE}_${AMBIENTE}.txt" [cite: 40]
# ------------------------------------------------------------------

echo "================================================================="
echo " EXECUTION PROTOCOL: $TECNOLOGIA | Size: $SIZE% | Environment: $AMBIENTE"
echo " Input selezionato: $INPUT_PARQUET"
echo " Output (Top 10):   $FINAL_LOCAL_OUTPUT"
echo "================================================================="
echo "[*] Elaborazione in corso... Attendere prego."

# Start Cronometro
START_TIME=$(date +%s)

# Routing dell'esecuzione alla tecnologia corretta con soppressione log inutili
if [ "$TECNOLOGIA" = "mapreduce" ]; then
    if [ "$SIZE" = "100" ]; then SIZE_LABEL="full"; else SIZE_LABEL="size${SIZE}"; fi
    
    if [ "$AMBIENTE" = "aws" ]; then
        MR_OUTPUT_PATH="${BASE_OUTPUT_DIR}/output_mapreduce_${SIZE}"
        # La rimozione preventiva della cartella S3 viene delegata all'automazione dello Step EMR o CLI
    else
        MR_OUTPUT_PATH="${BASE_OUTPUT_DIR}/risultato_mapreduce_${SIZE_LABEL}"
        if [ "$AMBIENTE" = "hdfs" ]; then hdfs dfs -rm -r $MR_OUTPUT_PATH 2>/dev/null; else rm -rf $MR_OUTPUT_PATH; fi
    fi

    if [ "$AMBIENTE" != "aws" ]; then
        # Esecuzione nativa sul cluster locale Single-Node
        hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
            -files /home/sofia/Big_Data_Second_Project/src/mapper.py,/home/sofia/Big_Data_Second_Project/src/reducer.py \
            -mapper "python3 mapper.py" \
            -reducer "python3 reducer.py" \
            -input $INPUT_MAPREDUCE \
            -output $MR_OUTPUT_PATH 2>/dev/null

        # Estrazione locale delle prime 10 righe
        rm -rf "$FINAL_LOCAL_OUTPUT"
        if [ "$AMBIENTE" = "hdfs" ]; then
            hdfs dfs -cat ${MR_OUTPUT_PATH}/part-* 2>/dev/null | head -n 10 > "$FINAL_LOCAL_OUTPUT" [cite: 33]
        else
            cat ${MR_OUTPUT_PATH}/part-* 2>/dev/null | head -n 10 > "$FINAL_LOCAL_OUTPUT"
        fi
    else
        echo "[*] Per l'ambiente AWS, inserire lo Step MapReduce tramite Console EMR."
        echo "    Output atteso in: $MR_OUTPUT_PATH"
    fi

elif [ "$TECNOLOGIA" = "hive" ]; then
    if [ "$AMBIENTE" != "aws" ]; then
        TEMP_HIVE="/home/sofia/Big_Data_Second_Project/output/hive_temp.txt"
        echo -e "ORIGIN\tMONTH\tVOLI_BASSO\tAVG_DEP_BASSO\tAVG_ARR_BASSO\tVOLI_MEDIO\tAVG_DEP_MEDIO\tAVG_ARR_MEDIO\tVOLI_ALTO\tAVG_DEP_ALTO\tAVG_ARR_ALTO\tCAUSA_CANC_FREQ" > "$TEMP_HIVE"
        
        hive -hivevar INPUT_PATH="$INPUT_PARQUET" -f /home/sofia/Big_Data_Second_Project/src/query_task32.sql >> "$TEMP_HIVE" 2>/dev/null
        
        head -n 11 "$TEMP_HIVE" > "$FINAL_LOCAL_OUTPUT"
        rm -f "$TEMP_HIVE"
    else
        echo "[*] Per l'ambiente AWS, inserire lo Step Hive tramite Console EMR."
        echo "    Argomenti da passare: -d INPUT_PATH=$INPUT_PARQUET"
    fi

elif [ "$TECNOLOGIA" = "spark" ]; then
    if [ "$AMBIENTE" != "aws" ]; then
        TEMP_SPARK_DIR="/home/sofia/Big_Data_Second_Project/output/risultato_spark_task32_temp"
        rm -rf "$TEMP_SPARK_DIR" "$FINAL_LOCAL_OUTPUT"
        
        spark-submit --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
                     --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
                     /home/sofia/Big_Data_Second_Project/src/analysis_task_32_spark.py "$INPUT_PARQUET" >/dev/null 2>&1
        
        if [ -d "$TEMP_SPARK_DIR" ]; then
            cat ${TEMP_SPARK_DIR}/part-*.csv 2>/dev/null | head -n 11 > "$FINAL_LOCAL_OUTPUT"
            rm -rf "$TEMP_SPARK_DIR"
        fi
    else
        echo "[*] Per l'ambiente AWS, inserire lo Step Spark tramite Console EMR."
        echo "    Argomenti da passare: $INPUT_PARQUET"
    fi

else
    echo "Tecnologia non supportata."
    exit 1
fi

# End Cronometro
END_TIME=$(date +%s)
ELAPSED_PRINT=$((END_TIME - START_TIME))

echo "================================================================="
echo " COMPLETATO IN: $ELAPSED_PRINT secondi."
if [ "$AMBIENTE" != "aws" ]; then
    echo " Report (Top 10) salvato in: $FINAL_LOCAL_OUTPUT"
else
    echo " Esecuzione tracciata per l'infrastruttura AWS Cloud."
fi
echo "================================================================="

# --- AGGIORNAMENTO FILE METRICS_HISTORY ---
if [ ! -f "$METRICS_FILE" ]; then
    echo "TECNOLOGIA,PERCENTUALE,AMBIENTE,TEMPO_SECONDI" > "$METRICS_FILE"
fi
echo "${TECNOLOGIA},${SIZE},${AMBIENTE},${ELAPSED_PRINT}" >> "$METRICS_FILE" [cite: 32]
echo "[+] Metriche registrate con successo in metrics_history.txt" [cite: 32]
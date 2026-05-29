# Big_Data_Second_Project
Progetto sviluppato per il corso di Big Data nell'A.A. 2025-2026 del corso di laurea magistrale in Ingegneria Informatica dell'università di Roma Tre

dataset: https://www.kaggle.com/datasets/hrishitpatil/flight-data-2024

ISTRUZIONI PER L'USO
Al fine di garantire la totale riproducibilità delle pipeline analitiche sviluppate, l'intera logica di esecuzione è stata centralizzata all'interno dello script di orchestrazione principale, denominato run_all.sh. 
Per consentire una corretta esecuzione del flusso su macchine locali caratterizzate da alberature di directory differenti, l'utente è tenuto preliminarmente ad aprirlo per valorizzare le variabili d'ambiente dedicate ai percorsi di sistema. 
Modificando esclusivamente le stringhe associate alla cartella radice del progetto e alla localizzazione del dataset sorgente, i riferimenti interni verranno propagati dinamicamente a tutte le sotto-operazioni di pulizia e interrogazione. 
Una volta allineati i percorsi locali e verificate le autorizzazioni di esecuzione del file tramite il comando di sistema dedicato (chmod +x run_all.sh), l'avvio del file run_all.sh provvederà a richiamare sequenzialmente i moduli Hadoop MapReduce, Hive e Spark SQL, rigenerando la matrice dei risultati e i relativi file di output all'interno delle cartelle di destinazione personalizzate.

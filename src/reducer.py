#!/usr/bin/env python3
import sys

current_key = None
flights_count = 0
delayed_flights_count = 0
total_delay = 0.0
min_delay = float('inf')
max_delay = float('-inf')
cancelled_count = 0
months_set = set()

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
        
    # Split tra Chiave (carrier,origin) e il resto dei valori
    parts = line.split("\t")
    if len(parts) < 4:
        continue
        
    key_str, arr_delay, cancelled, month = parts
    
    # Se cambiamo gruppo (nuova coppia compagnia,aeroporto), stampiamo il riepilogo del gruppo precedente
    if current_key and current_key != key_str:
        # Parsing della vecchia chiave per la stampa pulita
        carrier, origin = current_key.split(",")
        
        avg_delay = total_delay / delayed_flights_count if delayed_flights_count > 0 else 0
        cancellation_rate = (cancelled_count / flights_count) * 100 if flights_count > 0 else 0
        months_list = ",".join(sorted(list(months_set), key=int))
        
        print_min = min_delay if min_delay != float('inf') else 0
        print_max = max_delay if max_delay != float('-inf') else 0
        
        print(f"{carrier}\t{origin}\tVoliTotali:{flights_count}\tVoliInRitardo:{delayed_flights_count}\tDelayMin:{print_min}\tDelayMax:{print_max}\tDelayAvg:{avg_delay:.2f}\tTassoCanc:{cancellation_rate:.2f}%\tMesi:[{months_list}]")
        
        # Reset completo dei contatori per la nuova chiave
        flights_count = 0
        delayed_flights_count = 0
        total_delay = 0.0
        min_delay = float('inf')
        max_delay = float('-inf')
        cancelled_count = 0
        months_set = set()
        
    current_key = key_str
    flights_count += 1
    months_set.add(month)
    
    if int(cancelled) == 1:
        cancelled_count += 1
    else:
        try:
            delay = float(arr_delay)
            if delay > 0:
                delayed_flights_count += 1
                total_delay += delay
                if delay < min_delay: min_delay = delay
                if delay > max_delay: max_delay = delay
        except ValueError:
            # Gestisce eventuali stringhe sporche o vuote nei ritardi
            pass

# Stampa dell'ultimo gruppo rimasto in coda
if current_key:
    carrier, origin = current_key.split(",")
    avg_delay = total_delay / delayed_flights_count if delayed_flights_count > 0 else 0
    cancellation_rate = (cancelled_count / flights_count) * 100 if flights_count > 0 else 0
    months_list = ",".join(sorted(list(months_set), key=int))
    print_min = min_delay if min_delay != float('inf') else 0
    print_max = max_delay if max_delay != float('-inf') else 0
    
    print(f"{carrier}\t{origin}\tVoliTotali:{flights_count}\tVoliInRitardo:{delayed_flights_count}\tDelayMin:{print_min}\tDelayMax:{print_max}\tDelayAvg:{avg_delay:.2f}\tTassoCanc:{cancellation_rate:.2f}%\tMesi:[{months_list}]")
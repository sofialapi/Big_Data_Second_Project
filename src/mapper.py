#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
        
    fields = line.split(",")
    if len(fields) < 9: 
        continue
        
    month = fields[0].strip()
    carrier = fields[1].strip()
    origin = fields[3].strip()
    arr_delay = fields[6].strip()
    cancelled = fields[7].strip()
    
    # CHIAVE: carrier e origin uniti da una virgola (es. AA,JFK)
    # VALORE: arr_delay, cancelled, month
    # Hadoop userà l'intera stringa prima della prima tabulazione come chiave di ordinamento
    print(f"{carrier},{origin}\t{arr_delay}\t{cancelled}\t{month}")
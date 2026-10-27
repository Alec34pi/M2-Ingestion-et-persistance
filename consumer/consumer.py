import os
import sys
import time
import signal
import json
from confluent_kafka import Consumer, KafkaError

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.getenv("VITALS_TOPIC", "mimic-vitals")
GROUP_ID = os.getenv("CONSUMER_GROUP", "explo-1")
OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "earliest")
AUTO_COMMIT = os.getenv("AUTO_COMMIT", "true").lower() == "true"
INSTANCE = os.getenv("INSTANCE", "c1")

running = True

def signal_handler(sig, frame):
    global running
    print(f"[{INSTANCE}] Arrêt demandé par signal...", flush=True)
    running = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

lus = 0
partitions_compteur = {}

def sur_attribution(consommateur, partitions):
    global lus
    liste = ", ".join(str(p.partition) for p in partitions) or "aucune"
    print(f"[{INSTANCE}] ATTRIBUTION t={time.strftime('%H:%M:%S')} lus={lus} partitions=[{liste}]", flush=True)

def sur_retrait(consommateur, partitions):
    global lus
    liste = ", ".join(str(p.partition) for p in partitions) or "aucune"
    print(f"[{INSTANCE}] RETRAIT t={time.strftime('%H:%M:%S')} lus={lus} partitions=[{liste}]", flush=True)

def creer_consommateur():
    conf = {
        'bootstrap.servers': BOOTSTRAP,
        'group.id': GROUP_ID,
        'client.id': f"lecteur-{INSTANCE}",
        'auto.offset.reset': OFFSET_RESET,
        'enable.auto.commit': AUTO_COMMIT,
        'partition.assignment.strategy': 'range'
    }
    return Consumer(conf)

def main():
    global lus
    consommateur = creer_consommateur()
    consommateur.subscribe([TOPIC], on_assign=sur_attribution, on_revoke=sur_retrait)
    
    print(f"[{INSTANCE}] Démarrage de la lecture sur {TOPIC} (groupe: {GROUP_ID})", flush=True)
    
    messages_depuis_commit = 0
    try:
        while running:
            msg = consommateur.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"[{INSTANCE}] Erreur: {msg.error()}", flush=True)
                    break
            
            lus += 1
            p = msg.partition()
            partitions_compteur[p] = partitions_compteur.get(p, 0) + 1
            
            if lus % 50 == 0:
                print(f"[{INSTANCE}] Lu {lus} messages. Répartition: {partitions_compteur}", flush=True)
            
            if not AUTO_COMMIT:
                messages_depuis_commit += 1
                if messages_depuis_commit >= 100:
                    consommateur.commit(asynchronous=False)
                    messages_depuis_commit = 0
                    
    finally:
        print(f"[{INSTANCE}] Fermeture. Total lus={lus}, répartition={partitions_compteur}", flush=True)
        consommateur.close()

if __name__ == "__main__":
    main()

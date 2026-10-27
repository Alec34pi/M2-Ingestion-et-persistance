import json
import os
import random
import signal
import sys
import time
from confluent_kafka import Producer

# (1) Configuration lue dans l'environnement
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
VITALS_TOPIC = os.getenv("VITALS_TOPIC", "mimic-vitals")
NB_PATIENTS = int(os.getenv("NB_PATIENTS", "12"))
INTERVAL_S = float(os.getenv("INTERVAL_S", "1.0"))

# Table des plages physiologiques
PLAGES = {
    "HR": {"min": 55.0, "max": 115.0, "unit": "bpm"},
    "SPO2": {"min": 90.0, "max": 100.0, "unit": "%"},
    "ABP_SYS": {"min": 95.0, "max": 145.0, "unit": "mmHg"},
}

# Drapeau pour l'arrêt propre
running = True
messages_livres = 0
messages_echoues = 0


def gerer_signal(signum, frame):
    """Gestionnaire de signal pour l'arrêt propre (SIGINT, SIGTERM)."""
    global running
    print("\n[INFO] Signal d'arrêt reçu, fermeture propre en cours...")
    running = False


# Armement des signaux d'arrêt
signal.signal(signal.SIGINT, gerer_signal)
signal.signal(signal.SIGTERM, gerer_signal)


def accuse_reception(err, msg):
    """Callback de livraison appelé par poll() et flush()."""
    global messages_livres, messages_echoues
    if err is not None:
        messages_echoues += 1
        print(f"[ERREUR] Échec de livraison : {err}", file=sys.stderr)
    else:
        messages_livres += 1


def creer_producteur() -> Producer:
    """Construit le producteur Kafka avec sa configuration de fiabilité."""
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "vitals-simulator-s1",
        "acks": "all",
        "linger.ms": 10,
        "retries": 5,
        "enable.idempotence": True,
        "partitioner": "murmur2_random",
    }
    return Producer(conf)


def generer_releve(patient_id: str) -> dict:
    """Produit un relevé conforme au format (event_time en ms epoch)."""
    vital_type = random.choice(list(PLAGES.keys()))
    plage = PLAGES[vital_type]
    valeur = round(random.uniform(plage["min"], plage["max"]), 1)

    return {
        "patient_id": patient_id,
        "vital_type": vital_type,
        "value": valeur,
        "unit": plage["unit"],
        "event_time": int(time.time() * 1000),
    }


def main() -> int:
    global running
    producteur = creer_producteur()
    patients = [f"P{i:03d}" for i in range(1, NB_PATIENTS + 1)]

    print(f"[INFO] Démarrage du producteur vers {KAFKA_BOOTSTRAP} (topic: {VITALS_TOPIC})")
    temps_debut = time.time()
    total_envoyes = 0

    while running:
        debut_tour = time.time()

        for patient_id in patients:
            releve = generer_releve(patient_id)

            producteur.produce(
                topic=VITALS_TOPIC,
                key=patient_id.encode("utf-8"),
                value=json.dumps(releve).encode("utf-8"),
                on_delivery=accuse_reception,
            )
            total_envoyes += 1

        # Traite les callbacks sans bloquer la boucle
        producteur.poll(0)

        # Respect de l'intervalle d'échantillonnage
        temps_ecoule = time.time() - debut_tour
        temps_sommeil = max(0.0, INTERVAL_S - temps_ecoule)
        time.sleep(temps_sommeil)

    # (4) Arrêt propre : flush borné
    print("[INFO] Purge du buffer (flush) en cours...")
    restants = producteur.flush(10)

    duree_totale = time.time() - temps_debut
    debit = total_envoyes / duree_totale if duree_totale > 0 else 0

    print("\n--- Bilan de production ---")
    print(f"Durée totale : {duree_totale:.2f} s")
    print(f"Messages envoyés : {total_envoyes}")
    print(f"Messages livrés : {messages_livres}")
    print(f"Messages échoués : {messages_echoues}")
    print(f"Messages restants dans buffer : {restants}")
    print(f"Débit moyen : {debit:.2f} msg/s")

    return 0


if __name__ == "__main__":
    sys.exit(main())
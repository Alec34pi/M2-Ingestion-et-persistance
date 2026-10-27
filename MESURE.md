# Tableau de décisions

| Outil | Version relevée sur votre poste |
| --- | --- |
| **Docker Engine** | `29.8.2` |
| **Docker Compose** | `5.5.1` |
| **Python** | `3.12.4` |

---
---

# Partie 1 - Docker : image, conteneur, couches

## 1.1 - Image et conteneur : la distinction

### _Question 1.1_
> **Réponse :** Non, le 2ᵉ conteneur ne voit pas le fichier car la couche écriture du premier conteneur est isolée.


## 1.2 - Écrire un Dockerfile exploitant le cache


## 1.3 - Construire, lancer, inspecter

### Mesure 1
| Grandeur | Valeur relevée |
| --- | --- |
| **Taille de `python:3.12-slim`** | `179MB` |
| **Taille de votre image finale** | `193MB` |
| **Nombre de couches non vides dans `docker history`** | `9` |
| **Couche la plus volumineuse (instruction + taille)** | `debian.sh --arch 'amd64' out/ 'trixie'` (`87.7MB`) |

### Mesure 2
| Scénario de rebuild | Durée / Étapes `CACHED` |
| --- | --- |
| **Aucune modification** | `~0.6s` / 6 étapes `CACHED` |
| **Modification de `producer.py`** | `~1-2s` / 4 étapes `CACHED` |
| **Modification de `requirements.txt`** | `~5-10s` / 2 étapes `CACHED` |

### _Question 1.2_
> **Réponse :** Si le `COPY . ./` était placé avant l'instruction `RUN pip install`, toute modification apportée à `producer.py` invaliderait le cache à partir de cette ligne, ce qui rendrait l'installation des dépendances plus lente à chaque build.

---
---

# Partie 2 - Docker Compose : du `docker run` au service déclaré

## 2.1 - Pourquoi Compose


## 2.2 - Premier `docker-compose.yml`

### _Question 2.1_
> **Réponse :** Pour éviter les erreurs de permissions (`permission denied`) lors de l'accès aux volumes ou aux processus conteneurisés.


## 2.3 - Réseau : résolution par nom de service

### Mesure 3
> **Délai observé avant status healthy :** `0,07 s`

---
---

# Partie 3 - Kafka en local, en mode KRaft

## 3.1 - Les trois notions à retenir


## 3.2 - Ajouter le broker au Compose

### Mesure 4
> **Temps d'exécution / Délai :** `1,4 s`


## 3.3 - Créer le topic et inspecter ses partitions

### _Question 3.1_
> **Réponse :** Car le nombre de partitions d'un topic ne peut pas dépasser le nombre total de brokers Kafka actifs dans le cluster.


## 3.4 - Produire et lire en ligne de commande

### Mesure 5
| Clé | Partition(s) et offsets observés |
| --- | --- |
| **P001** | Partition `2`, Offsets `0, 1, 2` |
| **P002** | Partition `1`, Offset `0` |
| **P003** | Partition `0`, Offset `0` |

### _Question 3.2_
> **Réponse :** Oui, tous les messages associés à la clé `P001` sont systématiquement routés vers la **partition 2**, avec des offsets séquentiels (`0, 1, 2`). Cela démontre que Kafka garantit un ordre chronologique et strict des messages pour une même partition.

---
---

# Partie 4 - Producteur Python de signes vitaux

## 4.1 - Format des messages


## 4.2 - Pourquoi la clé de partitionnement compte


## 4.3 - Écrire le producteur

### Décisions de configuration
| Paramètre | Ce qu'il règle | Valeur retenue |
| --- | --- | --- |
| `bootstrap.servers` | Point d'entrée de découverte du cluster depuis l'environnement | `localhost:29092` (hôte) / `kafka:9092` (conteneur) |
| `client.id` | Nom affiché côté broker | `vitals-simulator-s1` |
| `acks` | Niveau d'accusé de réception exigé du broker | `all` |
| `linger.ms` | Temps d'attente avant l'expédition d'un lot de messages (ms) | `10` |
| `retries` | Nombre de réémissions en cas d'erreur transitoire | `5` |
| `enable.idempotence` | Garantit qu'un retry ne crée ni doublon ni désordre | `True` |
| `partitioner` | Fonction déterminant la partition cible à partir de la clé | `murmur2_random` |
| `NB_PATIENTS` | Nombre de lits/patients simulés | `12` |
| `INTERVAL_S` | Intervalle de temps entre deux relevés (s) | `1.0` |


## 4.4 - Exécuter depuis l'hôte


## 4.5 - Comportement au démarrage

### Mesure 6
| Forme de `depends_on` | Erreurs de connexion au démarrage ? | Nombre de lignes d'erreur avant stabilisation |
| --- | --- | --- |
| `depends_on: [kafka]` | **Oui** | 1 à 5 |
| `condition: service_healthy` | **Non** | 0 |

---
---

# Partie 5 - Vérification et mesure

## 5.1 - Débit de production

### Mesure 7
| Grandeur | Valeur |
| --- | --- |
| **Somme des offsets de fin avant ($t_0$)** | `533` |
| **Somme des offsets de fin après 60 s ($t_1$)** | `1253` |
| **Débit calculé :** $(t_1 - t_0) / 60$ | `12.0 msg/s` |
| **Débit annoncé par le producteur à l'arrêt** | `12.0 msg/s` |
| **Valeurs de `INTERVAL_S` et `NB_PATIENTS` utilisées** | `INTERVAL_S=1.0`, `NB_PATIENTS=12` |

### _Question 5.1_
> **Réponse :** Le débit mesuré (`12.0 msg/s`) correspond exactement au débit théorique calculé ($12 \text{ patients} / 1.0\text{ s} = 12 \text{ msg/s}$).

| Grandeur | Valeur |
| --- | --- |
| **Débit mesuré avec `INTERVAL_S=0.1`** | `119.8 msg/s` |
| **Écart avec la valeur théorique (`120.0 msg/s`)** | `< 0.2 %` |


## 5.2 - Coupure du broker pendant la production

### Mesure 8
| Observation | Valeur relevée |
| --- | --- |
| **Le processus producteur s'arrête-t-il ?** | Non |
| **Message d'erreur exact affiché (1ʳᵉ ligne)** | `Échec de livraison : Local: Broker transport failure` |
| **Délai entre le `stop` et la première erreur** | `2 s` |
| **Après `start`, la production reprend-elle sans relancer le script ?** | Oui |
| **Des messages produits pendant la coupure ont-ils été perdus ?** | Non |

### _Question 5.2_
> **Réponse :**
> - **Comportement :** À la coupure du broker, le producteur ne plante pas. Il conserve les messages dans son buffer mémoire et retente de les expédier (`retries=5`).
> - **Garantie d'intégration :** Les paramètres `enable.idempotence=True` et `acks=all` garantissent une livraison exacte (*exactly-once*) sans doublons ni inversion d'ordre au redémarrage du service.
> - **Limites :** La durée de résistance du producteur est limitée par la capacité de sa mémoire tampon et par le paramètre `delivery.timeout.ms`. Si la panne se prolonge ou que le buffer sature, les nouveaux messages seront définitivement rejetés.

---
---

# Partie 6

### _Question 6.1_
> **Réponse :**
> - `docker compose down` : Arrête et supprime les conteneurs et réseaux associés, mais **conserve** les volumes de données (ex: `kafka-data`).
> - `docker compose down -v` : Arrête, supprime les conteneurs/réseaux ET **détruit tous les volumes** associés, entraînant la perte des données persistées.

---
---
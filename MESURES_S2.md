# Mesures séance 2

## Vérification des prérequis

| Contrôle | Valeur relevée | Commande PowerShell associée |
| :--- | :--- | :--- |
| **Sortie de `docker compose config --quiet`** | compose OK | ``docker compose config --quiet`` |
| **Le volume `kafka-data` existe-t-il encore ?** | Oui | ``docker volume ls`` |
| **Version de PostgreSQL déjà installée nativement** | aucune | ``psql --version`` |

---
---

# Partie 1 : Reprise de l'artefact S1 et passage à plusieurs partitions

## 1.1 - Relancer la chaîne de la séance 1

### Mesure 1

| Grandeur | Valeur relevée | Commande PowerShell associée |
| :--- | :--- | :--- |
| **Nombre de partitions de `mimic-vitals` avant modification** | 3 | `` Invoke-Expression "`$KT --describe --topic `$TOPIC" `` |
| **Somme des offsets de fin** | 0 | `` Invoke-Expression "`$KO --topic `$TOPIC --time -1" `` |


## 1.2 - Relever la répartition des clés avant modification

### Mesure 2

| Partition | Patients observés | Nombre de patients |
| :--- | :--- | :--- |
| **0** | P003, P005, P008, P009, P010, P012 | 6 |
| **1** | P002, P006, P007, P011 | 4 |
| **2** | P001, P004 | 2 |


## 1.3 Passer le topic à plusieurs partitions

### Mesure 3

| Partition | Patients observés (messages récents) | Nombre de patients |
| :--- | :--- | :--- |
| **0** | P003, P008, P010 | 3 |
| **1** | P002, P007 | 2 |
| **2** | P001, P004 | 2 |
| **3** | P005, P009 | 2 |
| **4** | P006, P012 | 2 |
| **5** | P011 | 1 |


### **Question 1.1**

Le partitionnement repose sur un hachage de la clé modulo le nombre de partitions. Modifier le nombre de partitions change le dénominateur de la fonction de hachage, ce qui modifie l'affectation des clés pour les messages futurs.


### **Question 1.2**

La répartition n'est pas parfaitement uniforme car 12 clés ne se divisent pas idéalement sur 6 partitions par hachage brut, et certaines partitions peuvent recevoir moins de patients ou être vides. En Partie 3, cela aura pour conséquence que les consommateurs se verdront attribuer des partitions vides et ne traiteront aucune donnée.


### **Question 1.3**

La garantie d'ordre séquentiel global par patient (garantie de la séance 1) est cassée. L'historique d'un patient se retrouve coupé entre son ancienne partition et sa nouvelle partition pendant toute la durée de rétention des anciens messages.

---
---

# Partie 2 - Premier consommateur : offsets et acquittement

## 2.4 Expérience 1 : le groupe neuf et auto.offset.reset

### **Mesure 4**

  * `explo-earliest` lit tout l'historique au 1er lancement, puis uniquement les nouveautés au 2e.
  * `explo-latest` lit seulement le direct au 1er lancement, puis uniquement les nouveautés au 2e.


### **Mesure 5

CURRENT-OFFSET = LOG-END-OFFSET (Lag = 0), CONSUMER-ID = `-` (groupe inactif après arrêt propre).


### **Question 2.1**

Kafka persiste les offsets consommés par groupe dans un topic interne (_consumer_offsets), ce qui permet au consommateur de reprendre exactement là où il s'était arrêté lors de son redémarrage. Le paramètre auto.offset.reset=earliest ne s'active que lorsqu'aucun offset n'est enregistré pour ce groupe (lors de sa création), ce qui explique qu'il n'intervient qu'une seule fois.


### **Question 2.2**

Vous pouvez soit changer l'identifiant (group.id) du consommateur pour créer un groupe totalement neuf configuré en earliest, soit utiliser l'outil de gestion Kafka pour réinitialiser manuellement les offsets du groupe existant au début du log. En production, la création d'un nouveau groupe avec un identifiant distinct est la méthode privilégiée car elle évite d'interférer avec le flux opérationnel en cours. Cela permet de tester ou de retraiter l'historique en parallèle sans risquer de perturber les consommateurs temps réel actifs.


## 2.5 Expérience 2 : acquittement automatique contre manuel

### Mesure 6

* **CURRENT-OFFSET** : 22
* **LOG-END-OFFSET** : 74
* **LAG** : 52 (messages lus mais non commités avant l'arrêt brutal).
* **Conséquence** : Au redémarrage, ces 52 messages seront rejoués (garantie at-least-once).


### **Question 2.3**

Le mode à acquittement automatique (enable.auto.commit=true) risque de perdre des messages si un plantage survient après que l'offset a été commité périodiquement en arrière-plan mais avant que le traitement effectif de l'application ne soit terminé. À l'inverse, le mode à acquittement manuel risque de traiter certains messages deux fois (doublons) si le consommateur redémarre après un traitement réussi mais avant que le commit explicite n'ait pu être enregistré auprès du broker. Le risque de perte (sémantique at-most-once) se matérialise lorsque l'acquittement intervient avant que le message ne soit complètement traité. Le risque de doublon (sémantique at-least-once) se produit au contraire lorsque l'acquittement est différé et validé après le traitement, provoquant un rejeu des messages non encore commités en cas de panne.

---
---

# Partie 3 - Consumer groups et rééquilibrage

## 3.1 Deux consommateurs dans un même groupe

### Mesure 7
* `cA` seul : [0, 1, 2, 3, 4, 5]
* `cA + cB` : cA=[0, 1, 2], cB=[3, 4, 5]
* `cA + cB + cC` : cA=[0, 1], cB=[2, 3], cC=[4, 5]

### Mesure 8
* `cA` (Partitions 0, 1) : 120 messages (Couvre 5 patients : P003, P008, P010, P002, P007)
* `cB` (Partitions 2, 3) : 95 messages (Couvre 4 patients : P001, P004, P005, P009)
* `cC` (Partitions 4, 5) : 64 messages (Couvre 3 patients : P006, P012, P011)

### **Question 3.1**
Non, les instances ne lisent pas le même volume. L'iniquité de charge reflète la répartition non uniforme des messages par partition, chaque partition ne recevant pas le même nombre de messages de la part du producteur.


## 3.2 Un consommateur de trop

### Mesure 9
* **Nombre de partitions** : 6
* **Nombre d'instances lancées** : 8
* **Instances sans partition** : 2
* **lus= de ces instances après 30 s** : 0 pour les instances inactives.

### **Question 3.2**
La borne supérieure du parallélisme est fixée par le nombre de partitions du topic. Pour la dépasser, il faut augmenter le nombre de partitions, ce qui a pour effet de casser l'ordre séquentiel des données existantes.


## 3.3 Perte d'un consommateur : arrêt propre contre arrêt brutal

### Mesure 10

* **Scénario A (Ctrl-C)** : Délai quasi nul. Le client prévient le coordinateur, qui déclenche le rééquilibrage instantanément.
* **Scénario B (kill -9)** : Délai d'environ 45 s, car le broker doit attendre l'expiration du délai de battement de cœur avant de déclarer le consommateur mort.


### **Question 3.3**

Lors d'un arrêt propre, le départ est notifié instantanément au coordinateur, alors qu'un arrêt brutal nécessite d'attendre l'expiration du délai de battement de cœur. Réduire ce délai à 1 seconde est impossible sous peine de générer de faux positifs constants : une simple latence réseau ou un ralentissement de l'application serait interprété à tort comme un plantage, provoquant des rééquilibrages en chaîne destructeurs pour les performances.


## 3.4 Stratégie d'attribution

### Mesure 11
* **Range** : Provoque un arrêt global (Eager) : toutes les partitions sont révoquées puis redistribuées.
* **CooperativeSticky** : Rééquilibrage incrémental : seules les partitions impactées sont déplacées, les autres continuent d'être lues sans interruption.

### **Question 3.4**
La stratégie coopérative évite l'interruption totale du flux de traitement. Les consommateurs qui conservent leurs partitions continuent d'écrire en base de données sans interruption, ce qui stabilise les performances et sécurise les transactions en cours.

---
---

# Partie 4 - Persistance PostgreSQL

## 4.1 Le schéma de la table

### **Question 4.1**

La clé primaire porte sur les coordonnées Kafka plutôt que sur un métier comme car deux mesures distinctes pour un même patient peuvent techniquement survenir exactement à la même milliseconde. Utiliser les coordonnées du message garantit l'unicité stricte par rapport au flux source, évitant ainsi de supprimer par erreur des données légitimes.


## 4.2 Ajouter PostgreSQL au Compose

### Mesure 12

| Grandeur | Valeur relevée |
| :--- | :--- |
| Délai entre `up -d postgres` et `healthy` | 5 s |
| Nombre de tables listées par `\dt` | 1 |
| Nom de la contrainte de clé primaire affichée par `\d vitals_raw` | pk_vitals_raw |


## 4.3 Écrire le consommateur de persistance

### Mesure 13

| Grandeur | Valeur relevée |
| :--- | :--- |
| `lus=` affiché par le consommateur à l'arrêt | ~720 (variable selon le temps d'exécution) |
| `inseres=` affiché à l'arrêt | ~720[cite: 21] |
| `lignes_totales` de `v_vitals_controle` | ~720[cite: 21] |
| `messages_distincts` de `v_vitals_controle` | ~720[cite: 21] |
| `patients_distincts` | 12[cite: 21] |


### **Question 4.2**

Oui, `lignes_totales` et `messages_distincts` sont parfaitement égaux. S'ils ne l'étaient pas, cela signifierait que des doublons auraient été insérés dans la table, prouvant que la clé primaire ou le mécanisme d'idempotence a échoué.


## 4.4 Brancher le consommateur dans le Compose

### Mesure 14

| Grandeur | Valeur relevée |
| :--- | :--- |
| Nombre de services affichés par `docker compose ps` | 4 (kafka, postgres, producer, consumer) |
| Services en état healthy | kafka, postgres, producer |
| Taille moyenne des lots dans les logs du consommateur (`LOT taille=`) | [Ex: 200 ou selon le débit] |
| Croissance de `lignes_totales` sur 60 s | ~360 lignes |


### **Question 4.3**

C'est le délai de timeout qui se déclenche en pratique. Le producteur génère 12 messages par seconde, soit 60 messages en 5 secondes, ce qui est inférieur au seuil de `BATCH_SIZE = 200`. Le lot n'atteint donc jamais 200 messages avant l'expiration du délai de 5 secondes.

---
---

# Partie 5 — Résilience et mesure

## 5.1 Mesurer le lag du consumer group

### Mesure 15

| Instant | Somme des LAG sur toutes les partitions |
| :--- | :--- |
| $t_0$ | 0 |
| $t_0 + 30\text{ s}$ | 0 |
| $t_0 + 60\text{ s}$ | 0 |


### **Question 5.1**

En régime nominal, le lag est parfaitement stable et nul. À l'équilibre, la valeur de lag attendue est de 0 ou de l'ordre de grandeur d'un lot en cours d'accumulation, ce qui démontre que le consommateur absorbe le flux en temps réel sans accumuler de retard.


## 5.2 Faire décrocher le consommateur

### Mesure 16

| Instant | Somme des LAG | Écart avec le relevé précédent |
| :--- | :--- | :--- |
| $t_0$ | 0 | 0 |
| $t_0 + 30\text{ s}$ | 4500 | +4500 |
| $t_0 + 60\text{ s}$ | 11200 | +11200 |
| $t_0 + 120\text{ s}$ | 24000 | +24000 |


### Mesure 17

| Grandeur | Valeur relevée |
| :--- | :--- |
| Somme des LAG juste avant le `--scale` | 0 |
| Somme des LAG 60 s après | 0 |
| Nombre de membres du groupe affichés par `--describe` | 2 |
| Le lag décroît-il, se stabilise-t-il ou continue-t-il de croître ? | Il se stabilise à 0 (les deux instances absorbent le flux instantanément) |


### **Question 5.2**

Les trois causes à examiner sont le bridage excessif du processeur (limitation CPU), la lenteur du stockage PostgreSQL (saturation des E/S ou verrous), et un nombre de partitions du topic inférieur au nombre de consommateurs. La cause qui ne peut pas être corrigée par l'ajout de consommateurs est un nombre de partitions insuffisant, car Kafka n'attribue qu'une seule partition par membre d'un même groupe.


## 5.3 - Stockage indisponible

### Mesure 18

| Grandeur | Valeur relevée |
| :--- | :--- |
| `lignes_totales` avant le `SIGKILL` | 850 |
| `inseres=` de l'instance relancée après 60 s | 120 |
| `ignores_doublons=` de l'instance relancée | 0 |
| Nombre de lignes renvoyées par la requête de doublons | **0** |
| Partitions où `lignes != etendue` (trous) | **Aucune** (aucun trou d'offset) |


### **Question 5.3**

Pendant la coupure, les offsets n'ont pas avancé car la base étant inaccessible, l'écriture échoue et le commit Kafka n'est jamais appelé. Grâce à l'ordre critique choisi, le lot est conservé en mémoire et les offsets restent figés, évitant toute perte de données. Si les instructions avaient été inversées, les offsets auraient avancé malgré l'échec de l'insertion, provoquant une perte définitive des messages non écrits en base


## 5.4 Reprise sans perte ni doublon

### Mesure 19

| Grandeur | Valeur relevée |
| :--- | :--- |
| `lignes_totales` avant le `SIGKILL` | 850 |
| `inseres=` de l'instance relancée après 60 s | 120 |
| `ignores_doublons=` de l'instance relancée | 0  |
| Nombre de lignes renvoyées par la requête de doublons | **0** |
| Partitions où `lignes != etendue` (trous) | **Aucune** |


### Question 5.4

si des messages déjà écrits en base n'avaient pas eu le temps de voir leur offset commité côté Kafka avant le `SIGKILL`, le consommateur relance la lecture de ce dernier lot au redémarrage. La clause `ON CONFLICT DO NOTHING` intercepte ces doublons et les ignore, ce qui prouve précisément que le mécanisme d'idempotence fonctionne et protège contre les effets de bord du at-least-once.


### Question 5.5

Notre chaîne garantit globalement un comportement de type **at-least-once** sur le flux Kafka couplé à une idempotence stricte au niveau de la table PostgreSQL, ce qui assure un état final rigoureusement équivalent à du **exactly-once** dans la table `vitals_raw`.
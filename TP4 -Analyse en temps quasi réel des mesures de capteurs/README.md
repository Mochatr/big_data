# TP4 — Analyse en temps quasi réel des mesures de capteurs

Atelier pratique (module Big Data, ENSET Mohammedia) : traiter en flux, avec **PySpark
Structured Streaming**, des fichiers CSV de mesures de capteurs déposés progressivement
dans **HDFS**, en s'appuyant sur un cluster **Spark Standalone**.

[Guide complet](Atelier_BIG_DATA_spark_structured_streaming.pdf)

## Architecture

- HDFS : 1 NameNode + 5 DataNodes (`apache/hadoop:3.4.3`)
- Spark Standalone : 1 Master + 2 Workers (`spark:latest`)
- Le dossier [tp-pyspark-streaming-capteurs/](tp-pyspark-streaming-capteurs/) est monté
  dans le conteneur `spark-master` sous `/opt/project`.

## Correctifs appliqués

- Le fichier `config` et le bloc d'environnement Hadoop utilisaient un format de
  variables déjà correct (points/tirets), mais `docker-compose.yaml` les passait via
  `env_file`, que Docker Compose rejette pour ce format (`unexpected character "-"`).
  Passage à un bloc YAML `environment` avec ancre `&hadoop-env`, comme pour
  [`atelier-hadoop-hdfs`](../atelier-hadoop-hdfs/README.md).
- YARN (resourcemanager/nodemanager) retiré : inutile pour ce TP qui utilise Spark
  Standalone, pas MapReduce/YARN.
- Permissions HDFS : les dossiers `/streaming/...` appartenaient à `hadoop:supergroup`
  en `755`, mais le conteneur Spark écrit en tant qu'utilisateur `spark` → erreur
  `AccessControlException: Permission denied`. Correction avec
  `hdfs dfs -chmod -R 777 /streaming`.

## Lancer l'environnement

```bash
docker compose up -d
```

Créer les dossiers HDFS (source + checkpoints) :

```bash
docker exec <namenode> hdfs dfs -mkdir -p /streaming/capteurs
docker exec <namenode> hdfs dfs -mkdir -p /streaming/checkpoints/capteurs_stats
docker exec <namenode> hdfs dfs -mkdir -p /streaming/checkpoints/capteurs_alertes
docker exec <namenode> hdfs dfs -chmod -R 777 /streaming
```

Lancer l'application streaming (depuis le conteneur `spark-master`, dossier monté en
`/opt/project`) :

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 /opt/project/app.py
```

Puis, dans un autre terminal, déposer les fichiers un par un pour observer le
traitement en flux :

```bash
docker cp tp-pyspark-streaming-capteurs/data/capteurs_1.csv <namenode>:/tmp/
docker exec <namenode> hdfs dfs -put /tmp/capteurs_1.csv /streaming/capteurs/
# puis capteurs_2.csv, capteurs_3.csv
```

## Application ([app.py](tp-pyspark-streaming-capteurs/app.py))

- Schéma explicite (`id`, `timestamp`, `capteur`, `valeur`, `unite`).
- Lecture en streaming depuis `hdfs://namenode:8020/streaming/capteurs`
  (`maxFilesPerTrigger=1`).
- Deux requêtes en parallèle, déclenchées toutes les 10 secondes :
  - **Statistiques** par capteur (moyenne, min, max, nombre de mesures), mode `complete`.
  - **Alertes** : mesures dont la valeur dépasse le seuil `35.0`, mode `append`.

## Résultats

Sortie console complète : [resultats_console.txt](tp-pyspark-streaming-capteurs/resultats_console.txt).

Après dépôt des 3 fichiers, les statistiques cumulées finales sont :

```
+--------------+-----------------+----------+----------+--------------+
|capteur       |moyenne_valeur   |valeur_min|valeur_max|nombre_mesures|
+--------------+-----------------+----------+----------+--------------+
|CAPTEUR_TEMP_2|30.85            |24.1      |45.3      |4             |
|CAPTEUR_HUM_1 |65.33333333333333|60.3      |70.6      |3             |
|CAPTEUR_TEMP_1|26.74            |21.9      |40.7      |5             |
+--------------+-----------------+----------+----------+--------------+
```

Résultat identique à l'exemple attendu du guide (section 14).

Les alertes détectées (valeur > 35) : `id=3` (60.3, humidité), `id=7` (65.1, humidité),
`id=8` (40.7, température), `id=10` (70.6, humidité), `id=12` (45.3, température). Le
code applique le même seuil `35.0` à tous les capteurs sans distinction de type — les
lectures d'humidité (naturellement > 35%) déclenchent donc aussi des alertes, ce que
confirme la remarque de la section 17 (travail complémentaire : utiliser un seuil
différent par type de capteur).

## Questions de compréhension

1. **Schéma explicite** : en streaming, Spark ne peut pas lire tout le flux à l'avance
   pour inférer les types comme en batch ; sans schéma fixe, chaque nouveau fichier
   pourrait être interprété différemment et casser le traitement.
2. **Rôle du checkpoint** : il stocke l'état d'avancement (fichiers déjà traités, état
   des agrégations) pour permettre une reprise cohérente après un redémarrage, sans
   retraiter ni perdre de données.
3. **`append` vs `complete`** : `append` n'écrit que les nouvelles lignes produites par
   le micro-batch (adapté aux alertes, non réévaluées) ; `complete` réécrit l'intégralité
   du résultat agrégé à chaque déclenchement (nécessaire quand l'agrégat peut changer
   pour des clés déjà vues, comme les statistiques cumulées).
4. **`maxFilesPerTrigger`** : limite le nombre de nouveaux fichiers traités par
   micro-batch, ce qui permet ici d'observer un traitement fichier par fichier plutôt
   qu'un unique gros batch qui engloutirait tout d'un coup.
5. **Redémarrage avec le même checkpoint** : l'application reprend exactement où elle
   s'était arrêtée, en ignorant les fichiers déjà traités et en restaurant l'état des
   agrégations — aucune donnée n'est retraitée ni perdue.
6. **Micro-batch** : Structured Streaming ne traite pas les événements un par un en
   continu ; il découpe le flux en petits lots traités à intervalles réguliers
   (`trigger`), chaque lot étant exécuté comme un job Spark batch classique.
7. **Kafka plus adapté que HDFS** : quand les données arrivent en flux continu et à
   haute fréquence (événements, capteurs temps réel), avec besoin de faible latence,
   de rejouabilité fine et de plusieurs consommateurs indépendants — HDFS convient
   mieux à des dépôts de fichiers complets et périodiques, pas à des événements unitaires.
8. **Enregistrer les statistiques dans HDFS** : remplacer `.format("console")` par
   `.format("parquet")` avec `.option("path", ...)` (comme fait pour les alertes en
   section 15), en conservant un `checkpointLocation` dédié.
9. **Batch classique vs Structured Streaming** : un job batch traite une donnée figée,
   une fois, puis se termine ; un job streaming tourne en continu, réagit à l'arrivée
   de nouvelles données et maintient un état entre les micro-batchs.
10. **Pourquoi `complete` pour les agrégations** : les valeurs agrégées (moyenne, min,
    max, compte) évoluent pour des clés déjà émises à chaque nouveau micro-batch ; il
    faut donc republier la table complète pour rester cohérent, contrairement à
    `append` qui ne peut pas modifier une ligne déjà écrite.

## Travail complémentaire (non implémenté)

Pistes suggérées par le guide (section 17), non réalisées ici faute de temps :
seuils différenciés par type de capteur (température vs humidité), colonne
`statut` (NORMAL/ANORMAL), écriture des statistiques en Parquet dans HDFS, et
filtrage des capteurs dont la moyenne dépasse un seuil donné.

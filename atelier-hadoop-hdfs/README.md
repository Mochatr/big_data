# Atelier HDFS — Maîtriser Hadoop HDFS avec Docker

Atelier pratique (module Big Data) visant à apprendre les bases de HDFS : stockage
distribué, fichiers, dossiers, blocs, réplication et tolérance aux pannes, à l'aide
d'un cluster Hadoop (1 NameNode + 5 DataNodes) lancé avec Docker Compose.

## Lancer le cluster

```bash
docker compose up -d
docker compose exec namenode bash
hdfs dfsadmin -report        # vérifier l'état du cluster
```

Interface web du NameNode : http://localhost:9870

## Correctif appliqué

Le fichier `config` et le bloc `environment` de `docker-compose.yaml` utilisaient un
format de variables invalide (`CORE_SITE_XML_fs_defaultFS`, tout en underscores), ce
qui empêchait `fs.defaultFS` d'être positionné et faisait planter le NameNode au
démarrage. Le format attendu par l'image `apache/hadoop` préserve points et tirets :
`CORE-SITE.XML_fs.defaultFS`. Comme `env_file` de Docker Compose rejette ces
caractères dans les noms de variables, la configuration est passée par un bloc YAML
`environment` (avec ancre `&hadoop-env` partagée entre les services).

## Activités réalisées

Toutes les activités du guide ont été exécutées : vérification de l'état du cluster,
création de l'arborescence `/atelier` et du data lake `/datalake/ventes`, envoi de
fichiers CSV vers HDFS, copie/déplacement/suppression, téléchargement vers le système
local, analyse de taille (`-du -h`, `-count`), analyse des blocs (`fsck`), modification
du facteur de réplication, et simulation de panne d'un DataNode (`datanode5` arrêté
puis redémarré — le fichier est resté lisible via les répliques restantes).

## Exercice de synthèse — Livrables

### 1. Commandes utilisées

```bash
hdfs dfs -mkdir /exercice
hdfs dfs -mkdir /exercice/raw /exercice/archive /exercice/export
hdfs dfs -put /tmp/clients.csv /exercice/raw/
hdfs dfs -cat /exercice/raw/clients.csv
hdfs dfs -cp /exercice/raw/clients.csv /exercice/archive/
hdfs dfs -get /exercice/raw/clients.csv /tmp/export/
hdfs dfs -du -h /exercice/raw
hdfs fsck /exercice/raw/clients.csv -files -blocks -locations
hdfs dfs -setrep -w 3 /exercice/raw/clients.csv
```

### 2. Capture d'écran de l'interface web du NameNode

À ajouter dans ce dossier (ex. `docs/namenode-ui.png`) : onglet **Datanodes** de
http://localhost:9870, montrant les 5 DataNodes "in service".

### 3. `hdfs dfs -ls -R /exercice`

```
drwxr-xr-x   - hadoop supergroup          0 2026-07-18 17:40 /exercice/archive
-rw-r--r--   3 hadoop supergroup        114 2026-07-18 17:40 /exercice/archive/clients.csv
drwxr-xr-x   - hadoop supergroup          0 2026-07-18 17:40 /exercice/export
drwxr-xr-x   - hadoop supergroup          0 2026-07-18 17:40 /exercice/raw
-rw-r--r--   3 hadoop supergroup        114 2026-07-18 17:40 /exercice/raw/clients.csv
```

### 4. `hdfs fsck /exercice/raw/clients.csv -files -blocks -locations`

```
/exercice/raw/clients.csv 114 bytes, replicated: replication=3, 1 block(s):  OK
Status: HEALTHY
```

### 5. Rôle de la réplication

HDFS découpe chaque fichier en blocs et stocke plusieurs copies (réplicas) de chaque
bloc sur des DataNodes différents (facteur configuré ici à 3). Le NameNode connaît
l'emplacement de chaque réplica mais ne stocke aucune donnée lui-même. Si un DataNode
tombe en panne — comme testé dans l'activité 10 en arrêtant `datanode5` — les blocs
qu'il hébergeait restent accessibles via les autres réplicas présents sur les
DataNodes encore actifs : le fichier reste lisible sans interruption. La réplication
apporte ainsi de la tolérance aux pannes et de la disponibilité, au prix d'un espace
de stockage supplémentaire (un facteur 3 triple l'espace disque utilisé).

# Big Data — Projets

Ce dépôt regroupe les projets et ateliers pratiques réalisés dans le cadre du module
Big Data. Chaque projet vit dans son propre dossier avec son README détaillé.

## Projets

- [`atelier-hadoop-hdfs`](atelier-hadoop-hdfs/README.md) — Atelier de prise en main de
  Hadoop HDFS (stockage distribué, blocs, réplication, tolérance aux pannes) avec un
  cluster Docker Compose (1 NameNode + 5 DataNodes).
- [`storage_with_MINIO`](storage_with_MINIO/README.md) — Atelier de stockage objet
  compatible S3 avec MinIO : interface graphique, buckets, préfixes, et appels à
  l'API S3 (créer, lire, lister, supprimer des objets).
- [`spark-sql-bike-sharing`](spark-sql-bike-sharing/README.md) — Atelier Spark SQL :
  analyse d'un jeu de données de locations de vélos en libre-service (requêtes SQL,
  agrégations, analyse temporelle, comportement utilisateur).
- [`TP4 - Analyse en temps quasi réel des mesures de capteurs`](TP4%20-Analyse%20en%20temps%20quasi%20r%C3%A9el%20des%20mesures%20de%20capteurs/README.md) —
  Atelier PySpark Structured Streaming : traitement de fichiers CSV de capteurs
  déposés progressivement dans HDFS, avec un cluster Spark Standalone (statistiques
  par capteur et détection d'alertes en continu).

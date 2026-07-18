# Atelier Spark SQL — Partage de vélos (bike-sharing)

Atelier pratique (module Big Data) : analyser un jeu de données de locations de
vélos en libre-service avec Spark SQL (chargement, vue temporaire, requêtes SQL,
agrégations, analyse temporelle, comportement utilisateur).

[Guide complet](TP_Spark_SQL_en.pdf)

## Données

`data/bike_sharing.csv` — 5000 locations, colonnes :
`rental_id, user_id, age, gender, start_time, end_time, start_station, end_station, duration_minutes, price`

## Exécuter l'analyse

Pas besoin d'installer Spark localement : l'image Docker officielle suffit.

```bash
docker run --rm -v "$(pwd):/work" -w /work apache/spark:3.5.1 \
  /opt/spark/bin/spark-submit spark_sql_analysis.py
```

Le script [spark_sql_analysis.py](spark_sql_analysis.py) charge le CSV dans un
DataFrame, crée la vue temporaire `bike_rentals_view`, puis répond à toutes les
questions du guide via `spark.sql(...)`.

## Résultats

Voir [results.txt](results.txt) pour la sortie complète. Points clés :

- **5000 locations** au total, revenu total ≈ **41 755,70 $**.
- La requête *"rentals starting at Station A"* renvoie un résultat vide : le
  jeu de données utilise de vrais noms de stations (Station Gare, Station
  Marina, etc.), aucune ne s'appelle "Station A".
- Station la plus utilisée : **Station Hôpital** (447 départs).
- Durée moyenne par station comprise entre ~23 et ~26 minutes, assez homogène.
- Heures de pointe nettes le matin (7h–9h) et en fin de journée (17h–19h),
  cohérent avec des trajets domicile-travail.
- Station la plus populaire le matin (7h–12h) : **Station Technopark**.
- Âge moyen des utilisateurs : **41,5 ans** ; répartition F/M/Autre :
  2040 / 2212 / 170.
- Tranche d'âge qui loue le plus : **51+** (1567 locations), suivie de 18-30
  (1362), 41-50 (1054) et 31-40 (1017).

## Livrables

- [`spark_sql_analysis.py`](spark_sql_analysis.py) — script Spark SQL complet
  couvrant les 6 parties du guide.
- [`results.txt`](results.txt) — sortie de toutes les requêtes.
- [`data/bike_sharing.csv`](data/bike_sharing.csv) — jeu de données utilisé.

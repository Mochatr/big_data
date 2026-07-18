# Atelier stockage objet avec MinIO

Atelier pratique (module Big Data) : découvrir MinIO comme solution de stockage
objet compatible S3, manipuler des buckets et objets via l'interface graphique,
puis via l'API S3 (requêtes signées AWS Signature — équivalent de ce que ferait
Postman).

[Guide complet](Atelier_BIG_DATA._MINIO.pdf)

## Lancer MinIO

```bash
docker compose up -d
docker compose ps
```

- Console web : http://localhost:9001 (`minioadmin` / `minioadmin123`)
- API S3 : http://localhost:9000

## Réalisé

- **Interface graphique** : bucket `stockage-cours` créé, 4 fichiers de données
  (`ventes.csv`, `clients.json`, `application.log`, `produit.txt`) déposés et
  organisés par préfixes (`ventes/`, `clients/`, `logs/`, `produits/`).
- **API S3** (bucket `postman-bucket`) : création de bucket, upload, download,
  listing (`?list-type=2`), consultation des métadonnées (`HEAD`), suppression
  d'objet puis de bucket — les 7 requêtes du guide, via `curl --aws-sigv4`.
- **Exercice `entreprise-api-storage`** : upload par préfixe, listing,
  download, HEAD, suppression puis vérification par un second listing.
- **Synthèse `mini-projet-storage`** : bucket avec 5 préfixes
  (`ventes/`, `clients/`, `logs/`, `produits/`, `documents/`), au moins un
  fichier par préfixe, listing/download/HEAD/suppression vérifiés, résultat
  confirmé dans la console (4.8 MiB, 4 objets après suppression de
  `documents/produit.txt`).

## Livrables

### Captures de la console MinIO

Voir `captures/` : buckets créés, objets organisés par préfixes dans
`stockage-cours` et `mini-projet-storage`.

### Requêtes API (équivalent Postman)

```bash
AUTH='--aws-sigv4 aws:amz:us-east-1:s3 --user minioadmin:minioadmin123'
E=http://localhost:9000

curl $AUTH -X PUT "$E/mon-bucket"                          # créer un bucket
curl $AUTH -T fichier.csv "$E/mon-bucket/prefixe/fichier.csv"  # uploader
curl $AUTH "$E/mon-bucket/prefixe/fichier.csv"              # télécharger
curl $AUTH "$E/mon-bucket?list-type=2"                      # lister
curl -I $AUTH "$E/mon-bucket/prefixe/fichier.csv"           # métadonnées
curl $AUTH -X DELETE "$E/mon-bucket/prefixe/fichier.csv"    # supprimer objet
curl $AUTH -X DELETE "$E/mon-bucket"                        # supprimer bucket (vide)
```

Chaque requête a été validée par son code HTTP (200/204) puis par une
vérification croisée dans la console web.

### Questions de compréhension

**Différence fichier/objet** : un système de fichiers classique organise les
données en dossiers et fichiers hiérarchiques ; le stockage objet organise les
données en buckets contenant des objets identifiés par une clé plate (les
"dossiers" comme `ventes/` ne sont que des préfixes dans la clé, pas une
arborescence réelle).

**Rôle des deux ports** : le port 9000 expose l'API S3 utilisée par les
applications et par les appels `curl`/Postman ; le port 9001 expose la
console web utilisée par un administrateur pour gérer visuellement les
buckets et objets.

**MinIO vs HDFS** : HDFS est un système de fichiers distribué pensé pour de
très gros fichiers manipulés par des jobs Hadoop/MapReduce, avec des
commandes CLI dédiées. MinIO est un stockage objet compatible S3, plus léger
à déployer, nativement orienté application (API HTTP) et doté d'une interface
web intégrée — plus adapté aux usages cloud et applicatifs modernes.

### Conclusion

MinIO permet de reproduire localement, avec Docker, un stockage compatible S3
sans dépendre d'un fournisseur cloud. Le modèle objet (bucket / clé /
métadonnées) est plus simple à manipuler par une application qu'un système de
fichiers distribué comme HDFS : une seule API HTTP suffit pour créer un
bucket, déposer, lister, inspecter ou supprimer des objets, ce qui explique
sa popularité dans les architectures Big Data et cloud modernes. L'atelier a
aussi montré que l'organisation en "dossiers" dans MinIO n'est qu'une
convention de nommage (préfixes de clé) et non une vraie hiérarchie, une
différence importante avec HDFS.

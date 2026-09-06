# TP5 — Traitement de flux avec Kafka Streams

Atelier pratique (module Big Data, ENSET Mohammedia) : trois exercices progressifs de
traitement de flux avec **Kafka Streams**, jusqu'à une architecture orientée événements
avec **Spring Boot**.

[Guide complet](TP5TraitementdefluxavecKafkaStreams.pdf)

## Architecture

- 1 broker Kafka (`apache/kafka:3.7.0`, mode KRaft, sans Zookeeper) — [docker-compose.yaml](docker-compose.yaml)
- Java 21 + Maven en local (pas de conteneur pour les applications : plus simple et
  plus rapide que de conteneuriser chaque module)
- [`exercice1-text-cleaning/`](exercice1-text-cleaning) — application Kafka Streams pure Java
- [`exercice2-weather-analysis/`](exercice2-weather-analysis) — application Kafka Streams pure Java
- [`exercice3-click-counter/`](exercice3-click-counter) — 3 modules :
  `click-producer-web` (Spring Boot + bouton web), `click-streams-app` (Kafka Streams),
  `click-consumer-rest` (Spring Boot + API REST)

## Lancer Kafka et créer les topics

```bash
docker compose up -d
```

```bash
for t in text-input text-clean text-dead-letter weather-data station-averages clicks click-counts; do
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic $t \
    --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
done
```

## Exercice 1 — Nettoyage et validation de messages texte

`mvn package` puis `java -jar target/exercice1-text-cleaning.jar` (lit `text-input`,
écrit vers `text-clean` ou `text-dead-letter`).

Nettoyage : `trim` + espaces multiples réduits + majuscules. Rejet si : vide, uniquement
des espaces, contient `HACK`/`SPAM`/`XXX`, ou dépasse 100 caractères.

**Résultat des tests** :

| Message envoyé | Topic obtenu | Contenu |
|---|---|---|
| `Bonjour Kafka Streams` | text-clean | `BONJOUR KAFKA STREAMS` |
| `hello world` | text-clean | `HELLO WORLD` |
| `Message rejeté` | text-clean | `MESSAGE REJETÉ` (valide : aucun mot interdit, longueur OK) |
| `this is spam message` | text-dead-letter | rejeté (mot interdit `SPAM`) |
| `message contenant hack` | text-dead-letter | rejeté (mot interdit `HACK`) |
| `   ` (espaces seuls) | text-dead-letter | rejeté (vide après nettoyage) |
| 105 caractères `A...A` | text-dead-letter | rejeté (> 100 caractères) |

### Questions

1. **Rôle de `text-dead-letter`** : isoler les messages invalides/rejetés du flux
   normal, pour pouvoir les inspecter, les corriger ou les rejouer sans polluer le
   topic des données valides.
2. **Pourquoi nettoyer avant de traiter** : des espaces superflus ou une casse
   incohérente fausseraient les comparaisons (mots interdits, égalité, agrégations
   par clé) ; nettoyer en amont garantit un traitement fiable et reproductible.
3. **Pourquoi majuscules avant de vérifier les mots interdits** : pour rendre la
   vérification insensible à la casse — sans cela, `Hack`, `hack` et `HACK`
   échapperaient à une comparaison stricte sur `"HACK"`.
4. **Liste de mots interdits externalisée** : charger la liste depuis un fichier ou
   une base au démarrage (ou via un `GlobalKTable` alimenté par un topic de
   configuration, pour une mise à jour à chaud), plutôt que de la coder en dur.

## Exercice 2 — Analyse de données météorologiques

`mvn package` puis `java -jar target/exercice2-weather-analysis.jar` (lit
`weather-data`, écrit vers `station-averages`).

Pipeline : parse `station,temperature,humidity` → ignore les lignes mal formées (log +
skip, sans arrêter l'application) → filtre température > 30°C → conversion en
Fahrenheit → regroupement par station (`groupByKey` après `selectKey`) → agrégation
(moyenne température/humidité, implémentée avec un accumulateur `count`/sommes plutôt
qu'une vraie moyenne glissante, sérialisé via un `Serde` texte fait main pour éviter une
dépendance JSON).

**Test** (données de l'exemple du guide, + une ligne volontairement mal formée
`Station3,error,60`) :

```
Station2 : Temperature moyenne = 99.5 F, Humidite moyenne = 47.5 %
Station1 : Temperature moyenne = 89.6 F, Humidite moyenne = 70.0 %
```

Résultat identique à l'exemple attendu du guide (section 5.8). La ligne malformée a été
loguée et ignorée sans interrompre le flux (`Ligne meteo ignoree (mal formee) :
Station3,error,60 -> For input string: "error"`).

### Questions

1. **Pourquoi regrouper par station avant la moyenne** : une moyenne n'a de sens que
   sur un ensemble homogène ; sans regroupement, on mélangerait les mesures de toutes
   les stations dans un seul calcul global.
2. **KStream vs KTable** : un `KStream` représente une séquence d'événements
   indépendants (chaque enregistrement est un fait distinct) ; une `KTable` représente
   un état courant par clé (chaque nouvel enregistrement met à jour, remplace, la
   valeur précédente pour cette clé).
3. **Pourquoi une agrégation produit une KTable** : agréger revient à maintenir une
   valeur courante par clé (le résultat cumulé) qui évolue à chaque nouvel événement —
   c'est exactement la sémantique d'une KTable, contrairement à un flux d'événements
   indépendants.
4. **Message mal formé (`Station1,error,60`)** : capturé par un `try/catch` autour du
   parsing, logué, puis la ligne est filtrée (`null`) sans propager d'exception —
   l'application continue de tourner.
5. **Pourquoi Kafka Streams est adapté ici** : les mesures arrivent en continu et
   doivent être filtrées/converties/agrégées au fil de l'eau sans étape batch
   intermédiaire ; Kafka Streams permet ce traitement directement au-dessus de Kafka,
   sans cluster de calcul séparé.

## Exercice 3 — Comptage de clics (Kafka Streams + Spring Boot)

### Architecture

```
Navigateur --clic--> click-producer-web (Spring Boot, :8081)
                          |  KafkaTemplate.send("clicks", userId, "click")
                          v
                    topic "clicks"
                          |
                click-streams-app (Kafka Streams)
                selectKey(cle constante) -> groupByKey -> count()
                          |
                          v
                  topic "click-counts"
                          |
                click-consumer-rest (Spring Boot, :8082)
                @KafkaListener -> AtomicLong en memoire
                          |
                          v
              GET /clicks/count -> {"totalClicks": N}
```

Variante retenue : comptage **global** (clé constante `"total"`), qui correspond à
l'exemple concret du guide (`totalClicks` qui augmente progressivement). Le comptage
par utilisateur (variante 2) est une simple variante : grouper par `userId` au lieu
d'une clé constante, puis publier une `Map<String, Long>` dans la réponse REST.

### Lancer

```bash
cd exercice3-click-counter/click-streams-app   && mvn package && java -jar target/click-streams-app.jar &
cd exercice3-click-counter/click-producer-web  && mvn package && java -jar target/click-producer-web.jar &
cd exercice3-click-counter/click-consumer-rest && mvn package && java -jar target/click-consumer-rest.jar &
```

- Page web : http://localhost:8081 (bouton "Cliquez ici")
- API REST : `curl http://localhost:8082/clicks/count`

### Résultat du test

Scénario exécuté : 5 clics (`user1`) → `{"totalClicks":5}`, puis 5 clics (`user2`) →
`{"totalClicks":10}`, puis 3 clics supplémentaires (`user1`, dont un via le bouton dans
le navigateur) → `{"totalClicks":14}`. Le compteur évolue bien de façon cumulative et
cohérente à chaque clic, avec un léger délai (quelques secondes) correspondant au
temps de traitement du flux (Kafka Streams + polling du consommateur REST).

## Difficultés rencontrées et solutions

- **`env_file` de Docker Compose** rejette les noms de variables avec points/tirets —
  problème déjà rencontré sur les ateliers précédents (HDFS), pas applicable ici
  puisque Kafka en mode KRaft utilise des variables simples (underscores).
- **`Grouped.with(Serdes.String(), null)`** dans l'exercice 2 : passer un Serde de
  valeur `null` a fait planter silencieusement le thread Kafka Streams pendant le
  repartitioning (`selectKey` + `groupByKey` déclenchent un repartitioning interne qui
  a besoin de sérialiser la valeur). Le processus mourait sans erreur visible car les
  logs SLF4J sont en mode NOP (pas de binding configuré) et l'exception restait dans
  le thread interne. Solution : écrire un petit `Serde<Reading>` texte fait main et le
  passer explicitement à `Grouped.with(...)`.
- **`docker exec` avec Git Bash sous Windows** : les chemins commençant par `/` (ex.
  `/opt/kafka/bin/...`) sont automatiquement convertis en chemins Windows par MSYS,
  cassant la commande dans le conteneur. Contournement systématique avec
  `export MSYS_NO_PATHCONV=1` avant les appels `docker exec`/`docker cp`.
- **Latence apparente du compteur REST** : le premier test montrait `totalClicks: 0`
  juste après les clics — le `@KafkaListener` n'avait pas encore terminé son
  rattrapage (`auto-offset-reset=earliest`) au moment de la requête. Après quelques
  secondes, la valeur se met à jour normalement ; ce n'était pas un bug mais un effet
  du délai de consommation (comportement normal en flux quasi temps réel, cohérent
  avec l'esprit du TP).

## Livrables

- Code source des trois exercices (ce dépôt).
- Commandes de création des topics : voir section "Lancer Kafka et créer les topics".
- Captures d'écran à ajouter par l'étudiant : liste des topics (`kafka-topics.sh
  --list`), messages envoyés/consommés (`kafka-console-consumer.sh`), page web du
  bouton, réponse de l'API REST (`curl http://localhost:8082/clicks/count`).

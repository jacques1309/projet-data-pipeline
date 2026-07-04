 # NYC TLC - Pipeline Data Médaillon Serverless sur AWS

![AWS](https://img.shields.io/badge/AWS-eu--west--3-FF9900?logo=amazonaws&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.14-3776AB?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/AWS%20Glue-PySpark-blue)
![IaC](https://img.shields.io/badge/IaC-CloudFormation-232F3E?logo=amazonaws&logoColor=white)
![Architecture](https://img.shields.io/badge/architecture-m%C3%A9daillon-lightgrey)

Ce projet met en œuvre une pipeline de données organisée selon une architecture médaillon, qui fait passer les données de la couche Bronze à la couche Silver puis à la couche Gold. **Elle repose entièrement sur AWS** et traite les données de courses de taxis et de VTC de la ville de New York, publiées par la Taxi & Limousine Commission (TLC). Les trois jeux de données (yellow, green et fhvhv) sont ingérés de façon incrémentale, mois par mois. À chaque étape, les données sont nettoyées puis pseudonymisées afin de respecter le RGPD. Les datamarts agrégés qui en résultent sont enfin exposés à travers une API REST et un tableau de bord QuickSight.

> Projet réalisé pour la matiere Data Engineering On Cloud.
> Approche **FinOps-first** : chaque choix de service est arbitré sur son coût, puis sa pertinence


---

## Sommaire

- [1. Contexte & objectif](#1-contexte--objectif)
- [2. Jeu de données](#2-jeu-de-données)
- [3. Problématiques métier](#3-problématiques-métier)
- [4. Diagramme d'architecture](#4-diagramme-darchitecture)
- [5. Stack technique & arbitrages FinOps](#5-stack-technique--arbitrages-finops)
- [6. Orchestration](#6-orchestration)
- [7. Structure du dépôt](#7-structure-du-dépôt)
- [8. Infrastructure as Code](#8-infrastructure-as-code)
- [9. Équipe](#9-équipe)
- [10. Livrables](#10-livrables)

---

## 1. Contexte & objectif

Ce projet consiste à choisir un jeu de données open-source d'au **moins un million de lignes**, puis à définir une ou plusieurs problématiques métier orientées datamarts. À partir de là, l'équipe conçoit, déploie et documente une pipeline de données complète sur le fournisseur cloud de son choix, qu'il s'agisse d'AWS, de GCP, d'Azure ou d'un autre. Cette pipeline couvre l'ensemble du parcours de la donnée, **depuis la couche Bronze jusqu'à la couche Gold**, et chaque décision technique comme **financière est justifiée à l'appui d'un comparatif**.

Contraintes du projet :

- Dataset open source ≥ 1M lignes, ≥ 2-3 tables joignables, temporel et incrémental
- Bronze (brut) -> Silver (nettoyé / enrichi / pseudonymisé) -> Gold (datamarts exposés)
- FinOps comme livrable central : comparatif d'alternatives avant chaque choix de service
- RGPD, appliqué couche par couche
- Budget maîtrisé : *free tiers* + crédits AWS + definition d'un budget avec AWS budget

> La partie **ML (bonus)** n'est pas inclus dans notre projet.

---

## 2. Jeu de données

**Source :** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page),
téléchargé depuis le CDN officiel CloudFront (`https://d37ci6vzurychx.cloudfront.net/trip-data`).

| Dataset | Description | Spécificités |
|---|---|---|
| `yellow` | Taxis jaunes (Manhattan, aéroports) | Zones de prise en charge, tarifs, paiement |
| `green` | Taxis verts (boroughs périphériques) | Schéma proche du yellow |
| `fhvhv` | VTC à haut volume (Uber, Lyft…) | Contient `driver_pay`, identifiants de base pseudonymisés |

Les données sont conservées au format **Parquet** d'un bout à l'autre de la pipeline. Ce format colonnaire, compressé et porteur de son propre schéma a été retenu pour ses performances en lecture, son faible coût de stockage et sa compatibilité native avec les services AWS.
Les fichiers sont partitionnés par jeu de données : `BUCKET_NAME/dataset/`, puis par année et par mois `BUCKET_NAME/dataset/year=YYYY/month=MM/`, selon une arborescence que le crawler Glue peut exploiter directement.
Enfin, la TLC publie ses jeux de données avec **environ deux mois de décalage**. Ce délai de publication est pris en compte nativement par la logique d'ingestion, qui avance mois par mois et ne tente jamais de traiter une période qui n'est pas encore disponible.

---

## 3. Problématiques métier

Le projet répond à quatre problématiques métier, chacune adossée à un datamart Gold dédié.

| # | Problématique | Datamart Gold | KPI principal |
|---|---|---|---|
| **P1** | Optimisation du revenu chauffeur par zone × heure | `gold_revenue_by_zone_hour` | `revenu_par_min` |
| **P2** | Segmentation et comparaison des services (yellow / green / fhvhv) | `gold_service_comparison` | part de marché, `driver_share_pct`, panier médian |
| **P3** | Dynamique de la demande par jour de semaine et heure | `gold_demand_timeseries` | `nb_courses`, vitesse moyenne |
| - | Flux origine-destination inter-boroughs | `gold_od_matrix` | matrice OD, distance / revenu moyen |

---
La première problématique (P1) porte sur l'optimisation du revenu des chauffeurs. Elle cherche à identifier les zones et les créneaux horaires les plus rentables, en s'appuyant sur le datamart gold_revenue_by_zone_hour. Son indicateur clé est le revenu généré par minute de course (revenu_par_min).

La deuxième problématique (P2) compare les trois types de service entre eux, à savoir les taxis jaunes, les taxis verts et les VTC. Elle repose sur le datamart gold_service_comparison et mesure notamment la part de marché de chaque service, la part du revenu reversée au chauffeur (driver_share_pct) et le panier médian.

La troisième problématique (P3) analyse la dynamique de la demande selon le jour de la semaine et l'heure de la journée. Le datamart gold_demand_timeseries permet de suivre le nombre de courses ainsi que la vitesse moyenne des trajets.
La quatrième problématique s'intéresse enfin aux flux de déplacement entre les boroughs de New York. Le datamart gold_od_matrix reconstitue la matrice origine-destination et fournit, pour chaque flux, la distance et le revenu moyens.


## 4. Diagramme d'architecture

Ce diagramme est une vue d'ensemble. Son rôle est de montrer quels services AWS composent la pipeline et comment la donnée circule entre eux, de l'ingestion jusqu'à l'exposition. Il ne cherche pas à représenter la logique d'exécution détaillée de l'orchestrateur.  Faire figurer tous les états intermédiaires de la **State machine** (les choix conditionnels, les captures d'erreur, les mises à jour du registre DynamoDB) dans le diagramme d'architecture **l'aurait surchargé** et lui aurait fait perdre sa fonction première, qui est de se comprendre d'un seul coup d'œil.

![Architecture de la pipeline](Projet-data-pipeline.drawio.svg)

**Flux de bout en bout :**


###  Bronze - ingestion brute (`nyctlc-lambda-extract-bronze`)

- Téléchargement du Parquet source depuis le CDN TLC et upload dans `nyctlc-s3-bronze`.
- **Idempotence** via `head_object` : si l'objet existe déjà, l'ingestion est *skippée*.
- Écriture partitionnée `dataset/year=YYYY/month=MM/`.
- Chiffrement **SSE-KMS (CMK dédiée)** dès l'ingestion.
- Aucune transformation : la donnée brute est conservée telle quelle.
- Lancement du Crawler Bronze pour creer une table **Glue data catalog**

###  Silver - nettoyage, alignement & pseudonymisation (`nyctlc-glue-cleaning-silver-script`)

Job Glue PySpark, un seul script paramétré (`--dataset`, `--period`) pour les trois services :

1. **Lecture** depuis le Glue Data Catalog (Bronze).
2. **Alignement** sur un schéma canonique figé (les colonnes absentes d'un dataset =  `NULL`).
3. **Nettoyage** : déduplication, filtrage des valeurs aberrantes (durée, distance, montants,
   zones TLC valides 1-265) et **bornage temporel** dérivé de la période (élimine les timestamps
   parasites type 2002, 2007…).
4. **Pseudonymisation RGPD** : hachage **SHA-256 + pepper** des identifiants indirects fhvhv
   (`dispatching_base_num`, `originating_base_num`, `hvfhs_license_num`) sont irréversibles.
5. **Data Quality gate** via `EvaluateDataQuality` (DQDL) : complétude, plages de valeurs,
   `RowCount > 0`. Les métriques sont **publiées sur CloudWatch**, puis l'écriture est **bloquée**
   si une règle échoue (`raise`).
6. **Écriture Parquet** partitionnée (`service_type / year / month`) en *dynamic partition
   overwrite* (idempotence), avec `repartition(n)` numérique dimensionné à ~128 Mo/fichier
   pour éviter le piège des 200 partitions de *shuffle* par défaut.
7. Lancement du Crawler Silver pour creer une table **Glue data catalog**

###  Gold - datamarts exposés (CTAS Athena)

- **Lecture** depuis le Glue Data Catalog (Silver) via Athena
- Une vue unifiée `gold_trips_unified` (`UNION ALL` des trois services silver).
- **4 datamarts** reconstruits par CTAS Parquet/SNAPPY : `gold_revenue_by_zone_hour`,
  `gold_demand_timeseries`, `gold_service_comparison`, `gold_od_matrix`.
- **Stratégie d'idempotence** : `DROP TABLE` fait un nettoyage du préfixe S3
  (`nyctlc-lambda-clean-prefix-gold`, car Athena refuse d'écrire dans un `external_location` non
  vide) puis un `CTAS`. *Full reload* valide au volume agrégé du projet.
- **k-anonymat (k=5)** appliqué nativement : `HAVING COUNT(*) >= 5` sur les agrégats exposés.
- La creation des Datamarts entrainent par la meme occasion la creation de la Glue data catalog **(Gold)** qui aura son importance pour la dataviz et l'api

> La logique détaillée de la Step Functions est décrite séparément, présenté dans la section **Orchestration**.

## Monitoring et alerts

- **Dashboard CloudWatch** : métriques Step Functions, API Gateway (latence, 4XX/5XX, volume),
  consommation SPICE QuickSight, lignes ingérées.
- **Alarmes CloudWatch** couvrant les *angles morts* de l'orchestration, par exemple
  `ExecutionsTimedOut` / `ExecutionsAborted` : si l'exécution est tuée (timeout, throttle, abort),
  les états `Notify*` ne s'exécutent jamais, donc aucun mail ne part. L'alarme comble
  ce trou.
- **SNS applicatif** (`nyctlc-sns-stepfunction-full-pipeline`) : notifications de succès / échec
  émises *depuis* le workflow.

## API REST Gold

La couche Gold est exposée par une API REST construite avec API Gateway, dont l'implémentation repose sur une unique fonction Lambda nommée nyctlc-lambda-apirest-data-exposition. Plutôt que de multiplier les fonctions, le projet retient un modèle monolithique : une seule Lambda reçoit toutes les requêtes et assure elle-même le routage en interne, en fonction de la méthode HTTP et du chemin appelé. Chaque endpoint correspond à une vue Gold, qui est interrogée à la volée grâce à Athena.

| Endpoint | Datamart | Filtres |
|---|---|---|
| `GET /revenue-by-zone-hour` | `gold_revenue_by_zone_hour` | `heure`, `service` |
| `GET /demand-timeseries` | `gold_demand_timeseries` | `service`, `date` |
| `GET /service-comparison` | `gold_service_comparison` | `borough` |
| `GET /od-matrix` | `gold_od_matrix` | `origin`, `service` |

- **Anti-injection SQL** : toute valeur cliente passe par les `ExecutionParameters` Athena
  (placeholders `?` positionnels), jamais par *f-string*.
- **Contrôle d'accès** : clé d'API associée à un *Usage Plan* (quota + *rate limiting*).

## RGPD & sécurité

| Couche | Technique appliquée | Réversible |
|---|---|---|
| Bronze | Chiffrement **SSE-KMS (CMK)** dès l'ingestion + accès IAM restreint | Oui (clé) |
| Silver | **Pseudonymisation SHA-256 + pepper** des identifiants indirects fhvhv | Non |
| Gold | **k-anonymat (k=5)** via `HAVING COUNT(*) >= 5` sur tous les agrégats exposés | Non |

- **Chiffrement au repos** : CMK KMS dédiées par usage (bronze, silver, scripts, quicksight,
  cloudtrail) ; SSE-S3 pour la couche Gold (données agrégées / anonymisées). *Bucket Keys*
  activés pour réduire le coût des appels KMS.
- **Piste d'audit** : CloudTrail multi-région (`nyctlc-cloudtrail-audit`) avec validation
  d'intégrité des logs, *data events* S3 / Glue / Lambda, logs chiffrés KMS et stockés dans un
  bucket dédié avec archivage `DEEP_ARCHIVE`.

---

## 5. Stack technique & arbitrages FinOps

Chaque service de la pipeline a été choisi après comparaison avec deux ou trois alternatives, en pesant à la fois le coût, la performance et la complexité opérationnelle. Voici les décisions structurantes et les raisons qui les ont motivées :

| Besoin | Retenu | Alternatives écartées | Rationale FinOps / technique |
|---|---|---|---|
| Orchestration | **Step Functions + EventBridge Scheduler** | MWAA, Airflow self-hosted | MWAA ≈ 358 $/mois minimum (environnement toujours actif) ; Step Functions ≈ 0 € au volume du projet (facturation à la transition d'état) |
| Requêtage Gold | **Athena sur S3** | Redshift, Redshift Serverless | Datamarts pré-agrégés de petite taille ; Athena facture à la donnée scannée, pas de cluster à maintenir |
| Compute Silver | **AWS Glue (PySpark)** | EMR, Databricks | Serverless, pas de cluster à dimensionner ni à éteindre ; facturation à la DPU-heure |
| Ingestion Bronze | **Lambda** | Glue Python Shell, EMR | Tâche I/O légère (téléchargement + upload S3) ; *free tier* Lambda très généreux |
| Stockage | **S3 + Parquet** | Delta, Iceberg | Pas de besoin de *time-travel* / ACID ; Parquet natif suffit, coût de stockage minimal |
| API REST | **API Gateway + Lambda monolithique** | Lambda multiples, conteneur ECS | Un seul handler avec routage interne : moins de *cold starts* à surveiller, déploiement simplifié |
| Dashboard | **QuickSight (SPICE)** | Superset, Grafana, Metabase | Intégration native Athena, pas d'infra à héberger ; cache SPICE pour la performance |

- **Cycle de vie S3** : transitions chaînées `STANDARD_IA (30j) -> GLACIER_IR (90j) ->
  DEEP_ARCHIVE (180j)` sur Bronze ; Pour la silver et la gold `STANDARD_IA (30j) -> GLACIER_IR (90j)`
- **Serverless de bout en bout** : aucun cluster permanent (ni MWAA, ni EMR, ni Redshift)
  facturation à l'usage, coût quasi nul au repos.

> Le détail chiffré (le cout total sur la duree du projet) figure dans
> le rapport technique.
---


## 6. Orchestration

L'ensemble de la pipeline est piloté par une seule State machine, nommée `nyctlc-stepfunction-pipeline-orchestrator` et de type STANDARD. **Elle est déclenchée automatiquement par EventBridge Scheduler selon une planification récurrente**.

![Graphique de la Step Functions](stepfunctions_graph.svg)
---


La particularité de cette machine à états est qu'elle ne contient aucune requête ni aucun nom de table en dur. Tout lui est fourni au moment du déclenchement, à travers le `State input`  . Cet objet contient d'une part la liste des datasets à traiter (`yellow`, `green` et `fhvhv`), et d'autre part la liste des datamarts Gold à reconstruire. Chaque datamart est décrit par trois éléments : l'instruction de suppression de la table existante (drop), le préfixe S3 à vider avant reconstruction (`prefix`), et la requête de création du datamart (`ctas`).
Ce choix rend l'orchestrateur entièrement générique. La logique d'enchaînement ne dépend pas du contenu des requêtes, ce qui permet d'ajouter, de modifier ou de retirer un datamart sans jamais toucher à la définition de la machine à états : il suffit d'ajuster le tableau datamarts fourni en entrée.

Le traitement se déroule en deux grandes phases. La première phase traite les trois datasets en parallèle grâce à un état `Map` limité à trois exécutions simultanées. Pour chaque dataset, une fonction Lambda commence par interroger le registre d'état afin de déterminer la prochaine période à traiter. Si aucune période ne reste à traiter, la branche se termine proprement sans rien faire. Dans le cas contraire, les données brutes du mois concerné sont ingérées dans la couche Bronze, puis le job Glue de nettoyage est lancé pour produire la couche Silver. À chaque étape, le résultat (succès ou échec) est inscrit dans le registre DynamoDB.
La seconde phase ne démarre qu'une fois les trois datasets à jour. Elle rafraîchit d'abord la vue unifiée qui consolide les trois services, puis reconstruit les quatre datamarts Gold. Cette reconstruction s'appuie elle aussi sur un état `Map` parallèle qui applique, pour chaque datamart reçu en entrée, la même séquence : suppression de la table, nettoyage du préfixe S3 correspondant, puis exécution de la requête de création. À la fin, une notification de succès ou d'échec est émise via SNS.



---

## 7. Structure du dépôt

```
projet-data-pipeline/
├── iac/                                # Infrastructure as Code (CloudFormation, générée rétroactivement)
│   ├── storage/
│   │   ├── s3/                          # buckets bronze/silver/gold/scripts/cloudtrail-logs + policies
│   │   ├── dynamodb/                    # table de registre d'état
│   │   └── glue_data_catalog/           # database + tables + crawlers
│   ├── security/
│   │   ├── iam/
│   │   │   ├── role/
│   │   │   └── policy/
│   │   └── kms/                         # CMK par usage + alias
│   ├── compute/
│   │   ├── lambda/                      # fonctions bronze / getnextperiod / clean-prefix / api
│   │   ├── glue/                        # job silver
│   │   └── athena/                      # workgroup
│   ├── orchestration/
│   │   ├── step_functions/              # state machine
│   │   └── eventbridge/                 # scheduler
│   ├── api/
│   │   └── apigateway/                  # REST API + usage plan
│   └── observability/
│       ├── cloudwatch/                  # dashboard + alarmes
│       ├── cloudtrail/                  # trail d'audit
│       └── sns/                         # topics de notification
├── src/                                # Code source écrit à la main
│   ├── lambda/
│   │   ├── nyctlc-lambda-extract-bronze.py
│   │   ├── nyctlc-lambda-getnextperiod-bronze.py
│   │   ├── nyctlc-lambda-clean-prefix-gold.py
│   │   └── nyctlc-lambda-apirest-data-exposition.py
│   ├── glue/
│   │   └── nyctlc-glue-cleaning-silver-script.py
│   └── athena/
│       ├── vue_unifiee.sql              # gold_trips_unified (UNION ALL)
│       ├── od_matrix.sql                # gold_od_matrix (CTAS)
│       └── *.sql                        # requêtes de vérification / datamarts
├── Projet-data-pipeline.drawio          # schéma d'architecture (source)
├── Projet-data-pipeline.drawio.svg      # schéma d'architecture (rendu)
├── .gitignore
└── README.md
```

---

## 8. Infrastructure as Code

L'ensemble des ressources a été **construit via la console AWS**, puis exporté rétroactivement
en **templates CloudFormation** à l'aide du **Générateur IaC**, groupe logique par groupe logique
(`storage`, `security`, `compute`, `orchestration`, `api`, `observability`).

> Ces templates constituent une **documentation d'infrastructure** et une preuve de reproductibilité :
> ils ne sont pas destinés à un redéploiement en l'état (identifiants de ressources vivantes,
> ARN spécifiques au compte). Le passage à un IaC pleinement paramétré est identifié comme piste
> d'amélioration dans le rapport.

---

## 9. Équipe

Projet de groupe - M2 Data Engineering & IA, EFREI.

- **Jacques Lin**
- **Thomas Coutarel**
- **Anira José Mendes Pereira**

---

## 10. Livrables

- **L1** - Rapport technique & métier (architecture, comparatifs, FinOps, RGPD, interprétations).
- **L2** - Vidéo de démonstration (10-15 min) : intro, walkthrough archi, démo pipeline live,
  dashboards + API + monitoring, synthèse FinOps.

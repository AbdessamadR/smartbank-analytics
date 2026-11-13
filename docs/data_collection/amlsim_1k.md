\# Collecte des données AMLSim — Scénario 1K



\## 1. Objectif



Générer un jeu de données bancaires synthétiques permettant de développer et tester une plateforme Big Data de détection et d'analyse des transactions suspectes (AML).



\## 2. Source des données



\- Projet : IBM AMLSim

\- Dépôt officiel : https://github.com/IBM/AMLSim

\- Scénario : `1K`

\- Nature : données entièrement synthétiques, non issues de transactions bancaires réelles.

\- Configuration : `external/AMLSim/paramFiles/1K/conf.json`



\## 3. Environnement d'exécution



La simulation AMLSim est exécutée dans un conteneur Docker isolé.



\- Java : OpenJDK 8

\- Python : 3.7

\- Maven : 3.9.9

\- Image Docker : `amlsim:java8-python37`



L'environnement Python principal du projet reste indépendant.



\## 4. Processus de génération



1\. Préparation de la configuration du scénario 1K.

2\. Génération du graphe de transactions avec `transaction\_graph\_generator.py`.

3\. Exécution de la simulation Java sur 720 étapes.

4\. Conversion des journaux avec `convert\_logs.py`.

5\. Vérification des fichiers CSV générés.



\## 5. Localisation des données



Répertoire principal :



`data/raw/amlsim/1K/`



Les fichiers bruts et les fichiers convertis sont conservés localement. Le répertoire `data/raw/` est exclu du suivi Git.



\## 6. Volumes collectés



| Fichier | Nombre de lignes | Colonnes |

|---|---:|---:|

| accounts.csv | 1 446 | 24 |

| transactions.csv | 121 457 | 8 |

| cash\_tx.csv | 0 | 8 |

| alert\_transactions.csv | 65 | 9 |

| alert\_accounts.csv | 73 | 10 |

| sar\_accounts.csv | 72 | 7 |



Le scénario nommé 1K génère effectivement 1 446 comptes.



\## 7. Contrôles qualité



\### Transactions



\- 121 457 transactions.

\- 0 valeur manquante.

\- 0 doublon complet.

\- 0 identifiant `tran\_id` dupliqué.



\### Labels SAR



| Label | Nombre |

|---|---:|

| False | 121 392 |

| True | 65 |



Taux de transactions SAR : environ 0,0535 %.



La correspondance entre les 65 transactions SAR de `transactions.csv` et les 65 transactions de `alert\_transactions.csv` est exacte.



\### Comptes



\- 1 446 comptes.

\- 0 doublon complet.

\- Colonne `tx\_behavior\_id` entièrement vide (1 446 valeurs manquantes).



Cette colonne est conservée et documentée, sans suppression des comptes.



\### Autres observations



\- `cash\_tx.csv` est vide.

\- 73 comptes figurent dans `alert\_accounts.csv`.

\- 72 comptes figurent dans `sar\_accounts.csv`.



Ces différences doivent être prises en compte lors des analyses ultérieures.



\## 8. Limites du dataset



\- Données synthétiques et non représentatives de toutes les pratiques bancaires réelles.

\- Forte asymétrie entre transactions normales et transactions SAR.

\- Absence de valeurs pour `tx\_behavior\_id`.

\- Fichier `cash\_tx.csv` vide.

\- Les labels SAR indiquent des transactions associées à des typologies suspectes, et non une fraude judiciairement confirmée.



\## 9. Prochaines étapes



1\. Préserver les données brutes dans la couche Bronze.

2\. Construire la couche Silver avec PySpark.

3\. Définir les règles de nettoyage et de validation.

4\. Préparer les données analytiques Gold.

5\. Intégrer les flux Kafka et les traitements Spark.

6\. Évaluer les modèles de Machine Learning avec des métriques adaptées au déséquilibre des classes.


## 10. Reproduction de l'environnement

Après clonage du projet :

```bash
git submodule update --init --recursive
```

Restaurer la configuration personnalisée du scénario 1K :

```bash
cp docs/data_collection/conf_1k_custom.json external/AMLSim/paramFiles/1K/conf.json
```

Télécharger MASON 20 (dépendance externe non versionnée) :

```bash
curl -L "https://people.cs.gmu.edu/~eclab/projects/mason/mason.20.jar" -o external/AMLSim/jars/mason.20.jar
```

Construire l'image Docker depuis la racine du projet :

```bash
docker build -t amlsim:java8-python37 -f docker/amlsim/Dockerfile .
```

**Important :** le fichier `mason.20.jar` doit être présent avant la construction de l'image. Il n'est pas inclus dans Git.
\---



\*\*Statut :\*\* collecte AMLSim 1K exécutée et contrôles initiaux réalisés.



\*\*Date de collecte :\*\* 13 novembre 2026.


### 1. Contexte et cas d’usage

|  | | |
| - | - | - |
| Volume | | Environ 100 millions de notations d'utilisateurs |
| Vélocité | | Ingestion batch |
| Variété | | Fichiers texte bruts, Parquet, CSV |
| Véracité | | Nettoyage, typage strict, suppression des lignes invalides|
| Valeur | | Analyse des tendances de notation des films |


### 2. Architecture du pipeline


**Ingestion :**  
Les données de notation des utilisateurs Netflix (fichiers `.txt`) sont stockées dans S3 (raw/ratings/).   
Le fichier contenant les informations sur les films (`.csv`) est également stocké dans S3 (raw/movies/). 
Chaque fichier contient des blocs `MovieID:` suivis des notations des utilisateurs.

**Storage :**  
Les données brutes sont conservées dans S3.  
Les données transformées sont stockées en Parquet dans processed/ratings et processed/movies

**Processing :**  
Un job AWS Glue lit les fichiers bruts, restructure les données, convertit les types (ID, rating, date) et écrit les données en Parquet.

**Analysis & Visualization :**  
Athena est utilisé pour effectuer des requêtes sur les données transformées, et Power BI permet de visualiser.

Le crawlers Glue assurent la synchronisation entre S3 et Athena :

---

### 3. Analyse des coûts 

Glue qui domine largement les coûts (environ 89 %), cela est lié au fait que notre ETL traite sur plus de 100 millions de lignes. 

CloudWatch est le deuxième poste (environ 8,2 %) lié à la surveillance des jobs Glue.

Les autres services sont  (Athena, S3, Kinesis < 3 % au total).

La prévision de fin de mois (+56,7 %) suggère une augmentation significative de l'utilisation. 

![alt text](image.png)


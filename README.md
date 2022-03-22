# M1_X2I1010_BDD
## Sources
Le dataset utilisé pour la réalisation de notre entrepôt de données est disponible sur Kaggle : https://www.kaggle.com/yuanyuwendymu/airline-delay-and-cancellation-data-2009-2018  
Celui-ci peut être reconstruit directement depuis le site du *Bureau of Transportation Statistics* https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FGJ&QO_fu146_anzr=b0-gvzr

## Licence de l'entrepôt : **CC-BY**
Le *BOT* est une entité faisant partie du *Department of Transportation*. Les données accessibles mises à disposition sont ouvertes : https://www.transportation.gov/mission/digital-government-strategy-4.   
En cas d'utilisation de ces données, le *DOT* préconise d'utiliser une licence **CC-BY** ou équivalent.

## Méthodes d'intégration : echantillionage_dataset.py | csv_to_sql.py  
Le premier script permet de réaliser un échantillionage des données afin de ne conserver que 100000 vols : 30000 annulés et 70000 non-annulés.
Le deuxième script permet de créer plusieurs bases dans un serveur utilisant PostgreSQL, en ajoutant également des valeurs notant la vitesse évaluée d'un vol, ainsi que de préciser l'heure de son départ.  


## Schéma de données : schema_airport_delays.png  
Explication : Notre table de faits est la table *flight*, celle-ci contient tout les faits numériques concernant un vol, et celle-ci est donc associée aux companies aériennes et aux aéroports (principalement américain, mais la présence de vols internationaux permet d'avoir des aéroports étrangers), ainsi que les dates de ces vols.




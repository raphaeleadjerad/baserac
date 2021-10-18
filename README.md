[![pipeline status](https://gitlab.com/DREES_code/OSAM/bameds/open_base_rac_snds/badges/master/pipeline.svg)](https://gitlab.com/DREES_code/OSAM/bameds/open_base_rac_snds/-/commits/master)  [![coverage report](https://gitlab.com/DREES_code/OSAM/bameds/open_base_rac_snds/badges/master/coverage.svg)](https://gitlab.com/DREES_code/OSAM/bameds/open_base_rac_snds/-/commits/master)

# La base RAC (restes à charge en santé après Assurance maladie obligatoire)

**Objectif des programmes** :  
Ce dossier fournit les programmes permettant de produire la base de dépenses
et de restes à charge à partir du DCIRS et du PMSI.   
Cette base a permis de produire plusieurs [Études et Résultats](#publications-de-r%C3%A9f%C3%A9rence) sur le sujet des restes à charge en France. 

**Présentation de la DREES** :  
La Direction de la recherche, des études, de l'évaluation et des statistiques (DREES) 
est une direction de l'administration centrale des ministères sanitaires et sociaux.  
https://drees.solidarites-sante.gouv.fr/etudes-et-statistiques/la-drees/qui-sommes-nous/

**Données sources utilisées, producteur, date et version** : 
- *Source de données* : PMSI et DCIRS 2016 et 2017 
- *Version au* : 06/05/2021
- *Traitements* : Adjerad, Raphaële - Courtejoie, Noémie. DREES.


Date de la dernière exécution des programmes avant publication, et version des logiciels utilisés :   
Les programmes sont exécutés dans la pipeline d'intégration continue de Gitlab, via les scripts de tests,
et à partir de données simulées du SNDS construites pour ces tests.  
Le programme tourne avec la version 3.1.1 de Spark, Scala version 2.12.10.
Il tourne sur Python 3.7.6, avec les modules indiqués dans le fichier [requirements.txt](requirements.txt).

## Résumé

La connaissance de la distribution des dépenses de santé et des restes à charge (RAC) associés, 
après remboursement par l’Assurance maladie obligatoire (AMO), représente un enjeu important pour 
le pilotage des politiques de santé. Pour répondre à ce besoin, la DREES a construit la base RAC, une base de données 
simplifiée des dépenses et des RAC AMO. 

Le reste à charge après Assurance maladie obligatoire (RAC AMO) est le montant de la participation 
financière du patient après remboursement par l’AMO et par les pouvoirs publics (pour les bénéficiaires de la 
CMU-C, de l’aide médicale d’État, ou encore pour les détenus), et avant prise en charge par une Assurance maladie 
complémentaire (AMC). Il se compose d’une part opposable (différence entre le montant de la base de remboursement 
et le montant remboursé) et d’une part de liberté tarifaire (dépassements d’honoraires et tarifs libres sur 
certains produits).

La base des dépenses de santé et des restes à charge après AMO (base RAC) est indexée sur l’année. 
Elle regroupe par poste de soins les dépenses des assurés ayant consommé au moins une fois des soins sur 
le territoire français pendant l’année considérée (appelés par la suite les « consommants »). 
La base RAC est exhaustive sur la population des consommants, pour tous les régimes d’assurance maladie. 
Elle couvre l’exhaustivité des dépenses individualisables, remboursables et présentées au remboursement, 
en soins de ville et à l'hôpital (pour toutes les disciplines hospitalières (médecine chirurgie obstétrique [MCO], 
hospitalisation à domicile [HAD], psychiatrie, et soins de suites et de réadaptation [SSR]), pour les 
établissements de santé publics et privés. Elle ne couvre pas les dépenses du champ médico-social (e.g. USLD, EHPAD),
 ni les dépenses des bénéficiaires des régimes spéciaux du Sénat et de l’Assemblée Nationale 
 (fort risque de ré-identification).

La base RAC a été construite par appariement de deux sources provenant du Système national des données de santé (SNDS) : 
le Datamart de consommation inter-régime simplifié (DCIRS), géré par la Caisse nationale d’assurance maladie (CNAM), 
et le Programme de médicalisation des systèmes d’information (PMSI), géré par l’Agence technique de l’information sur 
l’hospitalisation (ATIH). Le DCIRS contient des informations sur les soins présentés au remboursement en ville ou en 
établissements de santé, pour l’ensemble des bénéficiaires des différents régimes d’assurance maladie 
(hors Sénat et Assemblée nationale). Le PMSI fournit une description médico-économique des soins hospitaliers 
en MCO, HAD, psychiatrie et SSR. Dans la base RAC, les informations sur les soins en établissements de 
santé publics (établissements publics et privés non lucratifs ex-DG) ont été extraites du PMSI ; 
celles sur les soins de ville et en établissements de santé privés (établissements privés à but lucratif et 
autres établissements à but non lucratif) ont été extraites du DCIRS.

La base RAC comprend une ligne par individu et par poste de soins, pour l’année sur laquelle elle est indexée. La ventilation des dépenses par poste de soins résulte du regroupement des prestations en grandes catégories de soins, en fonction de la nature de chaque prestation, de la nature du professionnel exécutant et du lieu d'exécution. La base RAC contient des informations sur les caractéristiques individuelles des consommants (âge, sexe, lieu de résidence, statut ALD, statut CMU-C et statut ACS) ; ainsi que des indicateurs de dépense et de remboursement (montant de la dépense, base de remboursement, dépense remboursée par l’AMO, RAC AMO avec distinction de 
la partie opposable et de la liberté tarifaire).

Certains points méthodologiques sont en cours d'ajout à la construction de la base, notamment des raffinements concernant
la quantité d'acte, qui pour le moment n'est pas utilisée dans les publications, ou encore les codes commune pour
exploiter la dimension géographique. 

Pour le détail concernant le champ et les sources, se référer aux [publications de référence](#publications-de-r%C3%A9f%C3%A9rence), ainsi qu'à la [présentation plus détaillée de la base RAC](../documentation/presentation_generale.md) qui figure dans le fichier [documentation](../documentation) de ce projet.

## Publications de référence

-	**Adjerad, R., Courtejoie, N.** (2020, novembre). 
[Pour 1 % des patients, le reste à charge obligatoire après assurance maladie obligatoire dépasse 3 700 euros annuels. 
DREES, Études et Résultats, 1171.](https://drees.solidarites-sante.gouv.fr/sites/default/files/2021-02/ER_1171_BAT%20BIS.pdf) 

-	**Adjerad, R., Courtejoie, N.** (2021, janvier). 
[Des restes à charge après assurance maladie obligatoire comparables entre patients âgés avec et sans affection de 
longue durée, malgré des dépenses de santé 3 fois supérieures. 
DREES, Études et Résultats, 1180.](https://drees.solidarites-sante.gouv.fr/sites/default/files/2021-02/er_1180.pdf) 

-	**Adjerad, R., Courtejoie, N.** (2021, mai). 
[Hospitalisation : des restes à charge après assurance maladie obligatoire plus élevés en soins de suite et de 
réadaptation et en psychiatrie. 
DREES, Études et Résultats, 1192.](https://drees.solidarites-sante.gouv.fr/sites/default/files/2021-05/ER1192.pdf) 


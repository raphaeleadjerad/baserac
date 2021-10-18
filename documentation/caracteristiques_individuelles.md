# Les caractéristiques individuelles des bénéficiaires dans la base RAC

Ces variables sont présentes à chaque ligne de la base RAC et sont uniques pour un bénéficiaire, sur une année donnée.  

## L'identifiant du bénéficiaire 

Dans la base RAC, l'identifiant du bénéficiaire est nommé `NUM_ENQ` et correspond au `BEN_IDT_ANO` crypté.  
Il s'agit :   
- du NIR du bénéficiaire `BEN_NIR_ANO` s'il existe (`BEN_IDT_TOP`==1)  
- de la concaténation du pseudo NIR `BEN_NIR_PSA` et du rang gémellaire `BEN_RNG_GEM` sinon (`BEN_IDT_TOP`==0)  

Pour rappel le `BEN_NIR_ANO` est un identifiant unique vie entière attribué à toute personne née en France (les personnes nées à l'étranger peuvent
se voir attribuer un NIR transitoire, puis définitif *cf.* [fiche sur les identifiants des bénéficiaires](https://documentation-snds.health-data-hub.fr/fiches/fiche_beneficiaire.html#les-identifiants-beneficiaires-dans-le-snds)).  
Pour un même `BEN_NIR_ANO`, il peut y avoir plusieurs identifiants ayant-droit ou pseudo-NIR (`BEN_NIR_PSA`), issus de la concaténation du NIR de l'ouvreur de droit, du sexe et de la date de naissance du bénéficiaire, étant donné que l'ouvreur de droit peut être l'un ou l'autre des deux parents, le bénéficiaire lui-même, etc.  

## Variables démographiques et géographiques
- `BEN_NAI_ANN` : année de naissance du bénéficiaire
- `age` : âge du bénéficiaire en années (année de la base moins année de naissance, complétée par `BEN_AMA_COD` (âge du bénéficiaire) si `BEN_NAI_ANN` manquante)
- `classe_age` : classe d'âge du bénéficiaire par tranche de 5 ans 
- `classe_age2` : classe d'âge du bénéficiaire par tranche de 10 ans
- `BEN_RES_DPT` : département de résidence du bénéficiaire
- `BEN_RES_COM` : commune de résidence du bénéficiaire
- `code_com_insee` : code commune INSEE de résidence du bénéficiaire (concaténation des deux derniers caractères du département et du code commune, sauf cas particulier
DOM, Corse)
- `code_res_dpt` : département de résidence du bénéficiaire après corrections   

Les codes commune (`code_com_insee`) et département (`code_res_dpt`) ont été corrigés comme indiqué dans la [fiche sur la localisation géographique des bénéficiaires](https://documentation-snds.health-data-hub.fr/fiches/fiche_beneficiaire.html#les-identifiants-beneficiaires-dans-le-snds).  

### Les tops (variables indicatrices)

- `top_cmu` : top indiquant les patients avec des droits à la couverture maladie universelle complémentaire (CMU-C) ouverts, pour l'année étudiée.  
Ce top est construit à partir du référentiel `IR_ORC_R`, et consiste à toper un individu en `CMU-C` si la variable `BEN_CTA_TYP`, qui indique le type de contrat d'affiliations à un organisme d'assurance maladie complémentaire, est égale à '89' (valeur qui correspond à la CMU-C)
et que les dates de début et de fin de contrat correspondent à l'année étudiée.  
L'utilisation du référentiel `IR_ORC_R` est la méthode conseillée pour recenser de façon exhaustive la population des CMUcistes (*cf.* [fiche sur la CMU-C](https://documentation-snds.health-data-hub.fr/fiches/cmu_c.html) documentation collaborative).  
La variable `BEN_CTA_TYP` dans `IR_ORC_R` est construite à partir de la variable du même nom qui est associée à chaque prestation dans la table de prestation centrale `NS_PRS_F`.  
Afin de compléter le top CMU-C dans le cas où `IR_ORC_R` serait imparfaitement renseigné, un individu est également toppé en CMU-C si la variable `BEN_CTA_TYP` dans `NS_PRS_F` prend la valeur "89" (valeur qui correspond à la CMU-C) pour au moins l'une des ses prestations de l'année.

- `top_acs` : top indiquant les patients ayant souscrit un contrat d'aide à la complémentaire santé (ACS) au moins un jour de l'année étudiée.   
Les patients avec des droits à l'ACS ouverts et ayant souscrit un contrat ACS sont identifiables à partir du référentiel `IR_ORC_R`. Un individu est toppé en `ACS` si la variable `BEN_CTA_TYP` (type de contrat complémentaire) est égale à "91", "92" ou "93" (liste des contrats ACS) 
et que les dates de début et de fin de contrat correspondent à l'année étudiée (*cf.* [fiche sur l'ACS en cours de rédaction](https://documentation-snds.health-data-hub.fr/fiches/acs_c.html) dans la documentation collaborative).  
Afin de compléter le top ACS dans le cas où `IR_ORC_R` serait imparfaitement renseigné, un individu est également toppé en ACS si la variable `BEN_CTA_TYP` dans `NS_PRS_F` prend les valeurs "91", "92" ou "93" (liste des contrats ACS) pour au moins l'une des ses prestations de l'année.


- `top_ald` : top indiquant les bénéficiaires d'une affection de longue durée (ALD) "active" : la condition est d’avoir été en ALD au moins un jour au cours de l'année étudiée, et d'avoir fait valoir son ALD au moins une fois sur cette année.    
On identifie les bénéficiaires d'une ALD au cours de l'année à partir de la variable `IMB_ETM_NAT` du référentiel médicalisé `IR_IMB_R` (valeurs 41 = ald sur liste, 43 = ald hors liste et 45 = ald polypathologie), 
ainsi que des dates de début et de fin d'exonération du ticket modérateur (`IMB_ALD_DTD` et `IMB_ALD_DTF`).  
Pour identifier les ALD dites "actives" (utilisées pour au moins une prestation dans l'année), on croise cette première information avec la variable `EXO_MTF` (motif d'exonération du ticket modérateur) 
dans la table centrale des prestations (`EXO_MTF` IN 41:46).   
Nous suivons ainsi les recommandations de la CNAM (*cf.* fiche sur [les bénéficiaires du dispositif ALD](https://documentation-snds.health-data-hub.fr/fiches/beneficiaires_ald.html) pour plus de précision sur la construction du top).  
En complément, on extraie le nombre d'ALD par bénéficiaire, ainsi que le(s) numéro(s) d'ALD correspondant(es) (liste), en suivant la méthode proposée dans la fiche sur [les bénéficiaires du dispositif ALD](https://documentation-snds.health-data-hub.fr/fiches/beneficiaires_ald.html). 
Dans la base RAC, ces variables sont appelées respectivement
`nb_ald` et `NUM_ALD`.

### Regroupement des Affections de longue durée 

Les ALD ont été regroupées en quatre groupes de pathologies ouvrant le plus fréquemment droit à ce dispositif :

- les *maladies cardio-neurovasculaires* : regroupement des ALD n° 1 (accident vasculaire cérébral invalidant), n° 3 (artériopathies chroniques avec manifestations ischémiques), n° 5 (insuffisance cardiaque, troubles du rythme, cardiopathies valvulaires, congénitales graves), n° 12 (hypertension artérielle sévère) et n° 13 (maladie coronaire). Elles concernent environ 35 % des assurés au régime général (RG) en ALD (RG) en 2017 ;  
- le *diabète* (25 % des ALD au RG) (ALD n° 8) ;  
- les *tumeurs malignes* (20 % des ALD au RG) (ALD n° 30) ;  
- les *affections psychiatriques de longues durées* (13 % des ALD au RG) (ALD n° 23).  

Les groupes de pathologies décrits ci-dessus ne sont pas mutuellement exclusifs : un patient peut être classé dans deux groupes différents.   
Les variables binaires `cardio_neurovasc`, `diabete`, `tumeurs` et `affection_psy` indiquent l'appartenance ou non de chaque bénéficiaire à chaque groupe de pathologies.  
En complément, la variable `groupe_ALD` indique l'appartenance exclusive des assurés en ALD à l'une ou l'autre des catégories d’ALD majoritaires.   
Elle prend les valeurs : *cardio_neurovasc*, *diabete*, *tumeurs*, *affection_psy*, *polypath_top4*, *autres_ald*, *non_ald*.   
Les assurés étant en ALD pour une seule des quatre pathologies les plus fréquentes sont classés dans le groupe correspondant, tandis que ceux étant dans au moins deux catégories sont classés dans le groupe nommé *polypath_top4* pour « poly-ALD parmi le top 4 ». 
Le reste des assurés en ALD sont classés parmi les *autres_ald*.   

Enfin, la variable `type_ALD` indique si l'assuré a des droits ouverts au titre de plusieurs ALD (valeur : *poly_ALD*), une seule (valeur : *mono_ald*), ou aucune (*non_ald*).

#!/usr/bin/env python
# coding: utf-8


#  Copyright (C) 2021. Logiciel élaboré par l'État, via la Drees.

# Nom de l'auteur : Adjerad, Raphaele - Courtejoie, Noémie, Drees.

# Ce programme informatique a été développé par la Drees.
# Il permet de produire la base de données à partir du SNDS utilisée notamment pour les Études et Résultats
# 1171, 1180 et 1192

# Ce programme est executé notamment dans la pipeline de Continuous integration (voir le badge "passed" qui indique que
# le code tourne bien sur les fausses données construites pour les tests et le badge de couverture du
# code associé),
# il tourne avec la version 3.1.1 de Spark, Scala version 2.12.10.
# Il tourne sur Python 3.7.6, avec les modules indiqués dans le fichier requirements.txt.

# Le texte et les tableaux de l'article peuvent être consultés sur le site de la DREES (voir le README pour les liens).

# Ce programme utilise les données du SNDS (PMSI et DCIRS) extraites par la CNAM pour la DREES,
# pour les années 2016 et 2017.

# Bien qu'il n'existe aucune obligation légale à ce sujet, les utilisateurs de ce programme sont invités à signaler
# à la DREES leurs travaux issus de la réutilisation de ce code, ainsi que les éventuels problèmes ou anomalies qu'ils
# y rencontreraient, en écrivant à DREES-CODE@sante.gouv.fr

# Ce logiciel est régi par la licence "GNU General Public License" GPL-3.0.
# https://spdx.org/licenses/GPL-3.0.html#licenseText

# À cet égard l'attention de l'utilisateur est attirée sur les risques associés au chargement, à l'utilisation, à la
# modification et/ou au développement et à la reproduction du logiciel par l'utilisateur étant donné sa spécificité de
# logiciel libre, qui peut le rendre complexe à manipuler et qui le réserve donc à des développeurs et des
# professionnels avertis possédant des connaissances informatiques approfondies. Les utilisateurs sont donc invités
# à charger et tester l'adéquation du logiciel à leurs besoins dans des conditions permettant d'assurer la sécurité
# de leurs systèmes et ou de leurs données et, plus généralement, à l'utiliser et l'exploiter dans les mêmes
# conditions de sécurité.

# Le fait que vous puissiez accéder à cet en-tête signifie que vous avez pris connaissance de la licence GPL-3.0,
# et que vous en avez accepté les termes.


# This program is free software: you can redistribute it and/or modify it under the terms of the GNU General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option)
# any later version.

# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

# You should have received a copy of the GNU General Public License along with this program.If not,
# see <https://www.gnu.org/licenses/>.

from time import time
import pyspark.sql.functions as psf
import pyspark.sql.types as pst
from .hospit import _load_ir_orc


def _load_ssr_c(path2flat, year, spark):
    ssr_c = (spark.read
             .parquet(path2flat + "single_table/SSR_C/year=" + str(year))
             .select(["NUM_ENQ", "ETA_NUM", "RHA_NUM", "EXE_SOI_DTD", "EXE_SOI_DTF"])
             )
    return ssr_c


def _load_ssr_stc(path2csv_ssr, year, spark):
    # Table de facturation
    if year == 2017:
        path_temp = path2csv_ssr + "STC.CSV"
    elif year == 2016:
        path_temp = path2csv_ssr + "stc.csv"
    else:
        raise ValueError("Erreur année non prise en charge")
    ssr_stc = (spark.read.option("header", "true")
               .option("sep", ";")
               .csv(path_temp)
               .select(["ETA_NUM", "RHA_NUM", "SEJ_FAC_AM", "TOT_MNT_AM", "TOT_MNT_AMC", "REM_BAS_MNT",
                        "FAC_MNT_TM", "FAC_MNT_FJ", "MAJ_MNT_PS", "REM_TAU", "NAT_ASS", "EXO_TM", "FJ_COD_PEC",
                        "FAC_18E", "BEN_CMU", "FAC_NBR_VEN"])
               )
    return ssr_stc


def agg_ssr(path2flat, path2csv_ssr, path2_ir_orc, year, filter_etab, list_output_var, spark):
    ssr_c = _load_ssr_c(path2flat, year, spark)
    # On importe la table S de description du sejour pour avoir le mode de sortie 
    if year == 2017:
        path_temp = path2csv_ssr + "S.CSV"
    elif year == 2016:
        path_temp = path2csv_ssr + "s.csv"
    else:
        raise ValueError("Erreur année non prise en charge")
    ssr_s = (spark.read
             .option("header", "true")
             .option("sep", ";")
             .csv(path_temp)
             .select(["ETA_NUM", "RHA_NUM", "ENT_MOD", "SOR_MOD", "PRE_JOU_NBR", "SEJ_NBJ"])
             )

    # assert ssr_s.count() == ssr_c.count()

    ssr_c = ssr_c.join(ssr_s, ["ETA_NUM", "RHA_NUM"], how="left")

    # On importe la table B de description du sejour pour avoir le type d'hospit
    if year == 2017:
        path_temp = path2csv_ssr + "B.CSV"
    elif year == 2016:
        path_temp = path2csv_ssr + "b.csv"
    else:
        raise ValueError("Erreur année non prise en charge")
    ssr_b = (spark.read
             .option("header", "true")
             .option("sep", ";")
             .csv(path_temp)
             .select(["ETA_NUM", "RHA_NUM", "HOS_TYP_UM", "GRG_GME", "BDI_DEP", "BDI_COD", "COD_SEX", "AGE_ANN"])
             )

    # Supprimer les GME en erreur (dont le code commence par 90)
    ssr_b = (ssr_b.filter(psf.substring(psf.col("GRG_GME"), 1, 2) != 90)
             .groupBy(["ETA_NUM", "RHA_NUM", "BDI_DEP", "BDI_COD", "COD_SEX", "AGE_ANN"]).agg({"HOS_TYP_UM": "min"})
             .withColumnRenamed("min(HOS_TYP_UM)", "HOS_TYP_UM_C")
             .dropDuplicates())

    ssr_c = ssr_c.join(ssr_b, ["ETA_NUM", "RHA_NUM"], how="right")

    ssr_stc = _load_ssr_stc(path2csv_ssr, year, spark)
    ssr_valo = ssr_c.join(ssr_stc, ["ETA_NUM", "RHA_NUM"], how="inner")

    # Supprimer les sejours non valorisés ou en attente de valorisation (1.41% de donnees)
    ssr_valo = ssr_valo.filter(psf.col("SEJ_FAC_AM") == 1)

    # Suppression des sejours sans information de facturation à l'AM
    ssr_valo = ssr_valo.filter(psf.col("TOT_MNT_AM") != 0)

    # Filtre sur les etablissements
    ssr_valo = ssr_valo.filter(~psf.col("ETA_NUM").isin(filter_etab))

    # Exclusion des prestations inter-établissement
    ssr_valo = ssr_valo.filter((psf.col("ENT_MOD") != 0) & (psf.col("SOR_MOD") != 0))

    for c in ["PRE_JOU_NBR", "SEJ_NBJ", "REM_TAU", "FAC_MNT_FJ", "REM_BAS_MNT", "TOT_MNT_AM",
              "FAC_MNT_TM", "TOT_MNT_AMC"]:
        ssr_valo = ssr_valo.withColumn(c, psf.col(c).cast(pst.DoubleType()))

    # Gestion des dates
    ssr_valo = ssr_valo.filter(psf.col("EXE_SOI_DTD") <= str(year) + "-12-31")
    # Suppression des sejours sans date de fin
    ssr_valo = ssr_valo.filter(psf.col("EXE_SOI_DTF").isNotNull())

    # corriger les séjours se terminant le "2030-12-31"
    # D'apres date debut et nb jours presence, c'est plutot "2016-12-31"
    ssr_valo = ssr_valo.withColumn("EXE_SOI_DTF",
                                   psf.when(psf.col("EXE_SOI_DTF") == "2030-12-31", str(year) + "-12-31").
                                   otherwise(psf.col("EXE_SOI_DTF")))

    # Supprimer tous les sejours qui ne se terminent pas dans l'année
    ssr_valo = ssr_valo.filter(psf.col("EXE_SOI_DTF") <= str(year) + "-12-31")

    # Creation de la variable de duree de sejour
    ssr_valo = ssr_valo.withColumn("duree_sejour", psf.datediff(psf.col("EXE_SOI_DTF"), psf.col("EXE_SOI_DTD")))
    ssr_valo = ssr_valo.withColumn("SEJ_NBJ",
                                   psf.when(psf.col("SEJ_NBJ").isNull(), psf.col("duree_sejour") + 1).
                                   otherwise(psf.col("SEJ_NBJ")))

    # Corriger et ramener le nombre de jours de présence au nombre de jour total
    ssr_valo = ssr_valo.withColumn("PRE_JOU_NBR",
                                   psf.when(psf.col("SEJ_NBJ") < psf.col("PRE_JOU_NBR"), psf.col("SEJ_NBJ")).
                                   otherwise(psf.col("PRE_JOU_NBR")))

    # Créer la variable nombre de jours de présence en temps complet
    ssr_valo = ssr_valo.withColumn("SEJ_NBJ_CPLT",
                                   psf.when((~psf.col("HOS_TYP_UM_C").isin(["2", "3", "4"])),
                                            psf.col("PRE_JOU_NBR")).
                                   otherwise(0))

    # Créer la variable nombre de jours de présence en temps partiel
    ssr_valo = ssr_valo.withColumn("SEJ_NBJ_PARTL",
                                   psf.when(psf.col("HOS_TYP_UM_C").isin(["2", "3", "4"]), psf.col("PRE_JOU_NBR")).
                                   otherwise(0))

    # Créer la variable type de séjours avec 4 modalités : séjour en HP court / long, séjour en HC court
    # (moins de 30 jours), séjour en HC long (30 jours ou plus)
    ssr_valo = ssr_valo.withColumn("TYP_SEJ",
                                   psf.when(
                                       (psf.col("HOS_TYP_UM_C").isin(["2", "3", "4"])) & (psf.col("PRE_JOU_NBR") < 30),
                                       psf.lit("PARTL_COURT")).
                                   when(
                                       (psf.col("HOS_TYP_UM_C").isin(["2", "3", "4"])) & (psf.col("PRE_JOU_NBR") >= 30),
                                       psf.lit("PARTL_LONG")).
                                   when((~psf.col("HOS_TYP_UM_C").isin(["2", "3", "4"])) &
                                        (psf.col("PRE_JOU_NBR") < 30), psf.lit("CPLT_COURT")).
                                   when((~psf.col("HOS_TYP_UM_C").isin(["2", "3", "4"])) &
                                        (psf.col("PRE_JOU_NBR") >= 30), psf.lit("CPLT_LONG")))

    # Recalculer taux de remboursement selon les directives de l'ATIH
    ssr_valo = ssr_valo.withColumn("taux_atih",
                                   psf.when(psf.col("EXO_TM").isin(["X", ""]) | psf.col("EXO_TM").isNull() | psf.col(
                                       "NAT_ASS").isin(["XX", ""]), None).
                                   when(psf.col("EXO_TM").isin(["0", "2"]) & psf.col("NAT_ASS").isin(["10", "13"]),
                                        80).
                                   when(psf.col("EXO_TM").isin(["0", "2"]), 100).
                                   when(psf.col("EXO_TM").isin(["9"]) & psf.col("NAT_ASS").isin(["10"]), 90).
                                   otherwise(100))

    # Creer un TAUX_C corrige
    ssr_valo = ssr_valo.withColumn("TAUX_C",
                                   psf.when((psf.col("FAC_18E") == 1), 100)
                                   .when(psf.col("REM_TAU") == psf.col("taux_ATIH"), psf.col("REM_TAU"))
                                   .when(psf.col("taux_ATIH").isNull(), psf.col("REM_TAU"))
                                   .when(psf.col("REM_TAU") < 75, psf.col("taux_ATIH"))
                                   .when((psf.col("REM_TAU") == 0) &
                                         ((psf.col("TOT_MNT_AM") != 0) | (psf.col("FAC_MNT_TM") != 0)),
                                         psf.col("taux_ATIH"))
                                   .when(((psf.col("REM_TAU") == 100) & (psf.col("taux_ATIH") < 100) &
                                          (psf.col("FAC_MNT_TM") == 0)), psf.col("REM_TAU"))
                                   .when(((psf.col("taux_ATIH") == 100) & (psf.col("REM_TAU") < 100) &
                                          (psf.col("FAC_MNT_TM") == 0)), psf.col("taux_ATIH"))
                                   .when(((psf.col("REM_TAU") == 100) & (psf.col("taux_ATIH") < 100) &
                                          (psf.col("TOT_MNT_AMC") != 0)), psf.col("taux_ATIH"))
                                   .when(((psf.col("taux_ATIH") == 100) & (psf.col("REM_TAU") < 100) &
                                          (psf.col("TOT_MNT_AMC") != 0)), psf.col("REM_TAU"))
                                   .when(((psf.col("taux_ATIH") == 100) & (psf.col("REM_TAU") < 100) &
                                          (psf.col("TOT_MNT_AMC") == 0)), psf.col("taux_ATIH"))
                                   .when(((psf.col("REM_TAU") == 100) & (psf.col("taux_ATIH") < 100) &
                                          (psf.col("TOT_MNT_AMC") == 0)), psf.col("REM_TAU"))
                                   .when(~psf.col("taux_ATIH").isNull(), psf.col("taux_ATIH"))
                                   .otherwise(None))

    # assert ssr_valo.select(["TAUX_C"]).filter(psf.col("TAUX_C").isNull()).count() == 0

    # Supprimer lignes avec TAUX_C nul (c'est sûrement une erreur et on ne couvre que le champ des dépenses 
    # remboursables  - contrairement en MCO où on peut avoir des PO)
    ssr_valo = ssr_valo.filter(psf.col("TAUX_C") != 0)

    # Recalculer le montant reel 18E qui s'applique en cas de PF18
    ssr_valo = ssr_valo.withColumn("FAC_18E_C", psf.col("FAC_18E") * 18)
    # TM
    ssr_valo = ssr_valo.withColumn("DIFF", psf.col("REM_BAS_MNT") - psf.col("TOT_MNT_AM"))

    # Creation de la variable TM_C (ticket moderateur corrige)
    ssr_valo = (ssr_valo.withColumn("TM_C",
                                    psf.when((psf.col("TAUX_C") == 100), 0)
                                    .when(psf.col("FAC_MNT_TM").isNull(), 0)
                                    .when(psf.col("FAC_MNT_TM") < 0, 0)
                                    .otherwise(psf.col("FAC_MNT_TM"))
                                    )
                .withColumn("TM_C", psf.col("TM_C").cast(pst.DoubleType())))
    # assert ssr_valo.select(["TM_C"]).filter(psf.col("TM_C").isNull()).count() == 0

    # Gérer le cas des séjours de plus de 30 jours
    ssr_valo = ssr_valo.withColumn("TM_C",
                                   psf.when(psf.col("PRE_JOU_NBR") > 30,
                                            (psf.col("TM_C") * 30) / psf.col("PRE_JOU_NBR")).
                                   otherwise(psf.col("TM_C")))
    # FJ
    ssr_valo = ssr_valo.withColumn("FJ_NC",
                                   psf.when(((psf.col("duree_sejour") == 0) | (psf.col("PRE_JOU_NBR") == 0)), 0)
                                   .when((psf.col("duree_sejour") > 0) & (~psf.col("SOR_MOD").isin(["6", "7", "9"])),
                                         psf.col("PRE_JOU_NBR") * 18)
                                   .when((psf.col("duree_sejour") > 0) & (psf.col("SOR_MOD").isin(["6", "7", "9"])),
                                         (psf.col("PRE_JOU_NBR") - 1) * 18)
                                   .when((psf.col("duree_sejour") > 0) & (psf.col("SOR_MOD").isNull()),
                                         psf.col("PRE_JOU_NBR") * 18)
                                   .otherwise(None))

    ssr_valo = (ssr_valo.withColumn("FJ_C",
                                    psf.when(psf.col("FJ_COD_PEC") == "R", 0).
                                    when((psf.col("duree_sejour") == 0), 0).
                                    when(psf.col("HOS_TYP_UM_C").isin(["2", "3", "4"]), 0).
                                    when(psf.col("FAC_MNT_FJ") == psf.col("FJ_NC"), psf.col("FJ_NC")).
                                    when(psf.col("FAC_MNT_FJ").isNull(), psf.col("FJ_NC")).
                                    when(psf.col("FJ_NC").isNull(), psf.col("FAC_MNT_FJ")).
                                    when(psf.col("FAC_MNT_FJ") == (psf.col("FJ_NC") - 18), psf.col("FAC_MNT_FJ")).
                                    when(psf.col("FAC_MNT_FJ") == (psf.col("FJ_NC") + 18), psf.col("FAC_MNT_FJ")).
                                    when(psf.col("FAC_MNT_FJ") > psf.col("FJ_NC"), psf.col("FJ_NC")).
                                    otherwise(psf.col("FJ_NC")))
                .withColumn("FJ_C", psf.col("FJ_C").cast(pst.DoubleType())))
    # assert ssr_valo.select("FJ_C").filter(psf.col("FJ_C").isNull()).count() == 0

    # Ajouter variable FJ_C2 qui prenne la valeur :
    # 0 en cas de sejour ambulatoire ou d'exoneration du FJ
    # 0 en cas de séjour en hospialisation partielle
    # FJ total si taux de remboursement de 100%
    # FJ jour de sortie si pas d'exoneration de TM (ni de PF18) et sejour < 30 j
    # FJ au-dela du 30 eme jour en cas de sejour long sans exoneration de TM
    # RQ : pas de valeurs manquantes pour PRE_JOU_NBR ni duree_sejour
    ssr_valo = ssr_valo.withColumn("FJ_C2",
                                   psf.when(psf.col("FJ_C") == 0, 0).
                                   when((psf.col("TAUX_C") == 100), psf.col("FJ_C")).
                                   when(
                                       (psf.col("PRE_JOU_NBR") <= 30) & (psf.col("SOR_MOD").isin(["0", "6", "7", "9"])),
                                       0).
                                   when((psf.col("PRE_JOU_NBR") <= 30), 18).
                                   # Cas des séjours en HC de plus de 30 jours de présence (FJ au-delà du 30 ème jour)
                                   when((psf.col("PRE_JOU_NBR") > 30) & (psf.col("SOR_MOD").isin(["0", "6", "7", "9"])),
                                        (psf.col("PRE_JOU_NBR") - 31) * 18).
                                   when((psf.col("PRE_JOU_NBR") > 30), (psf.col("PRE_JOU_NBR") - 30) * 18).
                                   # Cas des séjours en HC de moins de 30 jours (pour les cas avec "PRE_JOU_NBR" 
                                   # manquant mais pas "duree_sejour")
                                   # Car si "duree_sejour" < 30, PRE_JOU_NBR l'est forcément aussi
                                   when((psf.col("duree_sejour") > 0) & (psf.col("duree_sejour") <= 30) & (
                                       psf.col("SOR_MOD").isin(["0", "6", "7", "9"])), 0).
                                   when((psf.col("duree_sejour") > 0) & (psf.col("duree_sejour") <= 30), 18).
                                   # on ne peut rien faire si duree_sejour > 30 mais PRE_JOU_NBR 
                                   # manquant
                                   # On remplace par FJ_NC en cas de valeurs nulles
                                   when(psf.col("PRE_JOU_NBR").isNull(), psf.col("FJ_C")).
                                   when(psf.col("duree_sejour").isNull(), psf.col("FJ_C")).
                                   otherwise(psf.lit(None)))
    ssr_valo = ssr_valo.withColumn("FJ_C2", psf.col("FJ_C2").cast(pst.DoubleType()))
    # assert ssr_valo.select("FJ_C2").filter(psf.col("FJ_C2").isNull()).count() == 0

    # Identifier la dépense pour soins conformes au protocole ALD
    ssr_valo = (ssr_valo.withColumn("dep_lien_ald", psf.when(psf.col("EXO_TM").isin([2, 4]), 1).otherwise(0))
                .withColumn("rac_lien_ald", psf.when(psf.col("EXO_TM").isin([2, 4]), 1).otherwise(0)))

    # PF effectivement facturée
    ssr_valo = ssr_valo.withColumn("PF_sej",
                                   psf.when((psf.col("TAUX_C") == 100), psf.col("FAC_18E_C")).
                                   when(psf.col("TAUX_C") != 100, 0).
                                   otherwise(None))

    # TM effectivement facturé
    ssr_valo = ssr_valo.withColumn("TM_sej",
                                   psf.when(psf.col("TAUX_C") == 100, 0).
                                   when((psf.col("TAUX_C") != 100) & (psf.col("FJ_COD_PEC") == "R"),
                                        psf.col("TM_C")).
                                   when((psf.col("TAUX_C") != 100) & (psf.col("TM_C") > psf.col("FJ_C")),
                                        psf.col("TM_C")).
                                   when((psf.col("TAUX_C") != 100) & (psf.col("TM_C") <= psf.col("FJ_C")), 0).
                                   otherwise(None))

    # # Nettoyage 
    # Cas des séjours avec TM > BR 
    ssr_valo = ssr_valo.withColumn("TM_sej",
                                   psf.when(
                                       (psf.col("TM_sej") >= psf.col("REM_BAS_MNT")) & (psf.col("REM_BAS_MNT") != 0),
                                       psf.col("REM_BAS_MNT") * (100 - psf.col("TAUX_C")) / 100).
                                   otherwise(psf.col("TM_sej")))
    # FJ effectivement facturé
    ssr_valo = ssr_valo.withColumn("FJ_sej",
                                   psf.when(psf.col("FJ_COD_PEC") == "R", 0).
                                   when((psf.col("TAUX_C") == 100), psf.col("FJ_C2")).
                                   when((psf.col("TAUX_C") != 100) & (psf.col("TM_C") > psf.col("FJ_C")),
                                        psf.col("FJ_C2")).
                                   when((psf.col("TAUX_C") != 100) & (psf.col("TM_C") <= psf.col("FJ_C")),
                                        psf.col("FJ_C")).
                                   otherwise(None))

    # Calculer le RAC en sommant ces termes
    ssr_valo = ssr_valo.withColumn("RAC", psf.col("PF_sej") + psf.col("TM_sej") + psf.col("FJ_sej"))

    # Le montant total du séjour (pour l'AM) est donc la somme du montant "soi-disant" facturé à l'AM et du RaC
    ssr_valo = ssr_valo.withColumn("tot_dep", psf.col("TOT_MNT_AM") + psf.col("RAC"))

    # On recalcule également la dépense faite au titre de l'ALD
    ssr_valo = ssr_valo.withColumn("dep_lien_ald", psf.col("dep_lien_ald") * psf.col("tot_dep"))

    # Commencer par ajouter le TOP CMU
    ir_orc_r = _load_ir_orc(path2_ir_orc, year, spark)
    ssr_valo = (ssr_valo.join(ir_orc_r, ["NUM_ENQ"], how="left")
                .withColumn("top_cmu", psf.when(psf.col("top_cmu") == 1, 1).otherwise(0))

                # Pour les CMU-cistes, le RAC AMO supp est à 0 (prise en charge TM + FJ par l'Etat)
                .withColumn("rac_amo_sup", psf.when(psf.col("top_cmu") == 1, 0).otherwise(psf.col("RAC")))
                .withColumn("TM_sej", psf.when(psf.col("top_cmu") == 1, 0).otherwise(psf.col("TM_sej")))
                .withColumn("PF_sej", psf.when(psf.col("top_cmu") == 1, 0).otherwise(psf.col("PF_sej")))
                .withColumn("FJ_sej", psf.when(psf.col("top_cmu") == 1, 0).otherwise(psf.col("FJ_sej")))
                # Puis on recalcule le rac amo sup pour les dépenses faites au titre de l'ALD
                .withColumn("rac_lien_ald", psf.col("rac_lien_ald") * psf.col("rac_amo_sup")))

    for c in ["NBSEJ_PARTL_COURT", "NBSEJ_PARTL_LONG", "NBSEJ_CPLT_COURT", "NBSEJ_CPLT_LONG"]:
        ssr_valo = ssr_valo.withColumn(c, psf.when(psf.col("TYP_SEJ") == c.replace("NBSEJ_", ""), 1).otherwise(0))

    # Par bénéficiaire
    rac_ssr = ssr_valo.groupBy(psf.col("NUM_ENQ")).agg(
        {"RAC": "sum", "rac_amo_sup": "sum", "tot_dep": "sum", "PF_sej": "sum", "TM_sej": "sum", "FJ_sej": "sum",
         "NUM_ENQ": "count",
         "TOT_MNT_AM": "sum", "SEJ_NBJ_CPLT": "sum", "SEJ_NBJ_PARTL": "sum", "NBSEJ_PARTL_COURT": "sum",
         "NBSEJ_PARTL_LONG": "sum", "NBSEJ_CPLT_COURT": "sum", "NBSEJ_CPLT_LONG": "sum",
         "dep_lien_ald": "sum", "rac_lien_ald": "sum", })

    output_var = ['NUM_ENQ', 'NBSEJ_PARTL_COURT', 'MNT_PF', 'NBSEJ_PARTL_LONG', 'NBJ_PARTL', 'NBSEJ_CPLT_LONG',
                  'rac_lien_ald', 'rac', 'NBJ_CPLT', 'nb_sejour', 'remb_am', 'dep_tot', 'NBSEJ_CPLT_COURT',
                  'MNT_TM', 'MNT_FJ', 'dep_lien_ald', 'rac_amo_sup']
    input_var = ['NUM_ENQ', 'sum(NBSEJ_PARTL_COURT)', 'sum(PF_sej)', 'sum(NBSEJ_PARTL_LONG)', 'sum(SEJ_NBJ_PARTL)',
                 'sum(NBSEJ_CPLT_LONG)', 'sum(rac_lien_ald)', 'sum(RAC)', 'sum(SEJ_NBJ_CPLT)', 'count(NUM_ENQ)',
                 'sum(TOT_MNT_AM)', 'sum(tot_dep)', 'sum(NBSEJ_CPLT_COURT)', 'sum(TM_sej)', 'sum(FJ_sej)',
                 'sum(dep_lien_ald)', 'sum(rac_amo_sup)']
    mapping = dict(zip(input_var, output_var))
    rac_ssr = rac_ssr.select([psf.col(c).alias(mapping.get(c, c)) for c in rac_ssr.columns])

    # Ajouter poste et poste aggrege
    rac_ssr = (rac_ssr.withColumn("poste_ag", psf.lit("etab_public_ssr"))
               .withColumn("poste", psf.lit("sejour")))

    # Ajouter informations individuelles 
    rac_ssr = rac_ssr.join(ssr_valo.select(["NUM_ENQ", "BDI_DEP", "BDI_COD", "COD_SEX", "AGE_ANN"]).distinct(),
                           ["NUM_ENQ"], how="left")

    # Selectionner et réordonner colonnes pour qu'elles soient dans le même ordre
    rac_ssr = rac_ssr.select(list_output_var)

    return rac_ssr


def agg_med(path2flat, path2csv_ssr, rac_ssr, year, list_output_var, spark):
    ssr_c = _load_ssr_c(path2flat, year, spark)
    if year == 2017:
        path_temp = path2csv_ssr + "MED.CSV"
    elif year == 2016:
        path_temp = path2csv_ssr + "med.csv"
    else:
        raise ValueError("Erreur année non prise en charge")
    ssr_med = (spark.read
               .option("header", "true")
               .option("sep", ";")
               .csv(path_temp)
               .select(["ETA_NUM", "RHA_NUM", "ACH_PRI_ADM", "ADM_NBR", "UCD_UCD_COD", "ADM_MOIS", "ADM_ANN"])
               .withColumn("ACH_PRI_ADM", psf.col("ACH_PRI_ADM").cast(pst.DoubleType()))
               .withColumn("ADM_NBR", psf.col("ADM_NBR").cast(pst.DoubleType()))
               .filter((psf.col("ADM_NBR") != 0) & (psf.col("ADM_NBR") < 100) & (psf.col("ACH_PRI_ADM") != 0))
               .filter(psf.col("ADM_ANN") <= year)
               )
    if year == 2017:
        path_temp = path2csv_ssr + "MEDATU.CSV"
    elif year == 2016:
        path_temp = path2csv_ssr + "medatu.csv"
    else:
        raise ValueError("Erreur année non prise en charge")
    ssr_medatu = (spark.read
                  .option("header", "true")
                  .option("sep", ";")
                  .csv(path_temp)
                  .select(["ETA_NUM", "RHA_NUM", "ACH_PRI_ADM", "ADM_NBR", "UCD_UCD_COD", "ADM_MOIS", "ADM_ANN"])
                  .withColumn("ACH_PRI_ADM", psf.col("ACH_PRI_ADM").cast(pst.DoubleType()))
                  .withColumn("ADM_NBR", psf.col("ADM_NBR").cast(pst.DoubleType()))
                  .filter((psf.col("ADM_NBR") != 0) & (psf.col("ADM_NBR") < 100) & (psf.col("ACH_PRI_ADM") != 0))
                  .filter(psf.col("ADM_ANN") <= year)
                  )

    # Est-ce que les médicaments de med et medatu sont complémentaires ?
    temp = ssr_med.join(ssr_medatu, ["ETA_NUM", "RHA_NUM", "UCD_UCD_COD"], how="inner")

    # Décision : suppression des doublons
    # Extraire les triplets "ETA_NUM","RHA_NUM","UCD_UCD_COD" à exclure de ssr_medatu
    temp = temp.withColumn("triplet",
                           psf.concat_ws("-", psf.col("ETA_NUM"), psf.col("RHA_NUM"), psf.col("UCD_UCD_COD")))
    temp = temp.select(["triplet"])
    temp = temp.dropDuplicates()
    temp = temp.withColumn("a_excl", psf.lit(1.0))

    # Supprimer les lignes de medatu qui sont aussi dans med
    ssr_medatu = ssr_medatu.withColumn("triplet",
                                       psf.concat_ws("-", psf.col("ETA_NUM"), psf.col("RHA_NUM"),
                                                     psf.col("UCD_UCD_COD")))
    ssr_medatu = ssr_medatu.join(temp, ["triplet"], how="left")

    # Supprimer les lignes pour lesquelles a_excl prend la valeur 1 (61 lignes)
    ssr_medatu = ssr_medatu.filter(psf.col("a_excl").isNull())

    # Regrouper dépenses médicaments et sommer par séjour (en conservant la distinction entre les tables)
    ssr_stc = _load_ssr_stc(path2csv_ssr, year, spark)

    dict_dfs = dict({"ssr_med": ssr_med, "ssr_atu": ssr_medatu})
    for k, df in dict_dfs.items():
        df = (df.groupBy([psf.col("ETA_NUM"), psf.col("RHA_NUM")]).agg({"ACH_PRI_ADM": "sum"})
              .withColumnRenamed("sum(ACH_PRI_ADM)", "dep_tot"))
        df = (df.filter(psf.col("dep_tot") < 1000000)
              .join(ssr_stc.select(["ETA_NUM", "RHA_NUM", "EXO_TM"]), ["ETA_NUM", "RHA_NUM"], how="inner")
              .withColumn("dep_lien_ald", psf.when(psf.col("EXO_TM").isin([2, 4]), 1).otherwise(0))
              .withColumn("dep_lien_ald", psf.col("dep_lien_ald") * psf.col("dep_tot")))
        df = df.join(ssr_c.select(["ETA_NUM", "RHA_NUM", "NUM_ENQ"]), ["ETA_NUM", "RHA_NUM"], how="left")
        df = (df.groupBy(psf.col("NUM_ENQ")).agg({"dep_tot": "sum", "dep_lien_ald": "sum", "NUM_ENQ": "count"})
              .withColumnRenamed("sum(dep_tot)", "dep_tot")
              .withColumnRenamed("count(NUM_ENQ)", "nb_sejour")
              .withColumnRenamed("sum(dep_lien_ald)", "dep_lien_ald")
              .withColumn("poste_ag", psf.lit("etab_public_ssr"))
              .withColumn("rac", psf.lit(0.0))
              .withColumn("rac_lien_ald", psf.lit(0.0))
              .select(['NUM_ENQ', 'nb_sejour', 'dep_tot', 'dep_lien_ald', 'rac_lien_ald', 'poste_ag', 'rac']))
        dict_dfs[k] = df

    ssr_med = dict_dfs["ssr_med"]
    ssr_medatu = dict_dfs["ssr_atu"]

    # Ajouter colonnes poste
    ssr_med = ssr_med.withColumn("poste", psf.lit("Med_SUS"))
    ssr_medatu = ssr_medatu.withColumn("poste", psf.lit("Med_ATU"))

    ssr_public_sus = ssr_med.union(ssr_medatu)

    # Ajouter colonne "remb_am"
    ssr_public_sus = ssr_public_sus.withColumn("remb_am", psf.col("dep_tot"))
    # Ajouter colonne "rac_amo_sup"
    ssr_public_sus = ssr_public_sus.withColumn("rac_amo_sup", psf.col("rac"))

    for c in ["MNT_TM", "MNT_PF", "MNT_FJ", "NBSEJ_CPLT_LONG", "NBSEJ_CPLT_COURT", "NBSEJ_PARTL_COURT",
              "NBSEJ_PARTL_LONG", "NBJ_CPLT", "NBJ_PARTL"]:
        ssr_public_sus = ssr_public_sus.withColumn(c, psf.lit(None))

    ssr_public_sus = ssr_public_sus.join(
        rac_ssr.select(["NUM_ENQ", "BDI_DEP", "BDI_COD", "COD_SEX", "AGE_ANN"]).distinct(), "NUM_ENQ", how="inner")

    # Selectionner et réordonner colonnes pour qu'elles soient dans le même ordre
    ssr_public_sus = ssr_public_sus.select(list_output_var)

    return ssr_public_sus


def agg_ace(path2flat, path2csv_ssr, path2nomenclature, path2_ir_orc, filter_etab, year, spark, list_output_var):
    ace_c = (spark.read
             .parquet(path2flat + "single_table/" "SSR_CSTC")
             .select(["NUM_ENQ", "ETA_NUM", "SEQ_NUM", "EXE_SOI_DTD", "EXE_SOI_DTF"])
             .withColumnRenamed("SEQ_NUM", "RHA_NUM")
             )
    if year == 2017:
        path_temp = path2csv_ssr + "FBSTC.CSV"
    elif year == 2016:
        path_temp = path2csv_ssr + "fbstc.csv"
    else:
        raise ValueError("Erreur année non prise en charge")
    fbstc = (spark.read
             .option("header", "true")
             .option("sep", ";")
             .csv(path_temp)
             .select(["ETA_NUM", "SEQ_NUM", "ACT_COD", "EXE_SPE", "EXO_TM"])
             .withColumnRenamed("SEQ_NUM", "RHA_NUM")
             .withColumn("ATU", psf.when(psf.col("ACT_COD") == "ATU", 1).otherwise(0))
             .groupBy(["ETA_NUM", "RHA_NUM"]).agg({"ATU": "max", "EXE_SPE": "max", "EXO_TM": "collect_set"})
             .withColumnRenamed("max(ATU)", "ATU")
             .withColumnRenamed("max(EXE_SPE)", "EXE_SPE_fbstc")
             .withColumnRenamed("collect_set(EXO_TM)", "EXO_TM")
             .withColumn("dep_lien_ald_fbstc", psf.when((psf.array_contains(psf.col("EXO_TM"), '2')) |
                                                        (psf.array_contains(psf.col("EXO_TM"), '4')), 1).otherwise(0))
             )

    if year == 2017:
        path_temp = path2csv_ssr + "FCSTC.CSV"
    elif year == 2016:
        path_temp = path2csv_ssr + "fcstc.csv"
    else:
        raise ValueError("Erreur année non prise en charge")
    fcstc = (spark.read
             .option("header", "true")
             .option("sep", ";")
             .csv(path_temp)
             .withColumnRenamed("SEQ_NUM", "RHA_NUM")
             .groupBy(["ETA_NUM", "RHA_NUM"]).agg({"EXE_SPE": "max", "EXO_TM": "collect_set"})
             .withColumnRenamed("max(EXE_SPE)", "EXE_SPE_fcstc")
             .withColumnRenamed("collect_set(EXO_TM)", "EXO_TM")
             .withColumnRenamed("collect_set(EXO_TM)", "EXO_TM")
             .withColumn("dep_lien_ald_fcstc", psf.when((psf.array_contains(psf.col("EXO_TM"), '2')) |
                                                        (psf.array_contains(psf.col("EXO_TM"), '4')), 1).otherwise(0))
             )

    fcstc = (fcstc.drop("EXO_TM").join(fbstc.drop("EXO_TM"), ["ETA_NUM", "RHA_NUM"], how="full")
             .withColumn("ATU",
                         psf.when(psf.col("ATU") == 1, psf.col("ATU")).
                         otherwise(0))
             .withColumn('EXE_SPE',
                         psf.when((psf.col("EXE_SPE_fcstc").isNull()) & (psf.col("EXE_SPE_fbstc").isNotNull()),
                                  psf.col("EXE_SPE_fbstc")).
                         when((psf.col("EXE_SPE_fcstc").isNotNull()) & (psf.col("EXE_SPE_fbstc").isNull()),
                              psf.col("EXE_SPE_fcstc")).
                         when(psf.col("EXE_SPE_fcstc") >= psf.col("EXE_SPE_fbstc"), psf.col("EXE_SPE_fcstc")).
                         when(psf.col("EXE_SPE_fcstc") < psf.col("EXE_SPE_fbstc"), psf.col("EXE_SPE_fbstc")))
             .drop("EXE_SPE_fcstc", "EXE_SPE_fbstc")
             .withColumn('dep_lien_ald',
                         psf.when(psf.col("dep_lien_ald_fbstc") >= psf.col("dep_lien_ald_fcstc"),
                                  psf.col("dep_lien_ald_fbstc")).
                         otherwise(psf.col("dep_lien_ald_fcstc")))
             .drop("dep_lien_ald_fbstc", "dep_lien_ald_fcstc")
             # En cas de plusieurs EXE_SPE et dep_lien_ald pour un meme ETA_NUM, RHA_NUM, ne garder que les max 
             .groupBy('ETA_NUM', 'RHA_NUM', "ATU").agg({'EXE_SPE': "max", "dep_lien_ald": "max"})
             .withColumnRenamed("max(EXE_SPE)", "EXE_SPE")
             .withColumnRenamed("max(dep_lien_ald)", "dep_lien_ald")

             )

    ace_c = (ace_c.join(fcstc, ["ETA_NUM", "RHA_NUM"], how="left").fillna({"ATU": 0})
             .withColumn("EXE_SPE",
                         psf.when(psf.col("EXE_SPE").isNull(), 0).
                         otherwise(psf.col("EXE_SPE")))
             .withColumnRenamed("EXE_SPE", "PSE_ACT_SPE")
             )

    # table de valorisation des consultations externes à l'hôpital public
    if year == 2017:
        path_temp = path2csv_ssr + "FASTC.CSV"
    elif year == 2016:
        path_temp = path2csv_ssr + "fastc.csv"
    else:
        raise ValueError("Erreur année non prise en charge")
    fa_ace = (spark.read
              .option("header", "true")
              .option("sep", ";")
              .csv(path_temp)
              .select(["ETA_NUM", "SEQ_NUM", "NAT_ASS", "EXO_TM", "PH_BRM", "PH_AMO_MNR", "HON_MNT",
                       "HON_AM_MNR", "PAS_OC_MNT", "PH_OC_MNR", "HON_OC_MNR", "PH_MNT", "PAT_CMU", "NUM_FAC",
                       "BDI_DEP", "BDI_COD", "COD_SEX", "AGE_ANN"])
              .withColumnRenamed("SEQ_NUM", "RHA_NUM")
              .fillna({"PAT_CMU": 0})
              )
    for c in ["PH_BRM", "PH_AMO_MNR", "HON_MNT", "HON_AM_MNR", "PAS_OC_MNT", "PH_OC_MNR", "HON_OC_MNR", "PH_MNT"]:
        fa_ace = fa_ace.withColumn(c, psf.col(c).cast(pst.DoubleType()))

    ace_valo = fa_ace.join(ace_c, ["ETA_NUM", "RHA_NUM"], how="inner")

    ace_valo = ace_valo.filter(~psf.col("ETA_NUM").isin(filter_etab))

    # Supprimer les ACE qui n'ont pas eu lieu dans l'année
    ace_valo = (ace_valo.filter((psf.col("EXE_SOI_DTD") <= str(year) + "-12-31") & (~psf.col("EXE_SOI_DTD").isNull()))
                .filter((psf.col("EXE_SOI_DTF") <= str(year) + "-12-31") & (~psf.col("EXE_SOI_DTF").isNull()))
                )
    # Création des postes détaillés à partir de la spécialité du PS exécutant
    specialite = (spark.read
                  .option("header", "true")
                  .option("sep", ",")
                  .csv(path2nomenclature)
                  .select(["pse_act_spe", "spe_act_det"])
                  .filter(psf.col("spe_act_det").isNotNull())
                  .withColumnRenamed("pse_act_spe", "PSE_ACT_SPE")
                  .filter(psf.col("spe_act_det") != "NA")
                  .withColumn("PSE_ACT_SPE", psf.col("PSE_ACT_SPE").cast(pst.IntegerType()))
                  )

    ace_valo = (ace_valo.join(psf.broadcast(specialite), ["PSE_ACT_SPE"], "left")
                .withColumn("poste",
                            psf.when(psf.col("spe_act_det") == "Labo", psf.lit("Labo")).
                            when(psf.col("spe_act_det") == "Fournisseur",
                                 psf.lit("autres_spe_cliniq")).
                            when(psf.col("spe_act_det") == "Pharma_", psf.lit("Pharma")).
                            when(psf.col("spe_act_det").isNotNull(),
                                 psf.concat(psf.col("spe_act_det"), psf.lit("_cliniq"))).
                            otherwise(psf.lit("autres_spe_cliniq"))
                            )
                .withColumn("poste_ag", psf.lit("etab_public_ssr_ace"))
                )
    # Calcul du RAC et du RAC AMO SUP
    ace_valo = (ace_valo.withColumn("remb_am", psf.col("PH_AMO_MNR") + psf.col("HON_AM_MNR"))
                .withColumn("fact_tot", psf.col("PH_MNT") + psf.col("HON_MNT")))
    # Supprimer les lignes où remb_am est nul (on ne veut que les dépenses remboursables)
    ace_valo = ace_valo.filter(psf.col("remb_am") != 0)

    # DECISION dans ces cas-là remplacer fact_tot par remb_am
    ace_valo = ace_valo.withColumn("fact_tot",
                                   psf.when(psf.col("fact_tot") < psf.col("remb_am"), psf.col("remb_am")).
                                   otherwise(psf.col("fact_tot")))

    # Calcul du RaC par consultation
    ace_valo = ace_valo.select("NUM_ENQ", "BDI_DEP", "BDI_COD", "COD_SEX", "AGE_ANN", "remb_am", "fact_tot",
                               "PAS_OC_MNT", "poste", "poste_ag", "dep_lien_ald")
    ace_valo = ace_valo.withColumn("rac_sej", psf.col("fact_tot") - psf.col("remb_am"))

    # Recalculer la dépense au titre de l'ALD
    ace_valo = ace_valo.withColumn("rac_lien_ald", psf.col("dep_lien_ald"))
    ace_valo = ace_valo.withColumn("dep_lien_ald", psf.col("dep_lien_ald") * psf.col("fact_tot"))

    # Rac amo sup par consultation
    # Commencer par ajouter le TOP CMU
    ir_orc_r = _load_ir_orc(path2_ir_orc, year, spark)

    ace_valo = (ace_valo.join(ir_orc_r, ["NUM_ENQ"], how="left")
                .withColumn("top_cmu", psf.when(psf.col("top_cmu") == 1, 1).
                            otherwise(0))

                # Pour les CMU-cistes, le RAC AMO supp est à 0 (prise en charge TM + FJ par l'Etat)
                .withColumn("rac_amo_sup", psf.when(psf.col("top_cmu") == 1, 0).
                            otherwise(psf.col("rac_sej")))
                # Recalculer le rac au titre de l'ALD
                .withColumn("rac_lien_ald", psf.col("rac_lien_ald") * psf.col("rac_sej")))

    # RAC par bénéficiaire
    rac_ace = (ace_valo.groupBy(["NUM_ENQ", "poste", "poste_ag"])
               .agg({"remb_am": "sum", "fact_tot": "sum", "PAS_OC_MNT": "sum", "rac_sej": "sum",
                     "NUM_ENQ": "count", "dep_lien_ald": "sum", "rac_lien_ald": "sum", "rac_amo_sup": "sum"})
               .withColumnRenamed("sum(remb_am)", "remb_am")
               .withColumnRenamed("sum(fact_tot)", "dep_tot")
               .withColumnRenamed("sum(PAS_OC_MNT)", "PAS_OC_MNT")
               .withColumnRenamed("sum(rac_sej)", "rac")
               .withColumnRenamed("count(NUM_ENQ)", "nb_sejour")
               .withColumnRenamed("sum(dep_lien_ald)", "dep_lien_ald")
               .withColumnRenamed("sum(rac_lien_ald)", "rac_lien_ald")
               .withColumnRenamed("sum(rac_amo_sup)", "rac_amo_sup"))

    # Ajouter colonnes vides pour que toutes les bases aient les mêmes colonnes
    for c in ["MNT_TM", "MNT_PF", "MNT_FJ", "NBSEJ_CPLT_LONG", "NBSEJ_CPLT_COURT", "NBSEJ_PARTL_COURT",
              "NBSEJ_PARTL_LONG", "NBJ_CPLT", "NBJ_PARTL"]:
        rac_ace = rac_ace.withColumn(c, psf.lit(None))

    # Ajouter informations individuelles 
    rac_ace = rac_ace.join(ace_valo.select(["NUM_ENQ", "BDI_DEP", "BDI_COD", "COD_SEX", "AGE_ANN"]).distinct(),
                           ["NUM_ENQ"], how="left")

    # Selectionner et réordonner colonnes pour qu'elles soient dans le même ordre
    rac_ace = rac_ace.select(list_output_var)
    return rac_ace

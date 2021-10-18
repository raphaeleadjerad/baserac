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
from .hospit import _compare_ucd
from .utils import union_all
from .hospit import _load_ir_orc


def _load_had_c(path2flat, year, spark):
    if year in [2016, 2017]:
        had_c = (spark.read
             .parquet(path2flat + "single_table/HAD_C/year=" + str(year))
             .select(["NUM_ENQ", "ETA_NUM_EPMSI", "RHAD_NUM", "EXE_SOI_DTD", "EXE_SOI_DTF"])
             )
    else:
        raise ValueError("Année non prise en charge")
    return had_c


def _load_had_stc(path2csv_had, year, spark):
    if year == 2017:
        path_temp = path2csv_had + "STC.CSV"
    elif year == 2016:
        path_temp = path2csv_had + "stc.csv"
    else:
        raise ValueError("Erreur année non prise en charge")
    had_stc = (spark.read
               .option("header", "true")
               .option("sep", ";")
               .csv(path_temp)
               .select(["ETA_NUM_EPMSI", "RHAD_NUM", "FAC_SEJ_AM", "TOT_MNT_AM", "TOT_MNT_AMC", "REM_BAS_MNT",
                        "FAC_MNT_TM", "FAC_MNT_FJ", "MAJ_MNT_PS", "REM_TAU", "NAT_ASS", "EXO_TM", "FJ_COD_PEC",
                        "FAC_18E", "PAT_CMU", "FAC_NBR_VEN"])
               )
    return had_stc


def agg_had(path2flat, path2csv_had, path2_ir_orc, filter_etab, year, list_output_var, spark):
    had_c = _load_had_c(path2flat, year, spark)

    had_stc = _load_had_stc(path2csv_had, year, spark)
    had_valo = had_c.join(had_stc, ["ETA_NUM_EPMSI", "RHAD_NUM"], how="inner")

    if year == 2017:
        path_temp = path2csv_had + "S.CSV"
    elif year == 2016:
        path_temp = path2csv_had + "s.csv"
    else:
        raise ValueError("ERREUR annee non prise en charge")
    had_s = (spark.read
             .option("header", "true")
             .option("sep", ";")
             .csv(path_temp)
             .select(["ETA_NUM_EPMSI", "RHAD_NUM", "HAD_DUREE", "SEJ_NBJ", "SEJ_ERR", "SEJ_FINI"])
             )
    had_valo = had_valo.join(had_s, ["ETA_NUM_EPMSI", "RHAD_NUM"], how="left")

    # Ajouter informations individuelles dans la table B : code département, code géographique, sexe et âge en années 
    had_b = (spark.read
             .parquet(path2flat + "single_table/" + "HAD_B")
             .select(["ETA_NUM_EPMSI", "RHAD_NUM", "BDI_DEP", "BDI_COD", "COD_SEX", "AGE_ANN"])
             .distinct()
             )

    # On fusionne les tables
    had_valo = had_valo.join(had_b, ["ETA_NUM_EPMSI", "RHAD_NUM"], how="left")

    # Supprimer les sejours non valorises ou en attente de valorisation
    had_valo = had_valo.filter(psf.col("FAC_SEJ_AM") == 1)

    # Suppression des sejours sans information de facturation à l'AM
    had_valo = had_valo.filter(psf.col("TOT_MNT_AM") != 0)

    # Suppression des séjours 100% en erreur, la valeur a été modifée entre 2016 et 2017
    if year == 2017:
        had_valo = had_valo.filter(psf.col("SEJ_ERR") == 0)
    elif year == 2016:
        had_valo = had_valo.filter(psf.col("SEJ_ERR") == 2)
    else:
        raise ValueError("Année non prise en charge")

    # Filtre sur les etablissements
    had_valo = had_valo.filter(~psf.col("ETA_NUM_EPMSI").isin(filter_etab))

    # Suppression des séjours non finis dans l'année
    had_valo = had_valo.filter(psf.col("SEJ_FINI") == 1)

    # Suppression des sejours non finis le dernier jour de l'année
    had_valo = (had_valo.filter(psf.col("EXE_SOI_DTF").isNotNull())
                .filter(psf.col("EXE_SOI_DTF") != "")
                .filter(psf.col("EXE_SOI_DTF") <= str(year) + "-12-31"))

    # Creation de la variable de duree de sejour
    had_valo = had_valo.withColumn("duree_sejour", psf.datediff(psf.col("EXE_SOI_DTF"), psf.col("EXE_SOI_DTD")))

    had_valo = had_valo.withColumn("TYP_SEJ",
                                   psf.when(psf.col("HAD_DUREE") < 30, psf.lit("PARTL_COURT")).
                                   when(psf.col("HAD_DUREE") >= 30, psf.lit("PARTL_LONG")))

    # Passer variables en double
    for c in ["REM_TAU", "REM_BAS_MNT", "TOT_MNT_AM", "FAC_MNT_TM", "TOT_MNT_AMC"]:
        had_valo = had_valo.withColumn(c, psf.col(c).cast(pst.DoubleType()))

    # Recalculer taux de remboursement selon les directives de l'ATIH
    had_valo = had_valo.withColumn("taux_atih",
                                   psf.when(psf.col("EXO_TM").isin(["X"]) | psf.col("EXO_TM").isNull() | psf.col(
                                       "NAT_ASS").isin(["XX"]), None).
                                   when(psf.col("EXO_TM").isin(["0", "2"]) & psf.col("NAT_ASS").isin(["10", "13"]),
                                        80).
                                   when(psf.col("EXO_TM").isin(["0", "2"]), 100).
                                   when(psf.col("EXO_TM").isin(["9"]) & psf.col("NAT_ASS").isin(["10"]), 90).
                                   otherwise(100))

    # Creer un TAUX_C corrige
    had_valo = had_valo.withColumn("TAUX_C",
                                   psf.when((psf.col("FAC_18E") == 1), 100).
                                   when(psf.col("REM_TAU") == psf.col("taux_ATIH"), psf.col("REM_TAU")).
                                   when(psf.col("taux_ATIH").isNull(), psf.col("REM_TAU")).
                                   when((psf.col("REM_TAU") == 0) & (
                                           (psf.col("TOT_MNT_AM") != 0) | (psf.col("FAC_MNT_TM") != 0)),
                                        psf.col("taux_ATIH")).
                                   when(((psf.col("REM_TAU") == 100) & (psf.col("taux_ATIH") < 100) & (
                                           psf.col("FAC_MNT_TM") == 0)), psf.col("REM_TAU")).
                                   when(((psf.col("taux_ATIH") == 100) & (psf.col("REM_TAU") < 100) & (
                                           psf.col("FAC_MNT_TM") == 0)), psf.col("taux_ATIH")).
                                   when(((psf.col("REM_TAU") == 100) & (psf.col("taux_ATIH") < 100) & (
                                           psf.col("TOT_MNT_AMC") != 0)), psf.col("taux_ATIH")).
                                   when(((psf.col("taux_ATIH") == 100) & (psf.col("REM_TAU") < 100) & (
                                           psf.col("TOT_MNT_AMC") != 0)), psf.col("REM_TAU")).
                                   when(((psf.col("taux_ATIH") == 100) & (psf.col("REM_TAU") < 100) & (
                                           psf.col("TOT_MNT_AMC") == 0)), psf.col("taux_ATIH")).
                                   when(((psf.col("REM_TAU") == 100) & (psf.col("taux_ATIH") < 100) & (
                                           psf.col("TOT_MNT_AMC") == 0)), psf.col("REM_TAU")).
                                   when(~psf.col("taux_ATIH").isNull(), psf.col("taux_ATIH")).
                                   otherwise(None))
    # assert had_valo.select(["TAUX_C"]).filter(psf.col("TAUX_C").isNull()).count() == 0

    # Supprimer ligne avec TAUX_C nul
    had_valo = had_valo.filter(psf.col("TAUX_C") != 0)

    # Participation forfaitaire 18 euros
    had_valo = had_valo.withColumn("FAC_18E_C", psf.when((psf.col("FAC_18E") == 1), 18).otherwise(0))

    # Ticket moderateur
    had_valo = had_valo.withColumn("DIFF", psf.col("REM_BAS_MNT") - psf.col("TOT_MNT_AM"))

    # Creation de la variable TM_C (ticket moderateur corrige)
    had_valo = had_valo.withColumn("TM_C",
                                   psf.when((psf.col("TAUX_C") == 100), 0).
                                   when((psf.col("TAUX_C") < 100) & (psf.col("FAC_MNT_TM") == 0), psf.col("DIFF")).
                                   when(psf.col("TAUX_C") < 100, psf.col("FAC_MNT_TM")).
                                   otherwise(None)).withColumn("TM_C", psf.col("TM_C").cast(pst.DoubleType()))

    # assert had_valo.select(["TM_C"]).filter(psf.col("TM_C").isNull()).count() == 0

    # Gérer le cas des séjours de plus de 30 jours 

    # Normalement le TM est exonéré au-delà du 30ème jour.
    # Le patient ne paie que le TM correspondant aux 30 1er jours du séjours.
    # Dans cette table on n'a pas le TM tronqué mais bien le TM total
    had_valo = had_valo.withColumn("TM_C",
                                   psf.when(psf.col("SEJ_NBJ") > 30, (psf.col("TM_C") * 30) / psf.col("SEJ_NBJ")).
                                   otherwise(psf.col("TM_C")))

    #  Identifier la dépense pour soins conformes au protocole ALD
    had_valo = (had_valo
                .withColumn("dep_lien_ald", psf.when(psf.col("EXO_TM").isin([2, 4]), 1).otherwise(0))
                .withColumn("rac_lien_ald", psf.when(psf.col("EXO_TM").isin([2, 4]), 1).otherwise(0)))

    # V2 : calcul du RAC par décomposition (PF, TM)
    # Participation forfaitaire effectivement facturée au cours du séjour
    had_valo = had_valo.withColumn("PF_sej",
                                   psf.when((psf.col("TAUX_C") == 100) & (psf.col("FAC_18E_C") != 18), 0).
                                   when((psf.col("TAUX_C") == 100) & (psf.col("FAC_18E_C") == 18),
                                        psf.col("FAC_18E_C")).
                                   when((psf.col("TAUX_C") != 100), 0).
                                   otherwise(None))
    # assert had_valo.select(["PF_sej"]).filter(psf.col("PF_sej").isNull()).count() == 0

    # Ticket modérateur effectivement facturé au cours du séjour
    had_valo = had_valo.withColumn("TM_sej",
                                   psf.when((psf.col("TAUX_C") == 100) & (psf.col("FAC_18E_C") != 18), 0).
                                   when((psf.col("TAUX_C") == 100) & (psf.col("FAC_18E_C") == 18), 0).
                                   when((psf.col("TAUX_C") != 100), psf.col("TM_C")).
                                   otherwise(None))

    # Nettoyage 
    # Cas des séjours avec TM > BR 
    # Dans ces cas-là : recalculer le TM comme (100-TAUX_C)*BR
    had_valo = had_valo.withColumn("TM_sej",
                                   psf.when((psf.col("TM_sej") >= psf.col("REM_BAS_MNT")) &
                                            (psf.col("REM_BAS_MNT") != 0),
                                            psf.col("REM_BAS_MNT") * (100 - psf.col("TAUX_C")) / 100).
                                   otherwise(psf.col("TM_sej")))
    # assert had_valo.select(["TM_sej"]).filter(psf.col("PF_sej").isNull()).count() == 0

    had_valo = had_valo.withColumn("RAC", psf.col("PF_sej") + psf.col("TM_sej"))

    had_valo = had_valo.withColumn("tot_dep", psf.col("TOT_MNT_AM") + psf.col("RAC"))

    # On recalcule également la dépense faite au titre de l'ALD
    had_valo = had_valo.withColumn("dep_lien_ald", psf.col("dep_lien_ald") * psf.col("tot_dep"))

    ir_orc_r = _load_ir_orc(path2_ir_orc, year, spark)

    had_valo = (had_valo.join(ir_orc_r, ["NUM_ENQ"], how="left")
                .withColumn("top_cmu", psf.when(psf.col("top_cmu") == 1, 1).otherwise(0))
                # Pour les CMU-cistes, le RAC AMO supp est à 0 (prise en charge TM + FJ par l'Etat)
                .withColumn("rac_amo_sup", psf.when(psf.col("top_cmu") == 1, 0).otherwise(psf.col("RAC")))
                # Dans ce cas, mettre également PF et TM à 0 pour que le RAC_AMO_SUP reste la somme de ces 3 composantes
                .withColumn("TM_sej", psf.when(psf.col("top_cmu") == 1, 0).otherwise(psf.col("TM_sej")))
                .withColumn("PF_sej", psf.when(psf.col("top_cmu") == 1, 0).otherwise(psf.col("PF_sej")))
                # Recalculer également le rac amo sup pour les dépenses faites au titre de l'ALD
                .withColumn("rac_lien_ald", psf.col("rac_lien_ald") * psf.col("rac_amo_sup")))

    # Ajouter variables pour compter nb de séjours de chaque type
    had_valo = had_valo.withColumn("NBSEJ_PARTL_COURT",
                                   psf.when(psf.col("TYP_SEJ") == "PARTL_COURT", 1).otherwise(0))

    had_valo = had_valo.withColumn("NBSEJ_PARTL_LONG",
                                   psf.when(psf.col("TYP_SEJ") == "PARTL_LONG", 1).otherwise(0))

    # PAR BENEFICIAIRE  -  Tout en conservant l'info sur le nombre de séjours par bénéficiaire
    rac_had = had_valo.groupBy(psf.col("NUM_ENQ")).agg(
        {"RAC": "sum", "tot_dep": "sum", "PF_sej": "sum", "TM_sej": "sum", "NUM_ENQ": "count",
         "TOT_MNT_AM": "sum", "HAD_DUREE": "sum", "NBSEJ_PARTL_COURT": "sum", "NBSEJ_PARTL_LONG": "sum",
         "dep_lien_ald": "sum", "rac_lien_ald": "sum", "rac_amo_sup": "sum"})

    output_var = ['RAC', 'dep_tot', 'MNT_PF', 'MNT_TM', "nb_sejour",
                  'remb_am', 'NBJ_PARTL', 'NBSEJ_PARTL_COURT', 'NBSEJ_PARTL_LONG',
                  "rac_lien_ald", "dep_lien_ald", "rac_amo_sup"]
    input_var = ['sum(RAC)', 'sum(tot_dep)', 'sum(PF_sej)', 'sum(TM_sej)', 'count(NUM_ENQ)',
                 'sum(TOT_MNT_AM)', 'sum(HAD_DUREE)', 'sum(NBSEJ_PARTL_COURT)', 'sum(NBSEJ_PARTL_LONG)',
                 "sum(rac_lien_ald)", "sum(dep_lien_ald)", "sum(rac_amo_sup)"]
    mapping = dict(zip(input_var, output_var))
    rac_had = rac_had.select([psf.col(c).alias(mapping.get(c, c)) for c in rac_had.columns])

    # Ajouter poste et poste agrege
    rac_had = (rac_had.withColumn("poste_ag", psf.lit("etab_public_had"))
               .withColumn("poste", psf.lit("sejour")))

    # Ajouter informations individuelles 
    rac_had = rac_had.join(had_valo.select(["NUM_ENQ", "BDI_DEP", "BDI_COD", "COD_SEX", "AGE_ANN"]).distinct(),
                           ["NUM_ENQ"], how="left")

    # Selectionner et réordonner colonnes pour qu'elles soient dans le même ordre
    rac_had = rac_had.select(list_output_var)

    return rac_had


def agg_med(path2flat, path2csv_had, path2_ucd, rac_had, year, list_output_var, spark):
    had_c = _load_had_c(path2flat, year, spark)
    if year == 2017:
        path_temp = path2csv_had + "MED.CSV"
    elif year == 2016:
        path_temp = path2csv_had + "med.csv"
    else:
        raise ValueError("Erreur année non prise en charge")
    had_med = (spark.read
               .option("header", "true")
               .option("sep", ";")
               .csv(path_temp)
               .dropDuplicates()
               .select(["ETA_NUM_EPMSI", "RHAD_NUM", "ACH_PRI_ADM", "UCD_UCD_COD", "ADM_NBR", "SEJ_NBR",
                        "ADM_ANN", "ADM_MOIS"])
               .withColumn("ACH_PRI_ADM", psf.col("ACH_PRI_ADM").cast(pst.DoubleType()))
               .withColumn("ADM_NBR", psf.col("ADM_NBR").cast(pst.DoubleType()))
               )
    if year == 2017:
        path_temp = path2csv_had + "MEDATU.CSV"
    elif year == 2016:
        path_temp = path2csv_had + "medatu.csv"
    else:
        raise ValueError("Erreur année non prise en charge")
    had_medatu = (spark.read
                  .option("header", "true")
                  .option("sep", ";")
                  .csv(path_temp)
                  .dropDuplicates()
                  .select(["ETA_NUM_EPMSI", "RHAD_NUM", "ACH_PRI_ADM", "UCD_UCD_COD", "ADM_NBR", "SEJ_NBR"])
                  .withColumn("ACH_PRI_ADM", psf.col("ACH_PRI_ADM").cast(pst.DoubleType()))
                  .withColumn("ADM_NBR", psf.col("ADM_NBR").cast(pst.DoubleType()))
                  )
    if year == 2017:
        path_temp = path2csv_had + "MEDCHL.CSV"
    elif year == 2016:
        path_temp = path2csv_had + "medchl.csv"
    else:
        raise ValueError("Erreur année non prise en charge")
    had_medchl = (spark.read
                  .option("header", "true")
                  .option("sep", ";")
                  .csv(path_temp)
                  .dropDuplicates()
                  .select(["ETA_NUM_EPMSI", "RHAD_NUM", "ACH_PRI_ADM", "UCD_UCD_COD", "ADM_NBR", "SEJ_NBR"])
                  .withColumn("ACH_PRI_ADM", psf.col("ACH_PRI_ADM").cast(pst.DoubleType()))
                  .withColumn("ADM_NBR", psf.col("ADM_NBR").cast(pst.DoubleType()))
                  )

    # Supprimer les lignes avec un nombre administré nul alors que le prix d'achat multiplié par le nombre administré
    # ne l'est pas (ATIH)
    had_med = had_med.filter((psf.col("ADM_NBR") != 0) & (psf.col("ACH_PRI_ADM") != 0))

    # Filtrer les dépenses qui ont eu lieu après l'année "annee" (aucun)
    had_med = had_med.filter(psf.col("ADM_ANN") <= year)

    # Suprimer lignes avec un nombre de séjours impliqués nul ou manquant
    had_med = had_med.filter((psf.col("SEJ_NBR") != 0) & (~psf.col("SEJ_NBR").isNull()))

    # Pas de code UCD nul
    # assert had_med.select(["UCD_UCD_COD"]).filter(psf.col("UCD_UCD_COD").isNull()).count() == 0
    had_med = had_med.withColumnRenamed("ADM_ANN", "DAT_ADM_ANN")
    had_med = _compare_ucd(had_med, path2_ucd, year, spark)

    # Supprimer les lignes avec un nombre administré nul alors que le prix d'achat multiplié par le nombre administré
    # ne l'est pas (ATIH)
    had_medatu = had_medatu.filter((psf.col("ADM_NBR") != 0) & (psf.col("ACH_PRI_ADM") != 0))

    # Filtrer les dépenses qui ont eu lieu après l'année "annee" (aucun)
    had_medatu = had_medatu.filter(psf.col("ADM_ANN") <= year)

    # Suprimer lignes avec un nombre de séjours impliqués nul ou manquant
    had_medatu = had_medatu.filter((psf.col("SEJ_NBR") != 0) & (~psf.col("SEJ_NBR").isNull()))

    # Pas de code UCD nul
    # assert had_medatu.select(["UCD_UCD_COD"]).filter(psf.col("UCD_UCD_COD").isNull()).count() == 0

    # Nettoyage table MEDCHL
    # Supprimer les lignes avec un nombre administré nul alors que le prix d'achat multiplié par le nombre administré
    # ne l'est pas (ATIH)
    had_medchl = had_medchl.filter((psf.col("ADM_NBR") != 0) & (psf.col("ACH_PRI_ADM") != 0))

    # Filtrer les dépenses qui ont eu lieu après l'année "annee" (aucun)
    had_medchl = had_medchl.filter(psf.col("ADM_ANN") <= year)

    # Suprimer lignes avec un nombre de séjours impliqués nul ou manquant
    had_medchl = had_medchl.filter((psf.col("SEJ_NBR") != 0) & (~psf.col("SEJ_NBR").isNull()))

    # Pas de code UCD nul
    # assert had_medchl.select(["UCD_UCD_COD"]).filter(psf.col("UCD_UCD_COD").isNull()).count() == 0

    had_stc = _load_had_stc(path2csv_had, year, spark)

    dict_dfs = dict({"had_med": had_med, "had_medatu": had_medatu, "had_medchl": had_medchl})
    for k, df in dict_dfs.items():
        df = df.withColumn("PRI_SEJ", psf.col("ACH_PRI_ADM") / psf.col("SEJ_NBR"))
        df = (df.groupBy([psf.col("ETA_NUM_EPMSI"), psf.col("RHAD_NUM")]).agg({"PRI_SEJ": "sum"})
              .withColumnRenamed("sum(PRI_SEJ)", "dep_tot"))
        df = (df.filter(psf.col("dep_tot") < 1000000)
              .join(had_stc.select(["ETA_NUM_EPMSI", "RHAD_NUM", "EXO_TM"]), ["ETA_NUM_EPMSI", "RHAD_NUM"], how="inner")
              .withColumn("dep_lien_ald", psf.when(psf.col("EXO_TM").isin([2, 4]), 1).otherwise(0))
              .withColumn("dep_lien_ald", psf.col("dep_lien_ald") * psf.col("dep_tot")))
        df = df.join(had_c.select(["ETA_NUM_EPMSI", "RHAD_NUM", "NUM_ENQ"]), ["ETA_NUM_EPMSI", "RHAD_NUM"],
                     how="left")
        df = (df.groupBy(psf.col("NUM_ENQ")).agg({"dep_tot": "sum", "dep_lien_ald": "sum", "NUM_ENQ": "count"})
              .withColumnRenamed("sum(dep_tot)", "dep_tot").withColumnRenamed("count(NUM_ENQ)", "nb_sejour")
              .withColumnRenamed("sum(dep_lien_ald)", "dep_lien_ald")
              .withColumn("poste_ag", psf.lit("etab_public_had"))
              .withColumn("rac", psf.lit(0.0))
              .withColumn("rac_lien_ald", psf.lit(0.0))
              .select(['NUM_ENQ', 'nb_sejour', 'dep_tot', 'poste_ag', 'rac', "rac_lien_ald", "dep_lien_ald"]))
        dict_dfs[k] = df

    had_med = dict_dfs["had_med"]
    had_medatu = dict_dfs["had_medatu"]
    had_medchl = dict_dfs["had_medchl"]

    # Ajouter colonnes poste, poste aggrege et rac
    had_med = had_med.withColumn("poste", psf.lit("Med_SUS"))
    had_medatu = had_medatu.withColumn("poste", psf.lit("Med_ATU"))
    had_medchl = had_medchl.withColumn("poste", psf.lit("Med_CHL"))

    had_public_sus = union_all(had_med, had_medatu, had_medchl)
    had_public_sus = had_public_sus.join(
        rac_had.select(["NUM_ENQ", "BDI_DEP", "BDI_COD", "COD_SEX", "AGE_ANN"]).distinct(), "NUM_ENQ", how="inner")

    # Ajouter colonne "remb_am"
    had_public_sus = (had_public_sus.withColumn("remb_am", psf.col("dep_tot"))
                      .withColumn("rac_amo_sup", psf.col("rac")))
    for c in ["MNT_TM", "MNT_PF", "MNT_FJ", "NBSEJ_PARTL_COURT", "NBSEJ_PARTL_LONG", "NBJ_PARTL"]:
        had_public_sus = had_public_sus.withColumn(c, psf.lit(None))

    # Selectionner et réordonner colonnes pour qu'elles soient dans le même ordre
    had_public_sus = had_public_sus.select(list_output_var)
    return had_public_sus

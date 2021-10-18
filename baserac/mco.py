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
from .utils import union_all
from .hospit import _compare_ucd
from .hospit import _load_ucd
from .hospit import _load_ir_orc


def _load_mco_c(path2flat, year, spark):
    mco_c = (spark.read
             .parquet(path2flat + "single_table/MCO_C/year=" + str(year))
             .select(["NUM_ENQ", "ETA_NUM", "RSA_NUM", "EXE_SOI_DTD", "EXE_SOI_DTF"])
             )
    return mco_c


def _load_ace_c(path2flat, year, spark):
    ace_c = (spark.read
             .parquet(path2flat + "single_table/MCO_CSTC/year=" + str(year))
             .select(["NUM_ENQ", "ETA_NUM", "SEQ_NUM", "EXE_SOI_DTD", "EXE_SOI_DTF"])
             .withColumnRenamed("SEQ_NUM", "RSA_NUM")
             )

    fbstc = (spark.read
             .parquet(path2flat + "single_table/MCO_FBSTC/year=" + str(year))
             .select(["ETA_NUM", "SEQ_NUM", "ACT_COD", "EXE_SPE", "EXO_TM"])
             .withColumnRenamed("SEQ_NUM", "RSA_NUM")
             .withColumn("ATU", psf.when(psf.col("ACT_COD") == "ATU", 1.0).otherwise(0.0))
             .groupBy(["ETA_NUM", "RSA_NUM"]).agg({"ATU": "max", "EXE_SPE": "max", "EXO_TM": "collect_set"})
             .withColumnRenamed("max(ATU)", "ATU")
             .withColumnRenamed("max(EXE_SPE)", "EXE_SPE_fbstc")
             .withColumnRenamed("collect_set(EXO_TM)", "EXO_TM")
             .withColumn("dep_lien_ald_fbstc", psf.when((psf.array_contains(psf.col("EXO_TM"), 2)) |
                                                        (psf.array_contains(psf.col("EXO_TM"), 4)), 1).otherwise(0))
             )

    # Ajout d'informations sur la spécialité du professionnel exécutant ("EXE_SPE")
    fcstc = (spark.read
             .parquet(path2flat + "single_table/MCO_FCSTC/year=" + str(year))
             .withColumnRenamed("SEQ_NUM", "RSA_NUM")
             .groupBy(["ETA_NUM", "RSA_NUM"]).agg({"EXE_SPE": "max", "EXO_TM": "collect_set"})
             .withColumnRenamed("max(EXE_SPE)", "EXE_SPE_fcstc")
             .withColumnRenamed("collect_set(EXO_TM)", "EXO_TM")
             .withColumnRenamed("collect_set(EXO_TM)", "EXO_TM")
             .withColumn("dep_lien_ald_fcstc", psf.when((psf.array_contains(psf.col("EXO_TM"), 2)) |
                                                        (psf.array_contains(psf.col("EXO_TM"), 4)), 1).otherwise(0))
             )
    fcstc = (fcstc.drop("EXO_TM").join(fbstc.drop("EXO_TM"), ["ETA_NUM", "RSA_NUM"], how="full")
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
             .groupBy('ETA_NUM', 'RSA_NUM', "ATU").agg({'EXE_SPE': "max", "dep_lien_ald": "max"})
             .withColumnRenamed("max(EXE_SPE)", "EXE_SPE").withColumnRenamed("max(dep_lien_ald)", "dep_lien_ald")
             )

    # Merge avec ace_c et creation d'une variable indiquant s'il s'agit d'un passage aux urgences
    ace_c = (ace_c.join(fcstc, ["ETA_NUM", "RSA_NUM"], how="left")
             .withColumn("ATU", psf.when(psf.col("ATU") == 1, psf.col("ATU")).otherwise(0.0))
             .withColumn("EXE_SPE",
                         psf.when(psf.col("EXE_SPE").isNull(), 0).
                         otherwise(psf.col("EXE_SPE")))
             .withColumnRenamed("EXE_SPE", "PSE_ACT_SPE"))
    return ace_c, fcstc


def _load_mco_stc(path2csv_mco, year, spark):
    if year == 2017:
        path_temp = path2csv_mco + "STC.CSV"
    elif year == 2016:
        path_temp = path2csv_mco + "stc.csv"
    else:
        raise ValueError("Année non prise en charge")

    mco_stc = (spark.read
               .option("header", "true")
               .option("sep", ";")
               .csv(path_temp)
               .select(["ETA_NUM", "RSA_NUM", "EXO_TM", "FJ_COD_PEC", "FAC_MNT_TM", "FAC_MNT_FJ", "REM_TAU", "NAT_ASS",
                        "FAC_18E", "BEN_CMU", "FAC_SEJ_AM", "TOT_MNT_AM", "TOT_MNT_AMC", "REM_BAS_MNT", "MAJ_MNT_PS",
                        "FAC_NBR_VEN"])
               )
    return mco_stc


def agg_mco(path2flat, path2csv_mco, path2_ir_orc, year, list_output_var, spark, filter_etab, coalesce_coef=5):
    if year not in [2016, 2017]:
        raise ValueError("Année non prise en charge")
    # On récupère l'identifiant PMSI (eta_num*rsa_num)
    mco_c = _load_mco_c(path2flat, year, spark)

    # chargement de la table de valorisation des séjours MCO public
    if year == 2017:
        path_temp = path2csv_mco + "VALO.CSV"
    elif year == 2016:
        path_temp = path2csv_mco + "valo.csv"
    else:
        print("Année non prise en charge.")
        raise ValueError
    mco_valo = (spark.read.option("header", "true")
                .option("sep", ";")
                .csv(path_temp)
                .select(["ETA_NUM", "RSA_NUM", "VALO", "MNT_18", "MNT_FJ2",
                         "MNT_GHS_AM", "MNT_TOT_AM", "TAUX2", "MNT_RAC"])
                )

    # Table de description du sejour
    mco_b = (spark.read.parquet(path2flat + "single_table/MCO_B/year=" + str(year))
             .select(["ETA_NUM", "RSA_NUM", "ENT_MOD", "SOR_MOD", "GRG_GHM",
                      "NBR_SEA", "TYP_GEN_RSA", "SEJ_TYP", "BDI_DEP", "BDI_COD", "COD_SEX", "AGE_ANN"])
             )

    # On fusionne les tables valo, c et b
    mco = mco_valo.join(mco_c, ["ETA_NUM", "RSA_NUM"], how="left")
    mco = mco.join(mco_b, ["ETA_NUM", "RSA_NUM"], how="left")

    # Nettoyage des données
    # Suppression des séjours non valorisés
    mco = mco.filter(psf.col("VALO").isin([1, 2, 3, 4, 5]))

    # Table de facturation
    mco_stc = _load_mco_stc(path2csv_mco, year, spark)
    mco = mco.join(mco_stc, ["ETA_NUM", "RSA_NUM"], how="left")

    # Supprimer les sejours pour lesquels le montant facture a l'AM est nul
    mco = mco.filter(psf.col("MNT_TOT_AM") != 0)

    # Suppression des FINESS en doublon (juridique et géographique)
    # Filtre sur les etablissements
    # Exclure les informations remontees par FINESS geogaphique pour APHP, HCL et APHM (doublons)
    # car ces informations remontent aussi par FINESS juridique
    mco = mco.filter(~psf.col("ETA_NUM").isin(filter_etab))

    # Exclusion des prestations inter-établissement
    mco = mco.filter((psf.col("ENT_MOD") != 0) & (psf.col("SOR_MOD") != 0))

    # verif GHM en erreur
    # print("GHM en erreur restants (lignes): {}"
    # .format(mco.filter(psf.substring(psf.col("GRG_GHM"), 1, 2) != 90).count()))
    # assert mco.filter(psf.substring(psf.col("GRG_GHM"), 1, 2) != 90).count() == 0

    # verif des prestations pour lesquelles un résumé de séjour n'a pas été généré
    # print("Prestations pour lesquelles un résumé de séjour n'a pas été généré restants (lignes): {}"
    # .format( mco.filter(psf.col("TYP_GEN_RSA") == 0).count()))
    # assert mco.filter(psf.col("TYP_GEN_RSA") == 0).count() == 0

    # Suppression des sejours commencés apres l'année
    mco = mco.filter(psf.col("EXE_SOI_DTD") <= str(year) + "-12-31")
    # Suppression des sejours sans date de fin ou terminé après l'année d'étude
    mco = mco.filter(psf.col("EXE_SOI_DTF").isNotNull()).filter(psf.col("EXE_SOI_DTF") <= str(year) + "-12-31")

    # Création de la variable de duree de séjour
    mco = mco.withColumn("duree_sejour", psf.datediff(psf.col("EXE_SOI_DTF"), psf.col("EXE_SOI_DTD")))

    # Créer la variable nombre de jours de présence en temps complet
    # En MCO, la distinction se fait sur les séjours avec / sans nuitée
    mco = mco.withColumn("SEJ_NBJ_CPLT",
                         psf.when(psf.col("duree_sejour") == 0, psf.lit(0)).
                         otherwise(psf.col("duree_sejour") + 1))

    # Créer la variable type de séjours avec 3 modalités : séjour en HP, séjour en HC court (moins de 30 jours),
    # séjour en HC long (30 jours ou plus)
    mco = mco.withColumn("TYP_SEJ",
                         psf.when(psf.col("duree_sejour") == 0, psf.lit("PARTL")).
                         when((psf.col("duree_sejour") > 0) & (psf.col("duree_sejour") < 30), psf.lit("CPLT_COURT")).
                         when(psf.col("duree_sejour") >= 30, psf.lit("CPLT_LONG")))

    for c in ["TAUX2", "REM_TAU", "TOT_MNT_AM", "MNT_18", "MNT_FJ2", "MNT_GHS_AM", "MNT_TOT_AM",
              "MNT_RAC", "FAC_MNT_TM"]:
        mco = mco.withColumn(c, mco[c].cast(pst.DoubleType()))

    # Séjours non valorisés avec prélèvement d'organe
    # Normalement : taux de remboursement de 0 (100% de TAUX2 == 0)
    # on corrige quand manquant ou pas egal à 0
    for c in ["TAUX2", "REM_TAU"]:
        mco = mco.withColumn(c,
                             psf.when(psf.col("VALO").isin([2]), 0.0).otherwise(psf.col(c)))

    # Recalcul des taux de remboursement
    mco = mco.withColumn("REM_TAU", psf.when(psf.col("REM_TAU") > 100, 100).otherwise(psf.col("REM_TAU")))

    # Recalculer les taux de remboursement avec la methode de l'ATIH et comparer
    # Cas général
    mco = mco.withColumn("taux_atih",
                         psf.when(psf.col("EXO_TM").isin(["X"]) | psf.col("EXO_TM").isNull() |
                                  psf.col("NAT_ASS").isin(["XX"]), None).
                         when(psf.col("EXO_TM").isin(["0", "2"]) & psf.col("NAT_ASS").isin(["10", "13"]), 80).
                         when(psf.col("EXO_TM").isin(["0", "2"]), 100).
                         when(psf.col("EXO_TM").isin(["9"]) & psf.col("NAT_ASS").isin(["10"]), 90).
                         otherwise(100))

    # Cas particulier avec FJ non applicable (séances, durée de séjour = 0 hors radiothérapie, ou GHM 23K02Z) :
    mco = mco.withColumn("taux_atih",
                         psf.when((psf.substring(psf.col("GRG_GHM"), 1, 2) != 28) & (psf.col("duree_sejour") != 0) &
                                  (psf.col("GRG_GHM") != "23K02Z"), psf.col("taux_atih")).
                         when(psf.col("EXO_TM").isin(["X"]) | psf.col("EXO_TM").isNull() | psf.col("NAT_ASS").isin(
                             ["XX"]), None).
                         when(psf.col("EXO_TM").isin(["0", "2"]) & psf.col("NAT_ASS").isin(["10"]), 80).
                         when(psf.col("EXO_TM").isin(["0", "2"]), 100).
                         when(psf.col("EXO_TM").isin(["9"]) & psf.col("NAT_ASS").isin(["10"]), 90).
                         otherwise(100))

    # Exceptions
    # Taux de remboursement de 100% et exonération de TM pour les séjours de radiothérapie
    # Faire pareil avec les séjours de nouveaux-nés avec âge > 30 jours et pour les séjours avec forfaits innovation
    # (I01, I02, I03, I04 et I05)
    mco = mco.withColumn("taux_atih", psf.when(psf.substring(psf.col("GRG_GHM"), 1, 2).isin(
        ["28Z11Z", "28Z18Z", "28Z19Z", "28Z20Z", "28Z21Z", "28Z22Z",
         "28Z23Z", "28Z24Z", "28Z25Z"]), 100).otherwise(psf.col("taux_atih")))
    mco = mco.withColumn("FJ_COD_PEC", psf.when(psf.substring(psf.col("GRG_GHM"), 1, 2).isin(
        ["28Z11Z", "28Z18Z", "28Z19Z", "28Z20Z", "28Z21Z", "28Z22Z",
         "28Z23Z", "28Z24Z", "28Z25Z"]), "R").otherwise(psf.col("FJ_COD_PEC")))

    # Taux de remboursement de 0 % pour les séjours non valorisés avec prélèvement d'organe
    mco = mco.withColumn("taux_atih", psf.when(psf.col("VALO").isin([2]), 0.0).otherwise(psf.col("taux_atih")))

    # Creer variable corrigee TAUX_C
    mco = mco.withColumn("TAUX_C", psf.when(psf.col("VALO").isin([3, 4, 5]), psf.col("REM_TAU")).
                         otherwise(psf.col("TAUX2")))

    # Cas particulier des détenus, AME, SU
    mco = mco.withColumn("TAUX_C",
                         psf.when((psf.col("VALO").isin([3, 4, 5])) & (psf.col("taux_atih").isNotNull()) &
                                  ((psf.col("TAUX_C").isNull()) | (psf.col("TAUX_C") < 80)), psf.col("taux_atih")).
                         when((psf.col("VALO").isin([3, 4, 5])) & (psf.col("taux_atih").isNull()) &
                              ((psf.col("TAUX_C").isNull()) | (psf.col("TAUX_C") < 80)), psf.lit(100)).
                         otherwise(psf.col("TAUX_C")))

    #  Cas général
    # 1) Commencer par remplacer TAUX_C par tx_ATIH quand tx_ATIH et REM_TAU concordent
    mco = mco.withColumn("TAUX_C", psf.when((psf.col("TAUX_C").isNull()) & (psf.col("taux_atih").isNotNull()) &
                                            (psf.col("REM_TAU").isNotNull()) & (
                                                    psf.col("REM_TAU") == psf.col("taux_atih")),
                                            psf.col("taux_atih")).
                         otherwise(psf.col("TAUX_C")))

    #  2) Si REM_TAU is NA ou nul on remplace TAUX_C par tx_ATIH
    mco = mco.withColumn("TAUX_C", psf.when((psf.col("TAUX_C").isNull()) & (psf.col("REM_TAU") == 0),
                                            psf.col("taux_atih")).otherwise(psf.col("TAUX_C")))

    # 3) Dans les cas ou REM_TAU est nul et tx_ATIH est manquant on supprime
    mco = mco.filter((psf.col("TAUX_C").isNotNull()) | (psf.col("taux_atih").isNotNull()) | (psf.col("REM_TAU") != 0))

    # 4) Garder systematiquement tx_ATIH quand REM_TAU et tx_ATIH different
    mco = mco.withColumn("TAUX_C", psf.when((psf.col("TAUX_C").isNull()), psf.col("taux_atih")).
                         otherwise(psf.col("TAUX_C")))

    # 5) Supprimer le reste (quand taux_c est null)
    mco = mco.filter(psf.col("TAUX_C").isNotNull())
    # assert mco.select("TAUX_C").filter(psf.col("TAUX_C").isNull()).count() == 0

    # 6) Corriger le taux de remboursement en cas de PF18 --> celui-ci doit etre systematiquement a 100%
    mco = mco.withColumn("TAUX_C", psf.when((psf.col("FAC_18E") == 1), 100.00).otherwise(psf.col("TAUX_C")))

    # Verifier que les seuls TAUX_C égaux à 0 sont pour des PO
    # assert mco.filter((psf.col("TAUX_C") == 0) & (~psf.col("VALO") == 2)).count() == 0

    # Reperage et nettoyage des valeurs aberrantes : montants total am
    # MNT_TOT_AM negatif : Que des sejours extremement couteux avec des durees >= 30 jours et beaucoup d'ALD,
    # on les supprime
    mco = mco.filter(psf.col("MNT_TOT_AM") >= 0)

    # Gestion des valeurs extrêmes de MNT_TOT_AM
    # En 2016, un MNT_TOT_AM superieur à 1 million et après examen,
    # c'est un decalage de virgule on le corrige
    if year == 2016:
        mco = mco.withColumn("MNT_TOT_AM",
                             psf.when((psf.col("MNT_TOT_AM") > 1000000), psf.col("MNT_TOT_AM") / 10).
                             otherwise(psf.col("MNT_TOT_AM")))

    # Montant participation forfaitaire de 18 euros
    # Privilegier FAC_18E et recalculer le montant reel 18E qui s'applique en cas
    # de PF18 et de non exoneration de TM (sauf avec un code C)
    mco = mco.withColumn("PF18_C", psf.when(psf.col("FAC_18E").isNull(), 0.0).otherwise(psf.col("FAC_18E") * 18))

    # Forfait journalier
    # Recalcul du FJ a partir de la duree de sejour et du mode de sortie (SOR_MOD)
    # Pas de FJ le jour de sortie si mutation (6), transfert (7) ou deces (9)
    mco = mco.withColumn("FJ_NC", psf.when(psf.col("duree_sejour") == 0, 0.0).
                         when((psf.col("duree_sejour") > 0) & (~psf.col("SOR_MOD").isin(["6", "7", "9"])),
                              (psf.col("duree_sejour") + 1) * 18).
                         when((psf.col("duree_sejour") > 0) & (psf.col("SOR_MOD").isin(["6", "7", "9"])),
                              (psf.col("duree_sejour")) * 18))

    # REGLE DE DECISION : creation de la variable FJ_C
    # On garde MNT_FJ2 quand egal a FAC_MNT_FJ, on le fixe a 0 quand FJ_COD_PEC="R"
    # En cas de séance, le FJ prend le montant de 0 (MNT_FJ2 toujours = 0 pour séances)
    mco = mco.withColumn("FJ_C",
                         psf.when((psf.col("FJ_COD_PEC") == "R"), psf.lit(0.0)).
                         when((psf.col("duree_sejour") == 0), psf.lit(0.0)).
                         when((psf.substring(psf.col("GRG_GHM"), 1, 2) == 28), psf.lit(0)).
                         when((psf.col("MNT_FJ2").isNotNull()) & (psf.col("FAC_MNT_FJ").isNotNull()) &
                              (psf.col("MNT_FJ2") == psf.col("FAC_MNT_FJ")), psf.col("MNT_FJ2")).
                         otherwise(psf.lit(None)))
    mco = mco.withColumn("FJ_C",
                         psf.when((psf.col("FJ_C").isNotNull()), psf.col("FJ_C")).
                         when((psf.col("MNT_FJ2").isNotNull()) & (psf.col("MNT_FJ2") > 0), psf.col("MNT_FJ2")).
                         otherwise(psf.lit(None)))
    mco = mco.withColumn("FJ_C",
                         psf.when((psf.col("FJ_C").isNotNull()), psf.col("FJ_C")).
                         when((psf.col("FAC_MNT_FJ") == psf.col("FJ_NC")), psf.col("FJ_NC")).
                         otherwise(psf.lit(None)))
    mco = mco.withColumn("diff", psf.col("FJ_NC") - psf.col("FAC_MNT_FJ"))
    mco = mco.withColumn("FJ_C",
                         psf.when((psf.col("FJ_C").isNotNull()), psf.col("FJ_C")).
                         when((psf.col("diff") > 0) & (psf.col("diff") <= 90), psf.col("FAC_MNT_FJ")).
                         otherwise(psf.col("FJ_NC")))

    mco = mco.withColumn("FJ_C", psf.col("FJ_C").cast(pst.DoubleType()))

    # Ajouter variable FJ_C2 qui prenne la valeur :
    # 0 si sejour ambulatoire ou exoneration de FJ
    # FJ total si taux de remboursement de 100%
    # FJ jour de sortie (ou 0) si pas d'exoneration de TM (et pas de PF18) - en fonction du mode de sortie
    # FJ au-dela du 30 eme jour en cas de sejour long sans exoneration de TM - en fonction du mode de sortie
    # le tout etant module par le mode de transfert
    mco = mco.withColumn("FJ_C2",
                         psf.when((psf.col("duree_sejour") == 0), psf.lit(0)).
                         when((psf.col("FJ_COD_PEC") == "R"), psf.lit(0)).
                         when((psf.substring(psf.col("GRG_GHM"), 1, 2) == 28), psf.lit(0)).
                         when(psf.col("TAUX_C") == 100, psf.col("FJ_C")).
                         when((psf.col("TAUX_C") < 100) & (psf.col("duree_sejour") > 0) & (
                                 psf.col("duree_sejour") <= 30) &
                              (~psf.col("SOR_MOD").isin(["0", "6", "7", "9"])), psf.lit(18.0)).
                         when((psf.col("TAUX_C") < 100) & (psf.col("duree_sejour") > 0) & (
                                 psf.col("duree_sejour") <= 30) &
                              (psf.col("SOR_MOD").isin(["0", "6", "7", "9"])), psf.lit(0.0)).
                         when((psf.col("TAUX_C") < 100) & (psf.col("duree_sejour") > 30) & (~
                                                                                            psf.col("SOR_MOD").isin(
                                                                                                ["6", "7", "9"])),
                              (psf.col("duree_sejour") - 29) * 18).
                         when((psf.col("TAUX_C") < 100) & (psf.col("duree_sejour") > 30) & (
                             psf.col("SOR_MOD").isin(["6", "7", "9"])),
                              (psf.col("duree_sejour") - 30) * 18).
                         when((psf.col("FAC_MNT_FJ") == psf.col("FJ_NC")), psf.col("FJ_NC")).
                         otherwise(psf.lit(None)))

    # Netoyage du montant du ticket modérateur
    # Remplacer les NA par des 0
    mco = (mco.withColumn("TM_C",
                          psf.when(psf.col("FAC_MNT_TM").isNull(), 0.0).
                          when(psf.col("FAC_MNT_TM") < 0, 0.0).
                          otherwise(psf.col("FAC_MNT_TM")))
           .withColumn("TM_C", psf.col("TM_C").cast(pst.DoubleType())))

    # assert mco.select("TM_C").filter(psf.col("TM_C").isNull()).count() == 0

    # Code d'exonération du FJ
    # mco.select("FJ_COD_PEC").filter(psf.col("FJ_COD_PEC").isNull()).count() 
    # Du fait de ces valeurs nulles, pour le calcul du RAC, on distinguera les cas où FJ_COD_PEC == R et le reste
    # (i.e. FJ_COD_PEC != R ou nul)

    # Gérer le cas des séjours de plus de 30 jours
    # mco.filter(psf.col("duree_sejour")>30).count()

    # Normalement le TM est exonéré au-delà du 30ème jour.
    # Le patient ne paie que le TM correspondant aux 30 1er jours du séjours.
    mco = mco.withColumn("TM_C",
                         psf.when(psf.col("duree_sejour") > 30, (psf.col("TM_C") * 30) / psf.col("duree_sejour")).
                         otherwise(psf.col("TM_C")))

    # Cas particulier des séances
    # Dans les cas des séances (acte unique, à la journée), si on a acte lourd on devrait avoir PF18
    # On décide dans ce cas d'attibuer systématiquement un taux de remboursement de 100%, un TM_C de 0 et une PF18
    # _C de 18€
    # En cas de séance qui par définition est censée être un acte unique ayant lieu sur une même journée
    # pour laquelle on avait au préalable un TM > 24 (=0.2*120 qui est la limite du prix d'un acte lourd)
    for c in ["TM_C", "PF18_C", "TAUX_C"]:
        mco = mco.withColumn(c, psf.when((psf.substring(psf.col("GRG_GHM"), 1, 2) == 28) & (psf.col("TM_C") > 24), 0.0).
                             otherwise(psf.col(c)))

    # Identifier la dépense pour soins conformes au protocole ALD
    mco = mco.withColumn("dep_lien_ald", psf.when(psf.col("EXO_TM").isin([2, 4]), psf.lit(1)).otherwise(psf.lit(0)))
    mco = mco.withColumn("rac_lien_ald", psf.when(psf.col("EXO_TM").isin([2, 4]), psf.lit(1)).otherwise(psf.lit(0)))

    # Calcul du RAC par décomposition (PF, FJ, TM)
    # - Commencer par calculer les montants : PF_sej, TM_sej et FJ_sej, effectivement facturés au cours du séjour
    # - Les sommer pour calculer le RAC
    mco = mco.withColumn("PF_sej",
                         # psf.when(psf.col("VALO").isin([3, 4, 5]), psf.lit(0.0)).
                         psf.when((psf.col("TAUX_C") != 100), psf.lit(0.0)).
                         when((psf.col("TAUX_C") == 100), psf.col("PF18_C")).
                         otherwise(None))

    # assert mco.select(["PF_sej"]).filter(psf.col("PF_sej").isNull()).count() == 0

    # Montant du TM effectivement facturé au cours du séjour
    mco = mco.withColumn("TM_sej",  # psf.when(psf.col("VALO").isin([3, 4, 5]), 0.0).
                         psf.when((psf.col("TAUX_C") == 100), 0.0).
                         when((psf.col("TAUX_C") != 100) & (psf.col("FJ_COD_PEC") == "R"), psf.col("TM_C")).
                         when((psf.col("TAUX_C") != 100) & (psf.col("TM_C") > psf.col("FJ_C")), psf.col("TM_C")).
                         when((psf.col("TAUX_C") != 100) & (psf.col("TM_C") <= psf.col("FJ_C")), 0.0).
                         otherwise(None))

    # Cas des séjours avec TM > BR  : recalculer le TM comme (100-TAUX_C)*BR
    mco = mco.withColumn("TM_sej",
                         psf.when((psf.col("TM_sej") >= psf.col("REM_BAS_MNT")) & (psf.col("REM_BAS_MNT") != 0),
                                  psf.col("REM_BAS_MNT") * (100 - psf.col("TAUX_C")) / 100).
                         otherwise(psf.col("TM_sej")))

    # Montant du FJ effectivement facturé au cours du séjour
    mco = mco.withColumn("FJ_sej", 
                         psf.when((psf.col("TAUX_C") == 100), psf.col("FJ_C2")).
                         when((psf.col("TAUX_C") != 100) & (psf.col("FJ_COD_PEC") == "R"), 0.0).
                         when((psf.col("TAUX_C") != 100) & (psf.col("TM_C") > psf.col("FJ_C")), psf.col("FJ_C2")).
                         when((psf.col("TAUX_C") != 100) & (psf.col("TM_C") <= psf.col("FJ_C")), psf.col("FJ_C")).
                         otherwise(None))

    # Calcul du RAC par séjour
    # Le RAC est la somme de ces trois termes
    mco = mco.withColumn("RAC", psf.col("PF_sej") + psf.col("TM_sej") + psf.col("FJ_sej"))

    # Nettoyage des RAC extremes 
    mco = (mco.withColumnRenamed("MNT_RAC", "rac_rempli_etab").withColumnRenamed("PF_sej", "MNT_PF")
           .withColumnRenamed("TM_sej", "MNT_TM").withColumnRenamed("FJ_sej", "MNT_FJ")
           .withColumnRenamed("MNT_TOT_AM", "remb_am")
           .withColumn("rac_C",
                       psf.when((psf.col("rac") > 10000) & (psf.col("rac") - psf.col("rac_rempli_etab") > 5000),
                                psf.col("rac_rempli_etab")).
                       otherwise(psf.col("rac")))
           .withColumn("MNT_TM",
                       psf.when((psf.col("rac") > 10000) & (psf.col("rac") - psf.col("rac_rempli_etab") > 5000),
                                (psf.col("rac_rempli_etab") - psf.col("MNT_FJ") - psf.col("MNT_PF"))).
                       otherwise(psf.col("MNT_TM")))
           )

    # Supprimer variables "_C"
    mco = mco.drop("rac").withColumnRenamed("rac_C", "rac")

    ir_orc_r = _load_ir_orc(path2_ir_orc, year, spark)
    mco = mco.join(ir_orc_r, ["NUM_ENQ"], how="left")
    mco = mco.withColumn("top_cmu",
                         psf.when(psf.col("top_cmu") == 1, psf.lit(1)).
                         otherwise(psf.lit(0)))
    # Pour les détenus, AME, SU et CMU-cistes, le RAC AMO supp est à 0 (prise en charge TM + FJ par l'Etat)
    mco = mco.withColumn("rac_amo_sup",
                         psf.when(psf.col("VALO").isin([3, 4, 5]), psf.lit(0)).
                         when(psf.col("top_cmu") == 1, psf.lit(0)).
                         otherwise(psf.col("rac")))

    # Dans ce cas-là, mettre également PF, FJ et TM à 0 pour que le RAC_AMO_SUP reste la somme de ces 3 composantes 
    mco = mco.withColumn("MNT_TM",
                         psf.when(psf.col("VALO").isin([3, 4, 5]), psf.lit(0)).
                         when(psf.col("top_cmu") == 1, psf.lit(0)).
                         otherwise(psf.col("MNT_TM")))
    mco = mco.withColumn("MNT_PF",
                         psf.when(psf.col("VALO").isin([3, 4, 5]), psf.lit(0)).
                         when(psf.col("top_cmu") == 1, psf.lit(0)).
                         otherwise(psf.col("MNT_PF")))
    mco = mco.withColumn("MNT_FJ",
                         psf.when(psf.col("VALO").isin([3, 4, 5]), psf.lit(0)).
                         when(psf.col("top_cmu") == 1, psf.lit(0)).
                         otherwise(psf.col("MNT_FJ")))
    # Attention à ne pas faire apparaître des rac > rac_amo_sup
    mco = mco.withColumn("rac_amo_sup",
                         psf.when(psf.col("rac") < psf.col("rac_amo_sup"), psf.col("rac")).
                         otherwise(psf.col("rac_amo_sup")))

    # Ajout de la dépense totale, ainsi que de la dépense et du RAC au titre de l'ALD
    # On calcule la dépense totale (remboursement de l'AM + RAC)
    mco = mco.withColumn("dep_tot", psf.col("rac") + psf.col("remb_am"))

    # On recalcule la dépense faite au titre de l'ALD
    mco = mco.withColumn("dep_lien_ald", psf.col("dep_lien_ald") * psf.col("dep_tot"))
    # On recalcule le rac amo sup pour dépenses faites au titre de l'ALD
    mco = mco.withColumn("rac_lien_ald", psf.col("rac_lien_ald") * psf.col("rac_amo_sup"))

    # Calcul du RAC par bénéficiaire
    # Ajouter variables pour compter nb de séjours de chaque type

    mco = (mco.withColumn("NBSEJ_PARTL", psf.when(psf.col("TYP_SEJ") == "PARTL", 1).otherwise(0))
           .withColumn("NBSEJ_CPLT_COURT", psf.when(psf.col("TYP_SEJ") == "CPLT_COURT", 1).otherwise(0))
           .withColumn("NBSEJ_CPLT_LONG", psf.when(psf.col("TYP_SEJ") == "CPLT_LONG", 1).otherwise(0))
           )

    # Regrouper par beneficiaire (plusieurs sejours / beneficiaire) en conservant l'info sur le nombre de séjours 
    # par bénéficiaire à des fins de comparaison on conserve la variable mnt_rac renseignée par l'établissement 
    # (mais l'ATIH déconseille de l'utiliser)
    # la bonne variable de reste à charge est la variable calculée (rac)

    rac_mco = mco.groupBy(psf.col("NUM_ENQ")).agg(
        {"rac": "sum", "rac_amo_sup": "sum", "rac_rempli_etab": "sum", "remb_am": "sum", "MNT_TM": "sum",
         "MNT_PF": "sum", "MNT_FJ": "sum", "NUM_ENQ": "count", "SEJ_NBJ_CPLT": "sum",
         "NBSEJ_PARTL": "sum", "NBSEJ_CPLT_COURT": "sum", "NBSEJ_CPLT_LONG": "sum", "dep_tot": "sum",
         "dep_lien_ald": "sum", "rac_lien_ald": "sum"})

    rac_mco = (rac_mco.withColumnRenamed("sum(rac)", "rac").withColumnRenamed("sum(rac_rempli_etab)", "rac_rempli_etab")
               .withColumnRenamed("sum(rac_amo_sup)", "rac_amo_sup").withColumnRenamed("sum(MNT_TM)", "MNT_TM")
               .withColumnRenamed("sum(remb_am)", "remb_am").withColumnRenamed("sum(MNT_PF)", "MNT_PF")
               .withColumnRenamed("sum(MNT_PF)", "MNT_PF").withColumnRenamed("sum(MNT_FJ)", "MNT_FJ")
               .withColumnRenamed("count(NUM_ENQ)", "nb_sejour").withColumnRenamed("sum(SEJ_NBJ_CPLT)", "NBJ_CPLT")
               .withColumnRenamed("sum(NBSEJ_PARTL)", "NBSEJ_PARTL").withColumnRenamed("sum(NBSEJ_CPLT_COURT)",
                                                                                       "NBSEJ_CPLT_COURT")
               .withColumnRenamed("sum(NBSEJ_CPLT_LONG)", "NBSEJ_CPLT_LONG").withColumnRenamed("sum(dep_tot)",
                                                                                               "dep_tot")
               .withColumnRenamed("sum(dep_lien_ald)", "dep_lien_ald").withColumnRenamed("sum(rac_lien_ald)",
                                                                                         "rac_lien_ald"))

    # Ajouter le nombre de jours en séjours HP (= le nombre de séjours en ambulatoire)
    rac_mco = rac_mco.withColumn("NBJ_PARTL", psf.col("NBSEJ_PARTL"))

    # Outliers : TM
    # Dans ces cas-là, le problème vient du TM
    # De manière générale, en cas de MNT_TM > 10 000 € et si plus de 5 000 # de différence entre rac et rac_rempli_etab 
    # Remplacer rac par rac_rempli_etab et MNT_TM par rac - MNT_FJ - MNT_PF
    rac_mco = rac_mco.withColumn("rac_C",
                                 psf.when(
                                     (psf.col("rac") > 10000) & (psf.col("rac") - psf.col("rac_rempli_etab") > 5000),
                                     psf.col("rac_rempli_etab")).
                                 otherwise(psf.col("rac")))
    rac_mco = rac_mco.withColumn("MNT_TM",
                                 psf.when(
                                     (psf.col("rac") > 10000) & (psf.col("rac") - psf.col("rac_rempli_etab") > 5000),
                                     (psf.col("rac_rempli_etab") - psf.col("MNT_FJ") - psf.col("MNT_PF"))).
                                 otherwise(psf.col("MNT_TM")))

    rac_mco = rac_mco.withColumn("dep_tot_C",
                                 psf.when(psf.col("rac_C") != psf.col("rac"), psf.col("remb_am") + psf.col("rac_C")).
                                 otherwise(psf.col("dep_tot")))
    rac_mco = rac_mco.withColumn("dep_lien_ald",
                                 psf.when((psf.col("dep_tot") == 0) | (psf.col("dep_tot_C") == 0), psf.lit(0)).
                                 when(psf.col("dep_tot_C") != psf.col("dep_tot"),
                                      (psf.col("dep_lien_ald") * psf.col("dep_tot_C")) / (psf.col("dep_tot"))).
                                 otherwise(psf.col("dep_lien_ald")))
    # Supprimer les variables "_C"
    rac_mco = rac_mco.drop("rac", "dep_tot")
    rac_mco = rac_mco.withColumnRenamed("rac_C", "rac").withColumnRenamed("dep_tot_C", "dep_tot")

    # Avant de corriger le rac_amo_sup, garder en memoire la relation entre le rac_lien_ald et le rac_amo_sup
    rac_mco = rac_mco.withColumn("rac_lien_ald",
                                 psf.when(psf.col("rac_lien_ald") == 0, psf.lit(0)).
                                 when((psf.col("rac_amo_sup") == 0) | (psf.col("rac") == 0), psf.lit(0)).
                                 when(psf.col("rac") < psf.col("rac_amo_sup"),
                                      (psf.col("rac_lien_ald") * psf.col("rac")) / (psf.col("rac_amo_sup"))).
                                 otherwise(psf.col("rac_amo_sup")))

    # Puis corriger rac_amo_sup
    rac_mco = rac_mco.withColumn("rac_amo_sup",
                                 psf.when(psf.col("rac") < psf.col("rac_amo_sup"), psf.col("rac")).
                                 otherwise(psf.col("rac_amo_sup")))
    # Ajouter poste et poste aggrege
    rac_mco = rac_mco.withColumn("poste_ag", psf.lit("etab_public_mco"))
    rac_mco = rac_mco.withColumn("poste", psf.lit("sejour"))

    # Ajouter informations individuelles 
    rac_mco = rac_mco.join(mco.select("NUM_ENQ", "BDI_DEP", "BDI_COD", "COD_SEX", "AGE_ANN").distinct(), ["NUM_ENQ"],
                           how="right")

    # Selectionner et réordonner colonnes pour qu'elles soient dans le même ordre
    rac_mco = rac_mco.select(list_output_var)
    return rac_mco


def agg_med(path2flat, path2csv_mco, path2_ucd, rac_mco, year, list_output_var, spark):
    """
    Médicaments et dispositifs implantables de la liste en SUS (public)

    Tables : MED (médicaments en SUS), MEDATU (médicaments soumis à autorisation temporaire d'utilisation),
    MEDTHROMBO (médicaments thrombolytiques pour le traitement des AVC ischémiques) et DMIP (DMI en sus)

    :param path2flat:
    :param path2csv_mco:
    :param path2_ucd:
    :param rac_mco:
    :param year:
    :param list_output_var:
    :param spark:
    :return:
    """

    mco_c = _load_mco_c(path2flat, year, spark).cache()
    ucd = _load_ucd(path2_ucd, spark).cache()
    if year == 2017:
        path_temp = path2csv_mco + "MED.CSV"
    elif year == 2016:
        path_temp = path2csv_mco + "med.csv"
    else:
        raise ValueError("Erreur année non prise en charge")

    # MED (médicaments en SUS)
    mco_med = (spark.read
               .option("header", "true")
               .option("sep", ";")
               .csv(path_temp)
               .dropDuplicates()
               .select(['ETA_NUM', 'RSA_NUM', 'ADM_MOIS', 'ADM_NBR', 'ADM_NBR_PRI', 'DAT_ADM_ANN', 'NBR_SEJ',
                        'UCD_UCD_COD'])
               .withColumn("ADM_NBR_PRI", psf.col("ADM_NBR_PRI").cast(pst.DoubleType()))
               .withColumn("ADM_NBR", psf.col("ADM_NBR").cast(pst.DoubleType()))
               ).cache()

    if year == 2017:
        path_temp = path2csv_mco + "MEDATU.CSV"
    elif year == 2016:
        path_temp = path2csv_mco + "medatu.csv"
    else:
        raise ValueError("Erreur année non prise en charge")
    # MEDATU (médicaments soumis à autorisation temporaire d'utilisation)
    mco_medatu = (spark.read
                  .option("header", "true")
                  .option("sep", ";")
                  .csv(path_temp)
                  .dropDuplicates()
                  .select(["ETA_NUM", "RSA_NUM", "ACH_PRI_ADM", "UCD_UCD_COD", "ADM_NBR",
                           "ADM_ANN", "ADM_MOIS", "SEJ_NBR"])
                  .withColumn("ADM_NBR", psf.col("ADM_NBR").cast(pst.DoubleType()))
                  .withColumn("ACH_PRI_ADM", psf.col("ACH_PRI_ADM").cast(pst.DoubleType()))
                  ).cache()

    if year == 2017:
        path_temp = path2csv_mco + "MEDTHROMBO.CSV"
    elif year == 2016:
        path_temp = path2csv_mco + "medthrombo.csv"
    else:
        raise ValueError("Erreur année non prise en charge")
    # MEDTHROMBO (médicaments thrombolytiques pour le traitement des AVC ischémiques)
    mco_medthrombo = (spark.read
                      .option("header", "true")
                      .option("sep", ";")
                      .csv(path_temp)
                      .select(["ETA_NUM", "RSA_NUM", "ADM_PRI_ADM", "UCD_UCD_COD", "ADM_NBR", "ADM_ANN", "SEJ_NBR"])
                      .withColumn("ADM_PRI_ADM", psf.col("ADM_PRI_ADM").cast(pst.DoubleType()))
                      .withColumn("ADM_NBR", psf.col("ADM_NBR").cast(pst.DoubleType()))
                      ).cache()

    if year == 2017:
        path_temp = path2csv_mco + "DMIP.CSV"
    elif year == 2016:
        path_temp = path2csv_mco + "dmip.csv"
    else:
        raise ValueError("Erreur année non prise en charge")
    # DMIP (DMI en sus)
    mco_dmip = (spark.read
                .option("header", "true")
                .option("sep", ";")
                .csv(path_temp)
                .select(["ETA_NUM", "RSA_NUM", "NBR_POS_PRI", "DAT_POS_ANN", "NBR_POS"])
                .withColumn("NBR_POS_PRI", psf.col("NBR_POS_PRI").cast(pst.DoubleType()))
                .withColumn("NBR_POS", psf.col("NBR_POS").cast(pst.DoubleType()))
                ).cache()

    # TABLE MED
    # Supprimer les lignes avec un nombre administré nul alors que le prix d'achat multiplié par le nombre administré
    # ne l'est pas (ATIH)
    mco_med = mco_med.filter((psf.col("ADM_NBR") != 0) & (psf.col("ADM_NBR_PRI") != 0))

    # Suprimer lignes avec un nombre de séjours impliqués nul ou manquant
    mco_med = mco_med.filter((psf.col("NBR_SEJ") != 0) & (~psf.col("NBR_SEJ").isNull()))

    # TABLE MEDATU
    # Supprimer les lignes avec un nombre administré nul alors que le prix d'achat multiplié par le nombre administré
    # ne l'est pas (ATIH)
    mco_medatu = mco_medatu.filter((psf.col("ADM_NBR") != 0) & (psf.col("ACH_PRI_ADM") != 0))
    mco_medatu = mco_medatu.filter((psf.col("SEJ_NBR") != 0) & (~psf.col("SEJ_NBR").isNull()))

    # TABLE MEDTHROMBO
    mco_medthrombo = mco_medthrombo.filter((psf.col("ADM_NBR") != 0) & (psf.col("ADM_PRI_ADM") != 0))
    mco_medthrombo = mco_medthrombo.filter((psf.col("SEJ_NBR") != 0) & (~psf.col("SEJ_NBR").isNull()))

    # TABLE DMIP
    mco_dmip = mco_dmip.filter((psf.col("NBR_POS") != 0) & (psf.col("NBR_POS_PRI") != 0))

    def _treat_med(df):
        df = df.join(mco_c.select(["ETA_NUM", "RSA_NUM", "NUM_ENQ"]), ["ETA_NUM", "RSA_NUM"], how="left")
        df = df.join(rac_mco.select(psf.col("NUM_ENQ")), "NUM_ENQ", how="inner")
        return df

    mco_med = _treat_med(mco_med)
    mco_medatu = _treat_med(mco_medatu)
    mco_medthrombo = _treat_med(mco_medthrombo)
    mco_dmip = _treat_med(mco_dmip)

    # Nettoyage MED SUS
    mco_med = _compare_ucd(mco_med, path2_ucd, year, spark)

    # On trouve de l'avastin dans MED SUS et MED ATU
    mco_medatu = mco_medatu.withColumnRenamed("UCD_UCD_COD", "UCD_13")
    # D'après ATIH, il faut tout mettre dans MED SUS et supprimer les doublons

    # MED ATU
    # Extraire lignes de MED ATU sur l'Avastin
    mco_medatu_avast = (mco_medatu
                        .filter(psf.col("UCD_13").isin(["3400892611044", "340092611105"]))
                        .coalesce(1)
                        .select(['ETA_NUM', 'RSA_NUM', 'NUM_ENQ', 'UCD_13',
                                 'ADM_MOIS', 'ADM_ANN', 'ADM_NBR', 'ACH_PRI_ADM', 'SEJ_NBR'])
                        .withColumnRenamed("ACH_PRI_ADM", 'ADM_NBR_PRI').withColumnRenamed("SEJ_NBR",
                                                                                           'NBR_SEJ')
                        # Fusionner avec ucd pour ajouter le prix
                        .join(ucd.select(["UCD_13", "PRIX_TTC"]), ["UCD_13"], how="left")
                        .select(['ETA_NUM', 'RSA_NUM', 'NUM_ENQ', 'UCD_13', 'ADM_MOIS',
                                 'ADM_ANN', 'ADM_NBR', 'ADM_NBR_PRI', 'NBR_SEJ', 'prix_TTC']))

    # MED SUS
    # Uniformiser les colonnes de MED SUS
    mco_med = (mco_med
               .withColumnRenamed("DAT_ADM_ANN", "ADM_ANN")
               .select(['ETA_NUM', 'RSA_NUM', 'NUM_ENQ', 'UCD_13', 'ADM_MOIS',
                        'ADM_ANN', 'ADM_NBR', 'ADM_NBR_PRI', 'NBR_SEJ', 'prix_TTC']))
    # Extraire lignes de MED SUS sur l'Avastin
    mco_med_avast = (mco_med.filter(psf.col("UCD_13").isin(["3400892611044", "340092611105"]))
                     .coalesce(1))
    # Rassembler le tout et enlever les duplicats
    avast = mco_med_avast.union(mco_medatu_avast).dropDuplicates()

    # Supprimer avastin dans medatu et med
    mco_medatu = mco_medatu.filter(~psf.col("UCD_13").isin(["3400892611044", "340092611105"]))
    mco_med = mco_med.filter(~psf.col("UCD_13").isin(["3400892611044", "340092611105"]))

    # Rajouter ensemble des lignes sur l'avastin sans doublon dans med
    mco_med = mco_med.union(avast)

    # Filtrer prix excessifs à l'aide des tarifs de responsabilité
    mco_med = (mco_med.withColumn("prix_unit", psf.col('ADM_NBR_PRI') / psf.col('ADM_NBR'))
               .filter(psf.col("prix_unit") < 20000)
               .withColumn("diff", psf.col("prix_unit") - psf.col("prix_TTC"))
               .withColumn("prop_diff",
                           psf.when(psf.col("prix_unit") != 0, psf.col("diff") / psf.col("prix_unit")).
                           otherwise(None))

               # Remplacer PRIX_UNIT par PRIX_TTC quand PRIX_UNIT semble excessif (plus de 10% superieur)
               .withColumn("PRIX_UNIT_C", psf.when(psf.col("prop_diff") > 0.1, psf.col("prix_TTC")).
                           otherwise(psf.col("prix_unit")))

               # Recalculer le montant global : prix unitaire * quantité administrée
               .withColumn("ADM_NBR_PRI_C", psf.col('PRIX_UNIT_C') * psf.col('ADM_NBR'))

               # Sortir les dépenses en SUS par UCD
               .withColumn("PRI_SEJ", psf.col("ADM_NBR_PRI_C") / psf.col("NBR_SEJ")))

    # Nettoyage autres tables
    # Les tables MED SUS et médicaments thrombolytiques sont bien complémentaires
    # par contre on peut avoir des doublons entre MED SUS et MED ATU
    temp = mco_med.join(mco_medatu, ["ETA_NUM", "RSA_NUM", "UCD_13"], how="inner")

    # Décision : suppression des doublons
    # Extraire les triplets "ETA_NUM","RSA_NUM","UCD_UCD_COD" à exclure de mco_medatu
    temp = (temp.withColumn("triplet", psf.concat_ws("-", psf.col("ETA_NUM"), psf.col("RSA_NUM"), psf.col("UCD_13")))
            .select(["triplet"])
            .dropDuplicates()
            .withColumn("a_excl", psf.lit(1)))

    # Supprimer les lignes de medatu qui sont aussi dans med
    mco_medatu = (mco_medatu.withColumn("triplet",
                                        psf.concat_ws("-", psf.col("ETA_NUM"), psf.col("RSA_NUM"), psf.col("UCD_13")))
                  .join(temp, ["triplet"], how="left"))

    # Supprimer les lignes pour lesquelles a_excl prend la valeur 1
    mco_medatu = (mco_medatu.filter(psf.col("a_excl").isNull())
                  # Table MEDATU
                  # Supprimer si plus de 1000 doses administrées
                  .filter(psf.col("ADM_NBR") < 1000)
                  # Repérer prix excessifs
                  .withColumn("prix_unit", psf.col('ACH_PRI_ADM') / psf.col('ADM_NBR'))
                  # Suppression des valeurs aberrantes
                  .filter(psf.col("prix_unit") < 50000)
                  # Diviser les quantités et prix d'achat par le nombre de séjours impliqués
                  .withColumn("PRI_SEJ", psf.col("ACH_PRI_ADM") / psf.col("SEJ_NBR")))

    # Diviser les quantités et prix d'achat par le nombre de séjours impliqués
    mco_medthrombo = mco_medthrombo.withColumn("PRI_SEJ", psf.col("ADM_PRI_ADM") / psf.col("SEJ_NBR"))

    # table de facturation
    mco_stc = _load_mco_stc(path2csv_mco, year, spark).cache()

    # Regrouper dépenses et sommer par séjour (en conservant la distinction entre les tables)
    def _regroup_expense_by_stay(df):
        if "PRI_SEJ" in df.columns:
            df = (df.groupBy([psf.col("ETA_NUM"), psf.col("RSA_NUM"), psf.col("NUM_ENQ")]).agg({"PRI_SEJ": "sum"})
                  .withColumnRenamed("sum(PRI_SEJ)", "dep_tot")
                  )
        else:
            df = (df.groupBy([psf.col("ETA_NUM"), psf.col("RSA_NUM"), psf.col("NUM_ENQ")]).agg({"NBR_POS_PRI": "sum"})
                  .withColumnRenamed("sum(NBR_POS_PRI)", "dep_tot")
                  )
        return df

    mco_med = _regroup_expense_by_stay(mco_med)
    mco_medatu = _regroup_expense_by_stay(mco_medatu)
    mco_medthrombo = _regroup_expense_by_stay(mco_medthrombo)
    mco_dmip = _regroup_expense_by_stay(mco_dmip)

    mco_med = mco_med.withColumn("poste", psf.lit("Med_SUS"))
    mco_medatu = mco_medatu.withColumn("poste", psf.lit("Med_ATU"))
    mco_medthrombo = mco_medthrombo.withColumn("poste", psf.lit("Med_THROMBO"))
    mco_dmip = mco_dmip.withColumn("poste", psf.lit("DMIP"))

    mco_public_sus = union_all(mco_med, mco_medatu, mco_medthrombo, mco_dmip)

    mco_public_sus = (mco_public_sus
                      .join(mco_stc.select(["ETA_NUM", "RSA_NUM", "EXO_TM"]), ["ETA_NUM", "RSA_NUM"], how="inner")
                      .withColumn("dep_lien_ald", psf.when(psf.col("EXO_TM").isin([2, 4]), 1).otherwise(0))
                      .withColumn("dep_lien_ald", psf.col("dep_lien_ald") * psf.col("dep_tot")))
    mco_public_sus = (mco_public_sus
                      .groupBy(psf.col("NUM_ENQ"))
                      .agg({"dep_tot": "sum", "dep_lien_ald": "sum", "NUM_ENQ": "count", "poste": "first"})
                      .withColumnRenamed("sum(dep_tot)", "dep_tot")
                      .withColumnRenamed("count(NUM_ENQ)", "nb_sejour")
                      .withColumnRenamed("sum(dep_lien_ald)", "dep_lien_ald")
                      .withColumnRenamed("first(poste)", "poste")
                      .withColumn("poste_ag", psf.lit("etab_public_mco"))
                      .withColumn("rac", psf.lit(0.0))
                      .withColumn("rac_lien_ald", psf.lit(0.0))
                      .withColumn("rac_amo_sup", psf.lit(0.0))
                      .filter(psf.col("nb_sejour") <= 100)
                      .filter(psf.col("dep_tot") <= 1000000)
                      )

    # Ne conserver que les séjours en MCO précédemment triés
    mco_public_sus = mco_public_sus.join(rac_mco.select(["NUM_ENQ", "BDI_DEP", "BDI_COD", "COD_SEX", "AGE_ANN"]),
                                         "NUM_ENQ", how="inner")

    # Ajouter colonne "remb_am"
    mco_public_sus = mco_public_sus.withColumn("remb_am", psf.col("dep_tot"))
    for c in ["MNT_TM", "MNT_PF", "MNT_FJ", "NBSEJ_CPLT_LONG", "NBSEJ_CPLT_COURT", "NBSEJ_PARTL",
              "NBJ_CPLT", "NBJ_PARTL"]:
        mco_public_sus = mco_public_sus.withColumn(c, psf.lit(None))

    # Selectionner et réordonner colonnes pour qu'elles soient dans le même ordre
    mco_public_sus = mco_public_sus.select(list_output_var)

    return mco_public_sus


def agg_ace(path2flat, path2csv_mco, path2nomenclature, path2_ir_orc, filter_etab, year, spark, list_output_var):
    # table de valorisation des consultations externes à l'hôpital public
    ace_c, __ = _load_ace_c(path2flat, year, spark)

    if year == 2017:
        path_temp = path2csv_mco + "VALOACE.CSV"
    elif year == 2016:
        path_temp = path2csv_mco + "valoace.csv"
    else:
        raise ValueError("Erreur année non prise en charge")
    valo_ace = (spark.read
                .option("header", "true")
                .option("sep", ";")
                .csv(path_temp)
                .select(["ETA_NUM", "SEQ_NUM", "PF18", "VALO", "MNT_BR", "MNT_REMB"])
                .withColumnRenamed("SEQ_NUM", "RSA_NUM")
                .withColumn("MNT_BR", psf.col("MNT_BR").cast(pst.DoubleType()))
                .withColumn("MNT_REMB", psf.col("MNT_REMB").cast(pst.DoubleType()))
                .join(ace_c, ["ETA_NUM", "RSA_NUM"], how="left")
                .filter(psf.col("VALO").isin([1, 2, 3, 4, 5]))
                )

    # Filtre sur les etablissements
    valo_ace = valo_ace.filter(~psf.col("ETA_NUM").isin(filter_etab))

    # assert valo_ace.select(["EXE_SOI_DTF"]).filter(psf.col("EXE_SOI_DTF").isNull()).count() == 0
    # assert valo_ace.select(["EXE_SOI_DTD"]).filter(psf.col("EXE_SOI_DTD").isNull()).count() == 0

    # On se limite aux ACE de l'année
    valo_ace = (valo_ace.filter((psf.col("EXE_SOI_DTD") <= str(year) + "-12-31") &
                                (~psf.col("EXE_SOI_DTD").isNull()))
                .filter((psf.col("EXE_SOI_DTF") <= str(year) + "-12-31") &
                        (~psf.col("EXE_SOI_DTF").isNull()))
                .filter(psf.col("MNT_REMB") != 0))

    # assert valo_ace.filter(psf.col("MNT_BR") < psf.col("MNT_REMB")).count() == 0

    # Création des postes détaillés à partir de la spécialité du PS exécutant
    # Import de la table de correspondance entre les codes spécialités et les postes de soins,
    # `pse_act_spe_x_spe_act.txt`.
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

    valo_ace = (valo_ace.join(psf.broadcast(specialite), ["PSE_ACT_SPE"], "left")
                .withColumn("poste",
                            psf.when(psf.col("ATU") == 1, psf.lit("ace_urgences")).
                            when(psf.col("spe_act_det") == "Fournisseur",
                                 psf.lit("autres_spe_cliniq")).  # car seulement 4 lignes
                            when(psf.col("spe_act_det") == "Labo", psf.lit("Labo")).
                            when(psf.col("spe_act_det") == "Pharma_", psf.lit("Pharma")).
                            when(psf.col("spe_act_det").isNotNull(),
                                 psf.concat(psf.col("spe_act_det"), psf.lit("_cliniq"))).
                            otherwise(psf.lit("autres_spe_cliniq"))
                            )
                .withColumn("poste_ag", psf.lit("etab_public_mco_ace"))
                .withColumn("rac_sej", psf.col("MNT_BR") - psf.col("MNT_REMB"))
                )

    ir_orc_r = _load_ir_orc(path2_ir_orc, year, spark)
    valo_ace = (valo_ace.join(ir_orc_r, ["NUM_ENQ"], how="left")
                .withColumn("top_cmu",
                            psf.when(psf.col("top_cmu") == 1, 1).
                            otherwise(0))
                # Pour les détenus, AME, SU et CMU-cistes, le RAC AMO supp est à 0 (prise en charge TM + FJ par l'Etat)
                .withColumn("rac_amo_sup",
                            psf.when(psf.col("VALO").isin([3, 4, 5]), 0).
                            when(psf.col("top_cmu") == 1, 0).
                            otherwise(psf.col("rac_sej")))
                .withColumn("rac_lien_ald", psf.col("dep_lien_ald") * psf.col("rac_amo_sup"))
                .withColumn("dep_lien_ald", psf.col("dep_lien_ald") * psf.col("MNT_BR"))
                )
    # Aller chercher informations individuelles dans fastc - ACE entête facture 
    fastc = (spark.read
             .option("header", "true")
             .option("sep", ";")
             .parquet(path2flat + "single_table/MCO_FASTC/year=" + str(year))
             .select(["ETA_NUM", "SEQ_NUM", "BDI_DEP", "BDI_COD", "COD_SEX", "AGE_ANN"])
             .withColumnRenamed("SEQ_NUM", "RSA_NUM")
             )
    # Ajouter infos persos à valo_ace 
    valo_ace = valo_ace.join(fastc, ["ETA_NUM", "RSA_NUM"], how="left")

    # Calcul du rac par bénéficiaire
    # En différenciant les passages aux urgence du reste des ATU
    # En comptant nombre de séjours par bénéficiaire en / hors ATU

    rac_ace = (valo_ace.groupBy("NUM_ENQ", "poste", "poste_ag")
               .agg({"MNT_BR": "sum", "MNT_REMB": "sum", "NUM_ENQ": "count", "rac_amo_sup": "sum",
                     "dep_lien_ald": "sum", "rac_lien_ald": "sum"})
               .withColumnRenamed("sum(MNT_REMB)", "remb_am")
               .withColumnRenamed("sum(MNT_BR)", "dep_tot")
               .withColumnRenamed("count(NUM_ENQ)", "nb_sejour")
               .withColumnRenamed("sum(rac_amo_sup)", "rac_amo_sup")
               .withColumn("rac", psf.col("dep_tot") - psf.col("remb_am"))
               .withColumnRenamed("sum(rac_lien_ald)", "rac_lien_ald")
               .withColumnRenamed("sum(dep_lien_ald)", "dep_lien_ald")
               )

    # Ajouter colonnes vides pour que toutes les bases aient les mêmes colonnes
    for c in ["MNT_TM", "MNT_PF", "MNT_FJ", "NBSEJ_CPLT_LONG", "NBSEJ_CPLT_COURT", "NBSEJ_PARTL",
              "NBJ_CPLT", "NBJ_PARTL"]:
        rac_ace = rac_ace.withColumn(c, psf.lit(None))

    # Ajouter informations individuelles 
    rac_ace = rac_ace.join(valo_ace.select(["NUM_ENQ", "BDI_DEP", "BDI_COD", "COD_SEX", "AGE_ANN"]).distinct(),
                           ["NUM_ENQ"], how="left")

    # Selectionner et réordonner colonnes pour qu'elles soient dans le même ordre
    rac_ace = rac_ace.select(list_output_var)
    return rac_ace


def agg_med_ace(path2flat, path2_ucd, rac_ace, path2csv_mco, year, list_output_var, spark):
    """
    Médicaments et dispositifs implantables de la liste en SUS (ACE)
    :param path2_ucd:
    :param path2flat:
    :param list_output_var:
    :param year:
    :param path2csv_mco:
    :param rac_ace:
    :param spark:
    :return:
    """
    ucd = _load_ucd(path2_ucd, spark)
    ace_c, fcstc = _load_ace_c(path2flat, year, spark)

    if year == 2017:
        path_temp = path2csv_mco + "FHSTC.CSV"
    elif year == 2016:
        path_temp = path2csv_mco + "fhstc.csv"
    else:
        raise ValueError("Erreur année non prise en charge")
    mco_fhstc = (spark.read
                 .option("header", "true")
                 .option("sep", ";")
                 .csv(path_temp)
                 .withColumnRenamed("SEQ_NUM", "RSA_NUM")
                 .withColumn("FAC_MNT", psf.col("FAC_MNT").cast(pst.DoubleType()))
                 )

    # Supprimer si quantité, prix unitaire ou montant facturé nul
    mco_fhstc = mco_fhstc.filter((psf.col("QUA") > 0) & (psf.col("ACH_PRU_TTC") > 0) & (psf.col("FAC_MNT") > 0))

    # Supprimer si coefficient de fractionnement nul
    mco_fhstc = mco_fhstc.filter(psf.col("FRACT_COE") != 0)

    # Joindre liste med sus a med sus
    mco_fhstc = mco_fhstc.withColumn("UCD_7", psf.substring(psf.col("UCD_UCD_COD"), 7, 7))
    mco_fhstc = mco_fhstc.join(ucd, ["UCD_7"], how="left")

    # Creer un top LES (liste en SUS) : 1 si dans la liste, 0 sinon
    mco_fhstc = mco_fhstc.withColumn("top_LES",
                                     # Gestion des medicaments qui ne changent pas de statut au cours de l'annéé
                                     psf.when((psf.col("date_insc") > str(year) + "-12-31") |
                                              (psf.col("date_insc").isNull()), 0).  # non inscrits avant année
                                     when(psf.col("date_rad") < str(year) + "-01-01", 0).  # radiés avant année
                                     # Gestion des médicaments radiés en cours d'année
                                     when((psf.col("ann_rad") == psf.col("ENT_ANN")) &
                                          (psf.col("ENT_MOI") > psf.col("mois_rad")), 0).
                                     # Gestion des médicaments inscrits en cours d'année
                                     when((psf.col("ann_insc") == psf.col("ENT_ANN")) &
                                          (psf.col("ENT_MOI") < psf.col("mois_insc")), psf.lit(0)).
                                     otherwise(1))

    # Supprimer medicaments hors liste en sus
    mco_fhstc = mco_fhstc.filter(psf.col("top_LES") != 0).drop(psf.col("top_LES"))

    # Regrouper par séjour et sommer
    mco_fhstc = (mco_fhstc.groupBy([psf.col("ETA_NUM"), psf.col("RSA_NUM")]).agg({"FAC_MNT": "sum"})
                 .withColumnRenamed("sum(FAC_MNT)", "DEP_SUS")
                 .join(fcstc.select('ETA_NUM', 'RSA_NUM', "dep_lien_ald"), ['ETA_NUM', 'RSA_NUM'], how="left")
                 .withColumn("dep_lien_ald", psf.col("dep_lien_ald") * psf.col("DEP_SUS")))

    # Regrouper pas bénéficiaire
    ace_sus = (mco_fhstc.join(ace_c.select(["ETA_NUM", "RSA_NUM", "NUM_ENQ"]), ["ETA_NUM", "RSA_NUM"], how="left")
               .groupBy(psf.col("NUM_ENQ")).agg({"DEP_SUS": "sum", "NUM_ENQ": "count", "dep_lien_ald": "sum"})
               .withColumnRenamed("sum(DEP_SUS)", "dep_tot")
               .withColumnRenamed("count(NUM_ENQ)", "nb_sejour")
               .withColumnRenamed("sum(dep_lien_ald)", "dep_lien_ald")
               .join(rac_ace.select(["NUM_ENQ", "BDI_DEP", "BDI_COD", "COD_SEX", "AGE_ANN"]), "NUM_ENQ", how="inner")
               .withColumn("poste_ag", psf.lit("etab_public_mco_ace"))
               .withColumn("poste", psf.lit("ace_Med_SUS"))
               .withColumn("rac", psf.lit(0.0))
               .withColumn("rac_amo_sup", psf.lit(0.0))
               .withColumn("rac_lien_ald", psf.lit(0.0))
               .withColumn("remb_am", psf.col("dep_tot")))

    # Ajouter colonnes vides pour que toutes les bases aient les mêmes colonnes
    for c in ["MNT_TM", "MNT_PF", "MNT_FJ", "NBSEJ_CPLT_LONG", "NBSEJ_CPLT_COURT", "NBSEJ_PARTL",
              "NBJ_CPLT", "NBJ_PARTL"]:
        ace_sus = ace_sus.withColumn(c, psf.lit(None))

    # Selectionner et réordonner colonnes pour qu'elles soient dans le même ordre
    ace_sus = ace_sus.select(list_output_var)

    return ace_sus

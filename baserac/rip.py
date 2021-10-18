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

import pyspark.sql.functions as psf
import pyspark.sql.types as pst
from .hospit import _load_ir_orc, _load_ir_imb_unity


def agg_rip(path2flat, path2csv_rip, path2_ir_orc, path2ref, year, spark, filter_etab, list_output_var, 
           path2aberr=None):
    if year == 2017:
        rip_c = (spark.read
                 .parquet(path2flat + "single_table/RIP_C/year=" + str(year) + "/")
                 .select(["NUM_ENQ", "ETA_NUM_EPMSI", "RIP_NUM", "EXE_SOI_DTD", "EXE_SOI_DTF"])
                 )
    elif year == 2016:
        rip_c = (spark.read
                 .parquet(path2flat + "single_table/RIP_C")
                 .select(["NUM_ENQ", "ETA_NUM_EPMSI", "RIP_NUM", "EXE_SOI_DTD", "EXE_SOI_DTF"])
                 )
    else:
        raise ValueError("Année non disponible")

    # On importe la table S de description du sejour pour avoir le mode de sortie 
    if year == 2017:
        path_temp = path2csv_rip + "S.CSV"
        var2select = ["ETA_NUM_EPMSI", "RIP_NUM", "ENT_MOD", "ENT_PRV", "SOR_MOD", "SOR_DES", "HOS_TYP",
                      "SEJ_DUREE", "SEJ_FINI", "SOR_ANN"]
    elif year == 2016:
        path_temp = path2csv_rip + "s.csv"
        var2select = ["ETA_NUM_EPMSI", "RIP_NUM", "ENT_MOD", "ENT_PRV", "SOR_MOD", "SOR_DES", "HOS_TYP",
                      "SEJ_DUREE", "SEJ_FINI", "SEJ_NBJ_CPLT", "SEJ_NBJ_PARTL", "SOR_ANN"]
    else:
        raise ValueError("Erreur année non prise en charge")

    rip_s = (spark.read
             .option("header", "true")
             .option("sep", ";")
             .csv(path_temp)
             .select(var2select)
             )
    # assert rip_s.count() == rip_c.count()
    rip_c = rip_c.join(rip_s, ["ETA_NUM_EPMSI", "RIP_NUM"], how="left")

    if year == 2017:
        rip_rsa = (spark.read
                   .option("header", "true")
                   .option("sep", ";")
                   .csv(path2csv_rip + "RSA.CSV")
                   .select(["ETA_NUM_EPMSI", "RIP_NUM", 'FOR_ACT', "SEQ_SEQ_NUM", "PRE_JOU_NBJ",
                            "PRE_DEM_JOU_NBJ", "SEQ_COU_NBJ", "BDI_DEP", "BDI_COD", "COD_SEX", "AGE_ANN"])
                   .withColumn("SEQ_NBJ_CPLT",
                               psf.when(psf.col("FOR_ACT").isin(["01", "02", "03", "04", "05", "06", "07"]),
                                        psf.col("PRE_JOU_NBJ")).
                               otherwise(0))
                   .withColumn("SEQ_NBJ_PARTL",
                               psf.when(psf.col("FOR_ACT").isin(["20", "21", "22", "23"]),
                                        psf.col("PRE_JOU_NBJ") + psf.col("PRE_DEM_JOU_NBJ") * 0.5).
                               otherwise(0))
                   .groupBy("ETA_NUM_EPMSI", "RIP_NUM", "BDI_DEP", "BDI_COD", "COD_SEX", "AGE_ANN")
                   .agg({"SEQ_NBJ_CPLT": "sum", "SEQ_NBJ_PARTL": "sum"})
                   .withColumnRenamed("sum(SEQ_NBJ_CPLT)", "SEJ_NBJ_CPLT").withColumnRenamed("sum(SEQ_NBJ_PARTL)",
                                                                                             "SEJ_NBJ_PARTL")
                   )
    elif year == 2016:
        rip_rsa = (spark.read
                   .option("header", "true")
                   .option("sep", ";")
                   .csv(path2csv_rip + "rsa.csv")
                   .select(["ETA_NUM_EPMSI", "RIP_NUM", "BDI_DEP", "BDI_COD", "COD_SEX", "AGE_ANN"])
                   .distinct()
                   )
    else:
        raise ValueError("Erreur année non prise en charge")

    rip_c = rip_c.join(rip_rsa, ["ETA_NUM_EPMSI", "RIP_NUM"], how="left")

    # Table de facturation
    if year == 2017:
        path_temp = path2csv_rip + "STC.CSV"
    elif year == 2016:
        path_temp = path2csv_rip + "stc.csv"
    else:
        raise ValueError("Erreur année non prise en charge")
    rip_stc = (spark.read
               .option("header", "true")
               .option("sep", ";")
               .csv(path_temp)
               .select(["ETA_NUM_EPMSI", "RIP_NUM", "SEJ_FAC_AM", "TOT_MNT_AM", "TOT_MNT_AMC", "REM_BAS_MNT",
                        "FAC_MNT_TM", "FAC_MNT_FJ", "MAJ_MNT_PS", "REM_TAU", "NAT_ASS", "EXO_TM", "FJ_COD_PEC",
                        "FAC_18E", "BEN_CMU", "FAC_NBR_VEN"])
               )

    rip_valo = rip_c.join(rip_stc, ["ETA_NUM_EPMSI", "RIP_NUM"], how="inner")

    # Suppression des valeurs nulles
    rip_valo = rip_valo.filter(~psf.col("HOS_TYP").isNull())

    ir_imb_unity = _load_ir_imb_unity(path2flat, path2ref, year, spark)
    ir_orc_r = _load_ir_orc(path2_ir_orc, year, spark)
    # Ajouter le TOP ALD et numéro / nombre ALD
    rip_valo = (rip_valo.join(ir_imb_unity, ["NUM_ENQ"], how="left")
                .join(ir_orc_r, ["NUM_ENQ"], how="left")
                .withColumn("top_cmu",
                            psf.when(psf.col("top_cmu") == 1, 1).otherwise(0))
                .withColumn("contains_ald23",
                            psf.array_contains(psf.col("NUM_ALD"), '23'))  # Affection psychiatrique de longue durée
                .withColumn("contains_ald15",
                            psf.array_contains(psf.col("NUM_ALD"), '15'))  
                .withColumn("contains_ald16",
                            psf.array_contains(psf.col("NUM_ALD"), '16'))
                )

    # Supprimer les sejours non valorises ou en attente de valorisation
    rip_valo = rip_valo.filter(psf.col("SEJ_FAC_AM") == 1)

    # Suppression des sejours sans information de facturation à l'AM
    rip_valo = rip_valo.filter((psf.col("TOT_MNT_AM") != 0))

    # Filtre sur les etablissements
    rip_valo = rip_valo.filter(~psf.col("ETA_NUM_EPMSI").isin(filter_etab))

    # Exclusion des prestations inter-établissement
    rip_valo = rip_valo.filter((psf.col("ENT_MOD") != 0) & (psf.col("SOR_MOD") != 0))
    for c in ["SEJ_DUREE", "SEJ_NBJ_CPLT", "SEJ_NBJ_PARTL", "TOT_MNT_AM", "TOT_MNT_AMC", "REM_BAS_MNT",
              "FAC_MNT_TM", "FAC_MNT_FJ", "REM_TAU"]:
        rip_valo = rip_valo.withColumn(c, psf.col(c).cast(pst.DoubleType()))

    # Gestion des dates
    if year == 2016:
        # il y a un "2016" qui s'est transforme en "2106"
        rip_valo = rip_valo.withColumn("EXE_SOI_DTF",
                                       psf.when(psf.col("EXE_SOI_DTF") == "2106-08-16", psf.lit("2016-08-16")).
                                       otherwise(psf.col("EXE_SOI_DTF")))

    # Suppression des sejours commencés après l'année
    rip_valo = rip_valo.filter(psf.col("EXE_SOI_DTD") <= str(year) + "-12-31")
    rip_valo = rip_valo.filter(psf.col("EXE_SOI_DTF").isNotNull())

    # Supprimer tous les sejours qui ne se terminent pas dans l'année
    rip_valo = rip_valo.filter(psf.col("EXE_SOI_DTF") <= str(year) + "-12-31")

    # Supprimer les séjours qui se terminent avant d'avoir commencé
    rip_valo = rip_valo.filter(psf.col("EXE_SOI_DTD") <= psf.col("EXE_SOI_DTF"))

    # Creation de la variable de duree de sejour
    rip_valo = rip_valo.withColumn("duree_sejour", psf.datediff(psf.col("EXE_SOI_DTF"), psf.col("EXE_SOI_DTD")))

    rip_valo = rip_valo.withColumn("SEJ_NBJ_CPLT",
                                   psf.when(psf.col("SEJ_NBJ_CPLT") > (psf.col("duree_sejour") + 1),
                                            (psf.col("duree_sejour") + 1)).
                                   otherwise(psf.col("SEJ_NBJ_CPLT")))

    rip_valo = rip_valo.withColumn("SEJ_NBJ_PARTL",
                                   psf.when(psf.col("SEJ_NBJ_PARTL") > (psf.col("duree_sejour") + 1),
                                            (psf.col("duree_sejour") + 1)).
                                   otherwise(psf.col("SEJ_NBJ_PARTL")))

    # Créer la variable type de séjours avec 4 modalités : séjour en HP court / long, séjour en HC court
    # (moins de 30 jours), séjour en HC long (30 jours ou plus)
    rip_valo = rip_valo.withColumn("TYP_SEJ",
                                   psf.when((psf.col("HOS_TYP") == 2) & (psf.col("SEJ_NBJ_PARTL") < 30),
                                            psf.lit("PARTL_COURT")).
                                   when((psf.col("HOS_TYP") == 2) & (psf.col("SEJ_NBJ_PARTL") >= 30),
                                        psf.lit("PARTL_LONG")).
                                   when((psf.col("HOS_TYP") == 1) & (psf.col("SEJ_NBJ_CPLT") < 30),
                                        psf.lit("CPLT_COURT")).
                                   when((psf.col("HOS_TYP") == 1) & (psf.col("SEJ_NBJ_CPLT") >= 30),
                                        psf.lit("CPLT_LONG")))

    # Remplacer les NULL par des 0
    rip_valo = rip_valo.withColumn("PF18_C", psf.when(psf.col("FAC_18E") == 1, 18).otherwise(0))

    # Recalculer taux de remboursement selon les directives de l'ATIH
    rip_valo = rip_valo.withColumn("taux_atih",
                                   psf.when(psf.col("EXO_TM").isin(["X", ""]) | psf.col("EXO_TM").isNull() |
                                            psf.col("NAT_ASS").isin(["XX", ""]), None).
                                   when(psf.col("EXO_TM").isin(["0", "2"]) & psf.col("NAT_ASS").isin(["10", "13"]), 80).
                                   when(psf.col("EXO_TM").isin(["0", "2"]), 100).
                                   when(psf.col("EXO_TM").isin(["9"]) & psf.col("NAT_ASS").isin(["10"]), psf.lit(90)).
                                   otherwise(100))

    # Creer un TAUX_C corrige
    rip_valo = rip_valo.withColumn("TAUX_C",
                                   psf.when((psf.col("PF18_C") == 18), 100).
                                   when(psf.col("contains_ald23") == True, 100).
                                   when(psf.col("contains_ald15") == True, 100).
                                   when(psf.col("contains_ald16") == True, 100).
                                   when(psf.col("REM_TAU") == psf.col("taux_ATIH"), psf.col("REM_TAU")).
                                   when(psf.col("taux_ATIH").isNull(), psf.col("REM_TAU")).
                                   when(psf.col("REM_TAU") < 80, psf.col("taux_ATIH")).
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
                                   when((psf.col("REM_TAU") == 0) & (~psf.col("taux_ATIH").isNull()),
                                        psf.col("taux_ATIH")).
                                   when(psf.col("taux_ATIH").isNotNull(), psf.col("taux_ATIH")).
                                   otherwise(None))

    rip_valo = rip_valo.filter(psf.col("TAUX_C") != 0)

    # Creation de la variable TM_C (ticket moderateur corrige)
    rip_valo = rip_valo.withColumn("DIFF", psf.col("REM_BAS_MNT") - psf.col("TOT_MNT_AM"))
    rip_valo = rip_valo.withColumn("TM_C",
                                   psf.when((psf.col("TAUX_C") == 100), 0).
                                   when(psf.col("FAC_MNT_TM").isNull(), 0).
                                   when(psf.col("FAC_MNT_TM") < 0, 0).
                                   otherwise(psf.col("FAC_MNT_TM"))
                                   )
    rip_valo = rip_valo.withColumn("TM_C", psf.col("TM_C").cast(pst.DoubleType()))

    # Recalcul du FJ a partir du type d'hospitalisation de la duree de sejour, du motif d'exonératin du FJ,
    # du nombre de jour de presence et du mode de sortie (SOR_MOD)
    rip_valo = rip_valo.withColumn("FJ_C",
                                   psf.when(psf.col("HOS_TYP") == 2, 0).
                                   when(psf.col("FJ_COD_PEC") == "R", 0).
                                   when((psf.col("duree_sejour") == 0) | (psf.col("SEJ_NBJ_CPLT") <= 1), 0).
                                   when(psf.col("SOR_MOD").isin(["6", "7", "9"]),
                                        (psf.col("SEJ_NBJ_CPLT") - 1) * 13.5).
                                   otherwise(psf.col("SEJ_NBJ_CPLT") * 13.5))

    # Ajouter variable FJ_C2 qui tienne compte de la limite de 30 jours
    # (limite au-delà de laquelle on paie le FJ même si on payait le TM les 30 premiers jours)
    rip_valo = rip_valo.withColumn("FJ_C2",
                                   psf.when(psf.col("FJ_C") == 0, 0).
                                   when((psf.col("TAUX_C") == 100), psf.col("FJ_C")).
                                   # Maintenant on traire les séjours avec des TAUX_C < 100
                                   # Cas des séjours en HC de moins de 30 jours de présence (pas de FJ (car TM) ou
                                   # alors FJ jour de sortie)
                                   when((psf.col("SEJ_NBJ_CPLT") <= 30) & (psf.col("SOR_MOD").isin(["6", "7", "9"])),
                                        psf.lit(0.0)).
                                   when((psf.col("SEJ_NBJ_CPLT") <= 30), psf.lit(13.5)).
                                   # Cas des séjours en HC de plus de 30 jours de présence (FJ au-delà du 30 ème jour)
                                   when((psf.col("SEJ_NBJ_CPLT") > 30) & (psf.col("SOR_MOD").isin(["6", "7", "9"])),
                                        (psf.col("SEJ_NBJ_CPLT") - 31) * 13.5).
                                   when((psf.col("SEJ_NBJ_CPLT") > 30), (psf.col("SEJ_NBJ_CPLT") - 30) * 13.5).
                                   when(psf.col("SEJ_NBJ_CPLT").isNull(), psf.col("FJ_C")).
                                   otherwise(None))
    rip_valo = rip_valo.withColumn("FJ_C2", psf.col("FJ_C2").cast(pst.DoubleType()))

    # assert rip_valo.select("FJ_C2").filter(psf.col("FJ_C2").isNull()).count() == 0

    # Gérer le cas des séjours de plus de 30 jours
    rip_valo = rip_valo.withColumn("TM_C",
                                   psf.when((psf.col("HOS_TYP") == 1) & (psf.col("SEJ_NBJ_CPLT") > 30),
                                            (psf.col("TM_C") * 30) / psf.col("SEJ_NBJ_CPLT")).
                                   when((psf.col("HOS_TYP") == 2) & (psf.col("SEJ_NBJ_PARTL") > 30),
                                        (psf.col("TM_C") * 30) / psf.col("SEJ_NBJ_PARTL")).
                                   otherwise(psf.col("TM_C")))

    rip_valo = (rip_valo.withColumn("dep_lien_ald", psf.when(psf.col("EXO_TM").isin([2, 4]), 1).otherwise(0))
                .withColumn("rac_lien_ald", psf.when(psf.col("EXO_TM").isin([2, 4]), 1).otherwise(0)))

    # Rac
    rip_valo = rip_valo.withColumn("PF_sej",
                                   psf.when(psf.col("TAUX_C") == 100, psf.col("PF18_C")).
                                   when(psf.col("TAUX_C") != 100, 0).
                                   otherwise(None))
    rip_valo = rip_valo.withColumn("TM_sej",
                                   psf.when(psf.col("TAUX_C") == 100, 0).
                                   when((psf.col("TAUX_C") != 100) & (psf.col("TM_C") > psf.col("FJ_C")),
                                        psf.col("TM_C")).
                                   when((psf.col("TAUX_C") != 100) & (psf.col("TM_C") <= psf.col("FJ_C")),
                                        0).
                                   otherwise(None))

    # Cas où le TM est supérieur à la base de remboursement 
    rip_valo = rip_valo.withColumn("TM_sej",
                                   psf.when(
                                       (psf.col("TM_sej") >= psf.col("REM_BAS_MNT")) & (psf.col("REM_BAS_MNT") != 0),
                                       psf.col("REM_BAS_MNT") * (100 - psf.col("TAUX_C")) / 100).
                                   otherwise(psf.col("TM_sej")))
    # On supprime une ligne aberrante en 2016 en raison caractéristiques du séjour qui ne correspondent
    # pas à la facturation
    if path2aberr is not None and year == 2016:
        mnt_aberrant = pd.read_csv(path2aberr)
        rip_valo = rip_valo.filter(psf.col("FAC_MNT_TM") != mnt_aberrant)

    rip_valo = rip_valo.withColumn("FJ_sej",
                                   psf.when(psf.col("FJ_C") == 0, 0).
                                   when((psf.col("TAUX_C") == 100), psf.col("FJ_C2")).
                                   when((psf.col("TAUX_C") != 100) & (psf.col("TM_C") > psf.col("FJ_C")),
                                        psf.col("FJ_C2")).
                                   when((psf.col("TAUX_C") != 100) & (psf.col("TM_C") <= psf.col("FJ_C")),
                                        psf.col("FJ_C")).
                                   otherwise(None))

    # Calculer le RAC en sommant ces termes
    rip_valo = rip_valo.withColumn("RAC", psf.col("PF_sej") + psf.col("TM_sej") + psf.col("FJ_sej"))

    # Le montant total du séjour (pour l'AM) est donc la somme du montant "soi-disant" facturé à l'AM et du RaC
    rip_valo = rip_valo.withColumn("tot_dep", psf.col("TOT_MNT_AM") + psf.col("RAC"))
    # Puis on recalcule la dépense faite au titre de l'ALD
    rip_valo = rip_valo.withColumn("dep_lien_ald", psf.col("dep_lien_ald") * psf.col("tot_dep"))
    # Pour les CMU-cistes, le RAC AMO supp est à 0 (prise en charge TM + FJ par l'Etat)
    rip_valo = (rip_valo.withColumn("rac_amo_sup",
                                    psf.when(psf.col("top_cmu") == 1, 0).
                                    otherwise(psf.col("RAC")))
                # Dans ce cas, mettre  PF, FJ et TM à 0 pour que le RAC_AMO_SUP reste la somme de ces 3 composantes
                .withColumn("TM_sej",
                            psf.when(psf.col("top_cmu") == 1, 0).
                            otherwise(psf.col("TM_sej")))
                .withColumn("PF_sej",
                            psf.when(psf.col("top_cmu") == 1, 0).
                            otherwise(psf.col("PF_sej")))
                .withColumn("FJ_sej",
                            psf.when(psf.col("top_cmu") == 1, 0).
                            otherwise(psf.col("FJ_sej")))
                # Puis on recalcule le rac amo sup pour les dépenses faites au titre de l'ALD
                .withColumn("rac_lien_ald", psf.col("rac_lien_ald") * psf.col("rac_amo_sup")))

    for c in ["NBSEJ_PARTL_COURT", "NBSEJ_PARTL_LONG", "NBSEJ_CPLT_COURT", "NBSEJ_CPLT_LONG"]:
        rip_valo = rip_valo.withColumn(c, psf.when(psf.col("TYP_SEJ") == c.replace("NBSEJ_", ""), 1).otherwise(0))

    # En conservant l'info sur le nombre de séjours par bénéficiaire
    rac_rip = rip_valo.groupBy(psf.col("NUM_ENQ")).agg(
        {"RAC": "sum", "tot_dep": "sum", "PF_sej": "sum", "TM_sej": "sum", "FJ_sej": "sum",
         "NUM_ENQ": "count", "TOT_MNT_AM": "sum", "SEJ_NBJ_CPLT": "sum", "SEJ_NBJ_PARTL": "sum",
         "NBSEJ_PARTL_COURT": "sum", "NBSEJ_PARTL_LONG": "sum", "NBSEJ_CPLT_COURT": "sum", "NBSEJ_CPLT_LONG": "sum",
         "dep_lien_ald": "sum", "rac_lien_ald": "sum", "rac_amo_sup": "sum", 'top_cmu': "max"})

    rac_rip = (rac_rip.withColumnRenamed("sum(RAC)", "rac").withColumnRenamed("sum(tot_dep)", "dep_tot")
               .withColumnRenamed("sum(PF_sej)", "MNT_PF")
               .withColumnRenamed("sum(TM_sej)", "MNT_TM")
               .withColumnRenamed("sum(FJ_sej)", "MNT_FJ")
               .withColumnRenamed("count(NUM_ENQ)", "nb_sejour")
               .withColumnRenamed("sum(TOT_MNT_AM)", "remb_am")
               .withColumnRenamed("sum(SEJ_NBJ_CPLT)", "NBJ_CPLT")
               .withColumnRenamed("sum(SEJ_NBJ_PARTL)", "NBJ_PARTL")
               .withColumnRenamed("sum(NBSEJ_PARTL_COURT)", "NBSEJ_PARTL_COURT")
               .withColumnRenamed("sum(NBSEJ_CPLT_COURT)", "NBSEJ_CPLT_COURT")
               .withColumnRenamed("sum(NBSEJ_PARTL_LONG)", "NBSEJ_PARTL_LONG")
               .withColumnRenamed("sum(NBSEJ_CPLT_LONG)", "NBSEJ_CPLT_LONG")
               .withColumnRenamed("sum(dep_lien_ald)", "dep_lien_ald")
               .withColumnRenamed("sum(rac_lien_ald)", "rac_lien_ald")
               .withColumnRenamed("sum(rac_amo_sup)", "rac_amo_sup")
               .withColumnRenamed("max(top_cmu)", "top_cmu")
               .withColumn("poste_ag", psf.lit("etab_public_rip"))
               .withColumn("poste", psf.lit("sejour"))
               )

    # Ajouter informations individuelles 
    rac_rip = rac_rip.join(rip_valo.select(["NUM_ENQ", "BDI_DEP", "BDI_COD", "COD_SEX", "AGE_ANN"]).distinct(),
                           ["NUM_ENQ"], how="left")

    # Selectionner et réordonner colonnes pour qu'elles soient dans le même ordre
    rac_rip = rac_rip.select(list_output_var)
    return rac_rip

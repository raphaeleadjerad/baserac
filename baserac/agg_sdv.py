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
from pyspark.ml.feature import Bucketizer
import numpy as np
from .indicators import redefine_columns


def union_sdv(path2output, agg_dict, audio, optical, spark, verbose=False):
    rac_nonlppdental_ag = (spark.read.parquet(path2output + "rac_nonlppdental_ag")
                           .withColumn("poste_ag", psf.lit("soins_ville")))
    if verbose:
        print("Nbre lignes base rac hors lpp et dentaire : {}".format(rac_nonlppdental_ag.count()))

    rac_dental_ag = (spark.read.parquet(path2output + "rac_dental_ag")
                     .withColumn("poste_ag", psf.lit("dentaire")))
    if verbose:
        print("Nbre lignes base rac dentaire avec codes rgpt dentaire : {}".format(rac_dental_ag.count()))

    rac_dental_nocode_ag = (spark.read.parquet(path2output + "rac_dental_nocode_ag")
                            .withColumn("poste_ag", psf.lit("dentaire")))
    if verbose:
        print("Nbre lignes base rac dentaire sans codes rgpt dentaire : {}".format(rac_dental_nocode_ag.count()))

    rac_lpp_ag = (spark.read.parquet(path2output + "rac_lpp_ag")
                  .withColumn("poste_ag", psf.when(psf.col("poste").isin(audio), "audioprotheses").
                              when(psf.col("poste").isin(optical), "optique").
                              otherwise(psf.lit("LPP_autre")))
                  .withColumn("acte_quantite", psf.col("acte_quantite").cast(pst.LongType()))
                  )
    if verbose:
        print("Nbre lignes base rac lpp avec rgpt : {}".format(rac_lpp_ag.count()))

    rac_lpp_nocode_ag = (spark.read.parquet(path2output + "rac_lpp_nocode_ag")
                         .withColumn("poste_ag",
                                     psf.when(psf.col("poste").isin(audio), "audioprotheses").
                                     when(psf.col("poste").isin(optical), "optique").
                                     otherwise(psf.lit("LPP_autre")))
                         .withColumn("acte_quantite", psf.col("acte_quantite").cast(pst.LongType()))
                         )
    if verbose:
        print("Nbre lignes base rac lpp sans rgpt : {}".format(rac_lpp_nocode_ag.count()))

    list_var = ['NUM_ENQ', 'poste', 'PRS_PAI_MNT', 'TOT_REM_MNT', 'TOT_REM_BSE',
                'top_cmu', 'EXO_MTF', 'acte_quantite', 'top_acs', 'poste_ag', 'top_exo_mtf', 'ARO_REM_MNT',
                'BEN_AMA_COD', 'dep_lien_ald', 'mnt_rem_lien_ald', 'aro_rem_lien_ald']

    rac_nonlppdental_ag = rac_nonlppdental_ag.select(list_var)
    rac_dental_ag = rac_dental_ag.select(list_var)
    rac_dental_nocode_ag = rac_dental_nocode_ag.select(list_var)
    rac_lpp_ag = rac_lpp_ag.select(list_var)
    rac_lpp_nocode_ag = rac_lpp_nocode_ag.select(list_var)
    rac_sdv = union_all(rac_nonlppdental_ag, rac_dental_ag, rac_dental_nocode_ag, rac_lpp_ag, rac_lpp_nocode_ag)
    if verbose:
        print("Nbre lignes base rac sdv total : {}".format(rac_sdv.count()))

    rac_sdv = rac_sdv.groupBy(psf.col("NUM_ENQ"), psf.col("poste")).agg(agg_dict)
    output_var = ['acte_quantite', 'poste', "poste_ag", 'PRS_PAI_MNT', 'TOT_REM_MNT', 'ARO_REM_MNT', 'top_cmu',
                  'top_acs',
                  'EXO_MTF', 'TOT_REM_BSE', 'top_exo_mtf', 'BEN_AMA_COD', 'dep_lien_ald', 'mnt_rem_lien_ald',
                  'aro_rem_lien_ald']
    input_var = ['sum(acte_quantite)', 'spe_act_x_presta_R', "first(poste_ag)", 'sum(PRS_PAI_MNT)', 'sum(TOT_REM_MNT)',
                 'sum(ARO_REM_MNT)',
                 'max(top_cmu)', 'max(top_acs)', 'first(EXO_MTF)', 'sum(TOT_REM_BSE)', 'max(top_exo_mtf)',
                 'max(BEN_AMA_COD)', 'sum(dep_lien_ald)', 'sum(mnt_rem_lien_ald)', 'sum(aro_rem_lien_ald)']
    mapping = dict(zip(input_var, output_var))
    rac_sdv = rac_sdv.select([psf.col(c).alias(mapping.get(c, c)) for c in rac_sdv.columns])
    return rac_sdv


def add_person_info(rac_sdv, year, path2flat, path2ref, spark, agg_var=None, coalesce_coef=500):
    if agg_var is None:
        agg_var = ["NUM_ENQ", "poste"]
    ald = [41, 43, 45]
    cnam_na = 1600
    if year == 2017:
        ir_iba = (spark.read
                  .parquet(path2flat + str(year)[-2:] + "/single_table/IR_IBA_R" + "/year=" + str(year))
                  .select(["NUM_ENQ", "BEN_NAI_ANN", "BEN_SEX_COD", "BEN_RES_DPT", "BEN_RES_COM", "BEN_DCD_DTE"])

                  )
    elif year == 2016:
        ir_iba = (spark.read
                  .parquet(path2flat + str(year)[-2:] + "/single_table/IR_IBA_R/")
                  .select(["NUM_ENQ", "BEN_NAI_ANN", "BEN_SEX_COD", "BEN_RES_DPT", "BEN_RES_COM", "BEN_DCD_DTE"])

                  )
    else:
        raise ValueError("Manquement année")
    ir_iba = (ir_iba.dropDuplicates()
              .withColumn("top_dcd_annee", psf.when(psf.year(psf.col("BEN_DCD_DTE")) == year, 1).
                          otherwise(0))
              .withColumn("mois_dcd", psf.when(psf.col("top_dcd_annee") == 1, psf.month(psf.col("BEN_DCD_DTE"))).
                          otherwise(0)))

    if year == 2017:
        ir_imb = (spark.read
                  .parquet(path2flat + str(year)[-2:] + "/single_table/IR_IMB_R" + "/year=" + str(year))
                  .select(["NUM_ENQ", "IMB_ALD_NUM", "MED_MTF_COD", "IMB_ALD_DTD", "IMB_ALD_DTF", "IMB_ETM_NAT"])
                  .withColumn("IMB_ALD_DTD", psf.to_timestamp(psf.col("IMB_ALD_DTD"), "dd/MM/yyyy"))
                  .withColumn("IMB_ALD_DTF", psf.to_timestamp(psf.col("IMB_ALD_DTF"), "dd/MM/yyyy"))
                  )
    elif year == 2016:
        ir_imb = (spark.read
                  .parquet(path2flat + str(year)[-2:] + "/single_table/IR_IMB_R/")
                  .select(["NUM_ENQ", "IMB_ALD_NUM", "MED_MTF_COD", "IMB_ALD_DTD", "IMB_ALD_DTF", "IMB_ETM_NAT"])
                  .withColumn("IMB_ALD_DTD", psf.to_timestamp(psf.col("IMB_ALD_DTD"), "dd/MM/yyyy"))
                  .withColumn("IMB_ALD_DTF", psf.to_timestamp(psf.col("IMB_ALD_DTF"), "dd/MM/yyyy"))
                  )
    else:
        raise ValueError("Manquement année")

    ir_imb = ir_imb.filter((psf.year(psf.col("IMB_ALD_DTD")) <= year) &
                           ((psf.year(psf.col("IMB_ALD_DTF")) >= year) | (psf.col("IMB_ALD_DTF").isNull()) |
                            (psf.year(psf.col("IMB_ALD_DTF")) == cnam_na)) &
                           (psf.col("IMB_ETM_NAT").isin(ald)))

    # Type of ALD
    ir_cim = (spark.read.option("sep", ";").option("header", "true").csv(path2ref + "IR_CIM_V.csv")
              .withColumnRenamed("CIM_COD", "MED_MTF_COD"))

    # si IMB_ALD_NUM est renseignée et prend une valeur entre 1 et 32 on la garde, sinon on va chercher l'info     
    # sur l'ALD à partir du code CIM 10 ou du type de prise en charge
    ir_imb = ir_imb.join(ir_cim.select(["MED_MTF_COD", "ALD_030_COD"]), ["MED_MTF_COD"], how="left")
    ir_imb = ir_imb.withColumn("NUM_ALD",
                               psf.when((psf.col("IMB_ALD_NUM") > 0) & (psf.col("IMB_ALD_NUM") <= 32),
                                        psf.col("IMB_ALD_NUM")).
                               when((psf.col("ALD_030_COD") > 0) & (psf.col("ALD_030_COD") <= 30),
                                    psf.col("ALD_030_COD")).
                               when((psf.col("IMB_ETM_NAT") == 43), psf.lit(31)).
                               when((psf.col("IMB_ETM_NAT") == 45),
                                    psf.lit(32)). 
                               otherwise(psf.lit(99)))

    # Regrouper par bénéficiaire - liste des ALD 
    ir_imb_unity = ir_imb.groupBy(psf.col("NUM_ENQ")).agg({"NUM_ALD": "collect_set"})
    ir_imb_unity = ir_imb_unity.withColumnRenamed("collect_set(NUM_ALD)", "NUM_ALD")

    # Compter le nombre d'ALD distinctes par patients
    ir_imb_unity_ct = ir_imb.groupBy(psf.col("NUM_ENQ")).agg(psf.countDistinct(psf.col("NUM_ALD")))
    ir_imb_unity_ct = (ir_imb_unity_ct.withColumnRenamed("count(DISTINCT NUM_ALD)", "nb_ald")
                      .withColumnRenamed("count(NUM_ALD)", "nb_ald"))
    ir_imb_unity = ir_imb_unity.join(ir_imb_unity_ct, ["NUM_ENQ"], how="inner")
    ir_imb_unity = ir_imb_unity.withColumn("top_ald", psf.when(psf.col("nb_ald") > 0, 1).otherwise(0))

    rac_sdv = rac_sdv.join(ir_imb_unity, ["NUM_ENQ"], how="left")
    rac_sdv = rac_sdv.join(ir_iba, ["NUM_ENQ"], how="left")
    rac_sdv = rac_sdv.withColumn("top_ald_exo_mtf",
                                 psf.when((psf.col("top_ald") == 1) & (psf.col("top_exo_mtf") == 1), 1)
                                 .otherwise(0))

    # Aggregation
    rac_sdv = rac_sdv.groupBy(agg_var).agg({"BEN_NAI_ANN": "first",
                                            "BEN_SEX_COD": "first",
                                            "BEN_RES_DPT": "first",
                                            "BEN_RES_COM": "first",
                                            "BEN_DCD_DTE": "first",
                                            "top_dcd_annee": "first",
                                            "mois_dcd": "first",
                                            "top_cmu": "max",
                                            "top_ald": "max",
                                            "PRS_PAI_MNT": "sum",
                                            "TOT_REM_BSE": "sum",
                                            "TOT_REM_MNT": "sum",
                                            "acte_quantite": "sum",
                                            "poste_ag": "first",
                                            "top_acs": "max",
                                            "top_ald_exo_mtf": "max",
                                            "BEN_AMA_COD": "max",
                                            "ARO_REM_MNT": "sum",
                                            "NUM_ALD": "first",
                                            "nb_ald": "first",
                                            "dep_lien_ald": "sum",
                                            "mnt_rem_lien_ald": "sum",
                                            "aro_rem_lien_ald": "sum"})
    rac_sdv = redefine_columns(rac_sdv)
    rac_sdv = rac_sdv.coalesce(coalesce_coef)
    return rac_sdv


def correct_amounts(rac_sdv):
    rac_sdv = rac_sdv.withColumn("aro_rem_lien_ald",
                                 psf.when(psf.col("aro_rem_lien_ald") < 0, psf.lit(0)).
                                 otherwise(psf.col("aro_rem_lien_ald")))

    rac_sdv = rac_sdv.withColumn("ARO_REM_MNT",
                                 psf.when(psf.col("ARO_REM_MNT") < 0, psf.lit(0)).
                                 otherwise(psf.col("ARO_REM_MNT")))
    rac_sdv = rac_sdv.withColumn("TOT_REM_BSE_C",
                                 psf.when(psf.col("TOT_REM_BSE") < 0, - psf.col("TOT_REM_BSE")).
                                 otherwise(psf.col("TOT_REM_BSE")))

    rac_sdv = rac_sdv.withColumn("mnt_rem_lien_ald",
                                 # Cas où la base de remboursement non corrigée était elle aussi négative :
                                 # on inverse signe de TOT_REM_MNT
                                 psf.when((psf.col("mnt_rem_lien_ald") < 0) & (psf.col("TOT_REM_BSE") < 0),
                                          - psf.col("mnt_rem_lien_ald")).
                                 # Cas où la base de remboursement n'est pas négative et inférieure en valeur absolue :
                                 # on inverse signe de TOT_REM_MNT
                                 when((psf.col("mnt_rem_lien_ald") < 0) & (psf.col("TOT_REM_BSE_C") >= 0) & (
                                             -psf.col("mnt_rem_lien_ald") <= psf.col("TOT_REM_BSE_C") *
                                             psf.col("mnt_rem_lien_ald") / psf.col("TOT_REM_MNT")),
                                      - psf.col("mnt_rem_lien_ald")).
                                 # Cas où la base de remboursement n'est pas négative et supérieure en valeur absolue :
                                 # on remplace TOT_REM_MNT par TOT_REM_BSE_C
                                 when((psf.col("mnt_rem_lien_ald") < 0) & (psf.col("TOT_REM_BSE_C") >= 0) & (
                                             -psf.col("mnt_rem_lien_ald") > psf.col("TOT_REM_BSE_C") *
                                             psf.col("mnt_rem_lien_ald") / psf.col("TOT_REM_MNT")),
                                      psf.col("TOT_REM_BSE_C") * psf.col("mnt_rem_lien_ald") / psf.col("TOT_REM_MNT")).
                                 otherwise(psf.col("mnt_rem_lien_ald")))

    rac_sdv = rac_sdv.withColumn("TOT_REM_MNT",
                                 # Cas où la base de remboursement non corrigée était elle aussi négative :
                                 # on inverse signe de TOT_REM_MNT
                                 psf.when((psf.col("TOT_REM_MNT") < 0) & (psf.col("TOT_REM_BSE") < 0),
                                          - psf.col("TOT_REM_MNT")).
                                 # Cas où la base de remboursement n'est pas négative et inférieure en valeur absolue :
                                 # on inverse signe de TOT_REM_MNT
                                 when((psf.col("TOT_REM_MNT") < 0) & (psf.col("TOT_REM_BSE_C") >= 0) & (
                                             -psf.col("TOT_REM_MNT") <= psf.col("TOT_REM_BSE_C")),
                                      - psf.col("TOT_REM_MNT")).
                                 # Cas où la base de remboursement n'est pas négative et supérieure en valeur absolue :
                                 # on remplace TOT_REM_MNT par TOT_REM_BSE_C
                                 when((psf.col("TOT_REM_MNT") < 0) & (psf.col("TOT_REM_BSE_C") >= 0) & (
                                             -psf.col("TOT_REM_MNT") > psf.col("TOT_REM_BSE_C")),
                                      psf.col("TOT_REM_BSE_C")).
                                 otherwise(psf.col("TOT_REM_MNT")))

    rac_sdv = rac_sdv.drop("TOT_REM_BSE")
    rac_sdv = rac_sdv.withColumnRenamed("TOT_REM_BSE_C", "TOT_REM_BSE")
    rac_sdv = rac_sdv.withColumn("TOT_REM_BSE",
                                 psf.when(psf.col("TOT_REM_BSE") < psf.col("TOT_REM_MNT"), psf.col("TOT_REM_MNT")).
                                 otherwise(psf.col("TOT_REM_BSE")))

    # on corrige quand il y a eu erreur et que TOT_BSE-TOT_REM = -18, cela signifie que la pf18 n'est pas prise en
    # compte dans la base de remboursement
    rac_sdv = rac_sdv.withColumn("TOT_REM_BSE_C",
                                 psf.when((psf.col("TOT_REM_BSE") - psf.col("TOT_REM_MNT")) == -18,
                                          psf.col("TOT_REM_BSE") + 18).
                                 otherwise(psf.col("TOT_REM_BSE")))

    # Participations financières
    rac_sdv = rac_sdv.withColumn("TOT_REM_BSE_C",
                                 psf.when((psf.col("TOT_REM_BSE") - psf.col("TOT_REM_MNT")) == -1,
                                          psf.col("TOT_REM_BSE") + 1).
                                 otherwise(psf.col("TOT_REM_BSE_C")))

    rac_sdv = rac_sdv.withColumn("TOT_REM_BSE_C",
                                 psf.when(((psf.col("TOT_REM_BSE") - psf.col("TOT_REM_MNT")) == -2) &
                                          (psf.col("poste") == "transport"), psf.col("TOT_REM_BSE") + 2).
                                 otherwise(psf.col("TOT_REM_BSE_C")))

    rac_sdv = rac_sdv.withColumn("TOT_REM_BSE_C",
                                 psf.when(((psf.col("TOT_REM_BSE") - psf.col("TOT_REM_MNT")) < -10000),
                                          psf.col("TOT_REM_MNT")).
                                 otherwise(psf.col("TOT_REM_BSE_C")))

    # Remplacer colonne "TOT_REM_BSE" par "TOT_REM_BSE_C"
    rac_sdv = rac_sdv.drop("TOT_REM_BSE")
    rac_sdv = rac_sdv.withColumnRenamed("TOT_REM_BSE_C", "TOT_REM_BSE")


    rac_sdv = rac_sdv.withColumn("dep_lien_ald_c",
                                 psf.when(
                                     (psf.col("dep_lien_ald") > 0) & (psf.col("PRS_PAI_MNT") < psf.col("TOT_REM_BSE")),
                                     psf.col("TOT_REM_BSE") * psf.col("dep_lien_ald") / psf.col("PRS_PAI_MNT")).
                                 otherwise(psf.col("dep_lien_ald")))
    rac_sdv = rac_sdv.withColumn("dep_lien_ald", psf.col("dep_lien_ald_c"))

    # Décision : remplacer "PRS_PAI_MNT" par "TOT_REM_BSE"
    rac_sdv = rac_sdv.withColumn("PRS_PAI_MNT",
                                 psf.when(psf.col("PRS_PAI_MNT") < psf.col("TOT_REM_BSE"), psf.col("TOT_REM_BSE")).
                                 otherwise(psf.col("PRS_PAI_MNT")))
    # Gestion des dépenses < remboursements 

    # Quand la dépense est inférieure au montant remboursé, on remplace la dépense par le montant remboursé +
    # participations supplémentaires, on remplace
    # la dépense par le montant remboursé + participations supplémentaires (hors part OC ACS)
    rac_sdv = rac_sdv.withColumn("dep_lien_ald_c",
                                 psf.when((psf.col("dep_lien_ald") > 0) & (
                                             psf.col("PRS_PAI_MNT") < psf.col("TOT_REM_MNT") + psf.col("ARO_REM_MNT")),
                                          (psf.col("TOT_REM_MNT") + psf.col("ARO_REM_MNT")) * psf.col(
                                              "dep_lien_ald") / psf.col("PRS_PAI_MNT")).
                                 otherwise(psf.col("dep_lien_ald")))
    rac_sdv = rac_sdv.withColumn("dep_lien_ald", psf.col("dep_lien_ald_c"))
    # Gestion des dépenses < remboursements 

    # Quand la dépense est inférieure au montant remboursé, on remplace la dépense par le montant remboursé +
    # participations supplémentaires, on remplace
    # la dépense par le montant remboursé + participations supplémentaires (hors part OC ACS)
    rac_sdv = rac_sdv.withColumn("PRS_PAI_MNT",
                                 psf.when(psf.col("PRS_PAI_MNT") < psf.col("TOT_REM_MNT") + psf.col("ARO_REM_MNT"),
                                          psf.col("TOT_REM_MNT") + psf.col("ARO_REM_MNT")).
                                 otherwise(psf.col("PRS_PAI_MNT")))
    rac_sdv = rac_sdv.withColumn("part_sup_bse",
                                 psf.when(psf.col("TOT_REM_BSE") - psf.col("TOT_REM_MNT") - psf.col("ARO_REM_MNT") >= 0,
                                          psf.col("ARO_REM_MNT")).
                                 otherwise(psf.col("TOT_REM_BSE") - psf.col("TOT_REM_MNT")))
    rac_sdv = rac_sdv.withColumn("part_sup_depass",
                                 psf.when(psf.col("TOT_REM_BSE") - psf.col("TOT_REM_MNT") - psf.col("ARO_REM_MNT") >= 0,
                                          0).
                                 otherwise(psf.col("ARO_REM_MNT") - psf.col("TOT_REM_BSE") + psf.col(
                                     "TOT_REM_MNT")))  # i.e."ARO_REM_MNT" - "part_sup_bse"
    # Ne garder qu'une décimale
    rac_sdv = rac_sdv.withColumn("part_sup_bse", psf.round(psf.col("part_sup_bse"), 1))
    rac_sdv = rac_sdv.withColumn("part_sup_depass", psf.round(psf.col("part_sup_depass"), 1))

    rac_sdv = rac_sdv.withColumn("rac_opposable",
                                 psf.col("TOT_REM_BSE") - psf.col("TOT_REM_MNT") - psf.col("part_sup_bse"))
    rac_sdv = rac_sdv.withColumn("rac", psf.col("PRS_PAI_MNT") - psf.col("TOT_REM_MNT"))
    rac_sdv = rac_sdv.withColumn("rac_amo_sup",
                                 psf.col("PRS_PAI_MNT") - psf.col("TOT_REM_MNT") - psf.col("ARO_REM_MNT"))
    rac_sdv = rac_sdv.withColumn("rac_lien_ald",
                                 psf.col("dep_lien_ald") - psf.col("mnt_rem_lien_ald") - psf.col("aro_rem_lien_ald"))
    rac_sdv = rac_sdv.withColumn("depass_c",
                                 psf.col("PRS_PAI_MNT") - psf.col("TOT_REM_BSE") - psf.col("part_sup_depass"))

    rac_sdv = (rac_sdv.withColumn("rac_opposable", psf.round(psf.col("rac_opposable"), 1))
               .withColumn("rac", psf.round(psf.col("rac"), 1))
               .withColumn("rac_amo_sup", psf.round(psf.col("rac_amo_sup"), 1))
               .withColumn("rac_lien_ald", psf.round(psf.col("rac_lien_ald"), 1))
               .withColumn("depass_c", psf.round(psf.col("depass_c"), 1)))
    rac_sdv = rac_sdv.withColumn("depass_c", psf.when(psf.col("depass_c") < 0, 0).otherwise(psf.col("depass_c")))

    rac_sdv = rac_sdv.withColumn("rac_opposable",
                                 psf.when(psf.col("rac_opposable") < 0, 0).otherwise(psf.col("rac_opposable")))
    return rac_sdv


def correct_categ(rac_sdv, private_institutions):
    rac_sdv = (rac_sdv.withColumn("poste_ag", 
                                  psf.when(psf.col("poste").isin(private_institutions), "etab_priv").
                                 otherwise(psf.col("poste_ag")))
        .withColumn("poste", psf.when(psf.col("poste").isin(["Pharma__cliniq", "Pharma__techniq"]), "pharma").
                                 otherwise(psf.col("poste")))
        .withColumn("poste", psf.when(psf.col("poste").isin(["ssr_autre_etab_priv"]), "ssr_etab_priv_lucratif").
                                 otherwise(psf.col("poste")))
        .withColumn("poste", psf.when(psf.col("poste").isin(["mco_autre_etab_priv"]), "mco_etab_priv_lucratif").
                                 otherwise(psf.col("poste")))
        .withColumn("poste", psf.when(psf.col("poste").isin(["autre_autre_etab_priv"]), "autre_etab_priv_lucratif").
                                 otherwise(psf.col("poste")))
        .withColumn("poste", psf.when(psf.col("poste").isin(["cures_thermales_transp"]), "transport").
                                 otherwise(psf.col("poste")))
        .withColumn("poste", psf.when(psf.col("poste").isin(["Transporteur_cliniq"]), "transport").
                                 otherwise(psf.col("poste")))
        .withColumn("poste", psf.when(psf.col("poste").isin(["Transporteur_techniq"]), "transport").
                                 otherwise(psf.col("poste")))
        .withColumn("poste_ag", psf.when(psf.col("poste").isin(["orthodontie"]), "dentaire").
                                 otherwise(psf.col("poste_ag"))))
    return rac_sdv


def define_age(rac_sdv, year, spark):
    rac_sdv = rac_sdv.withColumn("age", psf.lit(year) - psf.col("BEN_NAI_ANN"))
    rac_sdv = rac_sdv.withColumn("age", psf.when(psf.col("age") > 115, None).
                                 when(psf.col('age') < 0, None).
                                 otherwise(psf.col("age")))
    rac_sdv = rac_sdv.withColumn("BEN_AMA_COD", psf.when(psf.col("BEN_AMA_COD").isin(["9999", "0"]), None).
                                 when((psf.col("BEN_AMA_COD") > 130) & (psf.col("BEN_AMA_COD") < 1012), 0).
                                 when((psf.col("BEN_AMA_COD") < 1024) & (psf.col("BEN_AMA_COD") >= 1012), 1).
                                 when(psf.col("BEN_AMA_COD") == 1024, 2).
                                 otherwise(psf.col("BEN_AMA_COD")))
    rac_sdv = rac_sdv.withColumn("age", psf.when(psf.col("age").isNull(), psf.col("BEN_AMA_COD")).
                                 otherwise(psf.col("age")))

    rac_sdv = rac_sdv.withColumn("classe_age", psf.when(psf.col("age") < 6, "00 - 5 ans").
                                 when((psf.col("age") < 11) & (psf.col("age") >= 6), "06 - 10 ans").
                                 when((psf.col("age") < 16) & (psf.col("age") >= 11), "11 - 15 ans").
                                 when((psf.col("age") < 21) & (psf.col("age") >= 16), "16 - 20 ans").
                                 when((psf.col("age") < 26) & (psf.col("age") >= 21), "21 - 25 ans").
                                 when((psf.col("age") < 31) & (psf.col("age") >= 26), "26 - 30 ans").
                                 when((psf.col("age") < 36) & (psf.col("age") >= 31), "31 - 35 ans").
                                 when((psf.col("age") < 41) & (psf.col("age") >= 36), "36 - 40 ans").
                                 when((psf.col("age") < 46) & (psf.col("age") >= 41), "41 - 45 ans").
                                 when((psf.col("age") < 51) & (psf.col("age") >= 46), "46 - 50 ans").
                                 when((psf.col("age") < 56) & (psf.col("age") >= 51), "51 - 55 ans").
                                 when((psf.col("age") < 61) & (psf.col("age") >= 56), "56 - 60 ans").
                                 when((psf.col("age") < 66) & (psf.col("age") >= 61), "61 - 65 ans").
                                 when((psf.col("age") < 71) & (psf.col("age") >= 66), "66 - 70 ans").
                                 when((psf.col("age") < 76) & (psf.col("age") >= 71), "71 - 75 ans").
                                 when((psf.col("age") < 81) & (psf.col("age") >= 76), "76 - 80 ans").
                                 when((psf.col("age") < 86) & (psf.col("age") >= 81), "81 - 85 ans").
                                 when((psf.col("age") < 91) & (psf.col("age") >= 86), "86 - 90 ans").
                                 when((psf.col("age") >= 91), "plus de 91 ans").
                                 otherwise(None))

    return rac_sdv

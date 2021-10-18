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
import numpy as np
from pyspark.ml.feature import Bucketizer
from .utils import union_all
from .hospit import _load_ir_imb_unity
from .hospit import _load_ir_orc


def import_sdv_hospit(path2flat, path2data_sdv, path2data_hospit, path2_ir_orc, path2ref, year, list_output_var, spark):
    print("SDV - ES PRIV")
    rac_sdv = (spark.read.parquet(path2data_sdv)
               .withColumnRenamed("PRS_PAI_MNT", "dep_tot")
               .withColumnRenamed("TOT_REM_MNT", "remb_am")
               .withColumnRenamed("TOT_REM_BSE", "tot_rem_bse")
               .withColumn("MNT_TM", psf.lit(None))
               .withColumn("MNT_PF", psf.lit(None))
               .withColumn("MNT_FJ", psf.lit(None))
               .withColumn("NBSEJ_CPLT_COURT", psf.lit(None))
               .withColumn("nb_sejour", psf.lit(None))
               .withColumn("NBSEJ_CPLT_LONG", psf.lit(None))
               .withColumn("NBSEJ_PARTL_COURT", psf.lit(None))
               .withColumn("NBSEJ_PARTL_LONG", psf.lit(None))
               .withColumn("NBJ_CPLT", psf.lit(None))
               .withColumn("NBJ_PARTL", psf.lit(None)))

    rac_sdv = rac_sdv.select(list_output_var)

    ir_orc_r = _load_ir_orc(path2_ir_orc, year, spark)

    ir_iba = (spark.read.parquet(path2flat + "single_table/IR_IBA_R")
              .select(["NUM_ENQ", "BEN_NAI_ANN", "BEN_SEX_COD", "BEN_RES_DPT", "BEN_RES_COM"]))

    ir_imb_unity = _load_ir_imb_unity(path2flat, path2ref, year, spark)

    # Hospitalization tables
    # MCO
    print("MCO")
    rac_mco = (spark.read.parquet(path2data_hospit + "_MCO")
               .withColumn("poste", psf.concat(psf.lit("mco_"), psf.col("poste")))
               .filter(psf.col("poste_ag").isin("etab_public_mco", "etab_public_mco_ace"))
               .withColumn("tot_rem_bse", psf.col("dep_tot"))
               .withColumn("acte_quantite", psf.lit(None))
               .withColumn("age", psf.lit(None))
               .withColumn("classe_age", psf.lit(None))
               .withColumn("top_acs", psf.lit(None))
               .withColumn("NBSEJ_PARTL_LONG", psf.lit(None))
               .withColumnRenamed("NBSEJ_PARTL", "NBSEJ_PARTL_COURT")
               .withColumn('depass_c', psf.lit(0))
               .withColumn("rac_opposable", psf.col("rac_amo_sup"))
               .drop('BEN_NAI_ANN').drop('BEN_SEX_COD').drop('BEN_RES_DPT').drop('BEN_RES_COM')
               .join(ir_iba, ["NUM_ENQ"], how="left")
               .drop("top_cmu")
               .join(ir_orc_r, ["NUM_ENQ"], how="left")
               .withColumn("top_cmu", psf.when(psf.col("top_cmu") == 1, 1).otherwise(0))
               .drop("NUM_ALD", "nb_ald", "top_ald")
               .join(ir_imb_unity, ["NUM_ENQ"], how="left")
               .withColumn("top_ald_exo_mtf", psf.col("top_ald"))
               .withColumn("poste_hospit", psf.col("poste_ag"))
               )

    rac_mco = rac_mco.select(list_output_var)

    # SSR
    print("SSR")
    rac_ssr = (spark.read.parquet(path2data_hospit + "_SSR")
               .withColumn("poste", psf.concat(psf.lit("ssr_"), psf.col("poste")))
               .filter(psf.col("poste_ag").isin(["etab_public_ssr", "etab_public_ssr_ace"]))
               .withColumn('acte_quantite', psf.lit(None))
               .withColumn('rac_opposable', psf.col("rac_amo_sup"))
               .withColumn('age', psf.lit(None))
               .withColumn('classe_age', psf.lit(None))
               .withColumn('top_acs', psf.lit(None))
               .withColumn('tot_rem_bse', psf.col("dep_tot"))
               .withColumn('depass_c', psf.lit(0))
               .drop('BEN_NAI_ANN').drop('BEN_SEX_COD').drop('BEN_RES_DPT').drop('BEN_RES_COM')
               .join(ir_iba, ["NUM_ENQ"], how="left")
               .drop("top_cmu")
               .join(ir_orc_r, ["NUM_ENQ"], how="left")
               .withColumn("top_cmu", psf.when(psf.col("top_cmu") == 1, 1).otherwise(0))
               .withColumn("top_ald_exo_mtf", psf.lit(None))
               .drop("NUM_ALD", "nb_ald", "top_ald")
               .join(ir_imb_unity, ["NUM_ENQ"], how="left")
               .withColumn("top_ald_exo_mtf", psf.col("top_ald"))
               .withColumn("poste_hospit", psf.col("poste_ag"))
               )
    rac_ssr = rac_ssr.select(list_output_var)

    # HAD
    print("HAD")
    rac_had = (spark.read.parquet(path2data_hospit + "_HAD")
               .withColumn("poste", psf.concat(psf.lit("had_"), psf.col("poste")))
               .filter(psf.col("poste_ag") == "etab_public_had")
               .withColumn('acte_quantite', psf.lit(None))
               .withColumn('rac_opposable', psf.col("rac_amo_sup"))
               .withColumn('age', psf.lit(None))
               .withColumn('classe_age', psf.lit(None))
               .withColumn('top_acs', psf.lit(None))
               .withColumn('tot_rem_bse', psf.col("dep_tot"))
               .withColumn('top_ald_exo_mtf', psf.lit(None))
               .withColumn("MNT_FJ", psf.lit(None))
               .withColumn("NBJ_CPLT", psf.lit(None))
               .withColumn("NBSEJ_CPLT_COURT", psf.lit(None))
               .withColumn("NBSEJ_CPLT_LONG", psf.lit(None))
               .withColumn('depass_c', psf.lit(0))
               .drop('BEN_NAI_ANN').drop('BEN_SEX_COD').drop('BEN_RES_DPT').drop('BEN_RES_COM')
               .join(ir_iba, ["NUM_ENQ"], how="left")
               .drop("top_cmu")
               .join(ir_orc_r, ["NUM_ENQ"], how="left")
               .withColumn("top_cmu", psf.when(psf.col("top_cmu") == 1, 1).otherwise(0))
               .drop("NUM_ALD", "nb_ald", "top_ald")
               .join(ir_imb_unity, ["NUM_ENQ"], how="left")
               .withColumn("top_ald_exo_mtf", psf.col("top_ald"))
               .withColumn("poste_hospit", psf.col("poste_ag"))
               )

    rac_had = rac_had.select(list_output_var)

    # PSY
    print("PSY")
    rac_rip = (spark.read.parquet(path2data_hospit + "_RIP")
               .withColumn("poste", psf.concat(psf.lit("rip_"), psf.col("poste")))
               .filter(psf.col("poste_ag") == "etab_public_rip")
               .withColumn('acte_quantite', psf.lit(None))
               .withColumn('rac_opposable', psf.col("rac_amo_sup"))
               .withColumn('age', psf.lit(None))
               .withColumn('classe_age', psf.lit(None))
               .withColumn('top_acs', psf.lit(None))
               .withColumn('tot_rem_bse', psf.col("dep_tot"))
               .withColumn('depass_c', psf.lit(0))
               .drop('BEN_NAI_ANN').drop('BEN_SEX_COD').drop('BEN_RES_DPT').drop('BEN_RES_COM')
               .join(ir_iba, ["NUM_ENQ"], how="left")
               .drop("top_cmu")
               .join(ir_orc_r, ["NUM_ENQ"], how="left")
               .withColumn("top_cmu", psf.when(psf.col("top_cmu") == 1, 1).otherwise(0))
               .withColumn("top_ald_exo_mtf", psf.lit(None))
               .drop("NUM_ALD", "nb_ald", "top_ald")
               .join(ir_imb_unity, ["NUM_ENQ"], how="left")
               .withColumn("top_ald_exo_mtf", psf.col("top_ald"))
               .withColumn("poste_hospit", psf.col("poste_ag"))
               )
    rac_rip = rac_rip.select(list_output_var)

    data_rac = union_all(rac_sdv, rac_mco, rac_ssr, rac_had, rac_rip)
    return data_rac


def define_unique_ald(path2flat, path2ref, data_rac, year, spark):
    ir_imb_unity = _load_ir_imb_unity(path2flat, path2ref, year, spark)
    data_rac = data_rac.drop("NUM_ALD", "nb_ald", "top_ald")
    data_rac = data_rac.join(ir_imb_unity, ["NUM_ENQ"], how="left")
    return data_rac


def redefine_categ_health_accounts(data_rac, path2health_account_categ, path2result, spark):
    categ = (spark
             .read.option("sep", ",").option("header", "true")
             .csv(path2health_account_categ)
             .withColumnRenamed("Poste_base_rac", "poste"))

    data_rac = data_rac.join(psf.broadcast(categ), ["poste"], how="left")

    # On change poste agrégé en etab priv honoraires pour les specialistes et auxiliaires en etablissements prives
    data_rac = data_rac.withColumn("poste_ag",
                                   psf.when(psf.col("Poste_cns") == "consos_soins_hospitaliers_priv_honoraires",
                                            "etab_priv_honoraires").
                                   otherwise(psf.col("poste_ag")))

    # LPP, liste en sus, etc sont déjà dans etab priv mais pas dans la partie honoraires pour les CNS
    data_rac = data_rac.withColumn("poste_ag",
                                   psf.when(psf.col("poste") == "Labo_priv", "etab_priv_honoraires").
                                   otherwise(psf.col("poste_ag")))

    # Separating liberal and hospital
    data_rac = data_rac.withColumn("separation_sdv_hospit",
                                   psf.when(psf.col("poste_ag").isin(["etab_priv", "etab_public_had",
                                                                      "etab_priv_honoraires", "etab_public_mco",
                                                                      "etab_public_ssr", "etab_public_rip"]), "hospit")
                                   .when((psf.col("poste_ag") == "etab_public_mco_ace") &
                                         (psf.col("poste") == "mco_ace_urgences"), "hospit")
                                   .when((psf.col("poste_ag") == "etab_public_mco_ace") &
                                         (psf.col("poste") == "mco_ace_Med_SUS"), "hospit").
                                   otherwise("sdv"))  # Keep ACE in liberal

    data_rac = data_rac.withColumn("poste_ag",
                                   psf.when((psf.col("poste_ag") == "etab_public_mco_ace") &
                                            (psf.col("poste") == "mco_ace_urgences"),
                                            "etab_public_mco")
                                   .when((psf.col("poste_ag") == "etab_public_mco_ace") &
                                         (psf.col("poste") == "mco_ace_Med_SUS"),
                                         "etab_public_mco")
                                   .when((psf.col("poste_ag") == "etab_public_mco_ace") &
                                         (~psf.col("poste").isin(["mco_ace_urgences", "mco_ace_Med_SUS"])),
                                         "soins_ville")
                                   .when((psf.col("poste_ag") == "etab_public_ssr_ace"),
                                         "soins_ville")
                                   .otherwise(psf.col("poste_ag")))

    # Poste_racine: regroup Private and Public institutions
    data_rac = data_rac.withColumn("poste_racine",
                                   psf.when(psf.col("poste_ag").isin("etab_public_mco",
                                                                     "etab_public_ssr",
                                                                     "etab_public_had",
                                                                     "etab_public_rip"),
                                            psf.lit("etab_public"))
                                   .when(psf.col("poste_ag").isin("etab_priv",
                                                                  "etab_priv_honoraires"),
                                         psf.lit("etab_priv"))
                                   .otherwise(psf.col("poste_ag")))

    data_rac = data_rac.withColumn("poste_semi_ag",
                                   psf.when(psf.col("poste_ag").isin("audioprotheses", "dentaire", "optique",
                                                                     "etab_public_mco",
                                                                     "etab_public_ssr", "etab_public_had",
                                                                     "etab_public_rip"),
                                            psf.col("poste_ag")).
                                   when(psf.col("poste_ag").isin("etab_priv", "etab_priv_honoraires"),
                                        psf.lit("etab_priv")).
                                   when(psf.col("poste_ag") == "soins_ville", psf.col("Poste_cns")).
                                   otherwise(psf.col("poste_ag")))
    # Puis regrouper certains de ces postes 
    data_rac = data_rac.withColumn("poste_semi_ag",
                                   psf.when(psf.col("poste_semi_ag").isin("conso_cures_thermales",
                                                                          "conso_autres_biens_medicaux"),
                                            psf.lit("autres_soins_ville")).
                                   when(psf.col("poste_semi_ag").isin("consos_soins_hospitaliers_priv"),
                                        psf.lit("etab_priv")).
                                   when(psf.col("poste_semi_ag").isin("conso_soins_infirmier",
                                                                      "conso_autres_soins_auxi",
                                                                      "conso_sages_femmes"),
                                        psf.lit("auxilliaires")).
                                   when(psf.col("poste").isin("mco_Omni_cliniq", "ssr_Omni_cliniq"),
                                        psf.lit("conso_med_generalistes")).
                                   when((psf.col("poste_semi_ag") == "consos_soins_hospitaliers_publics") &
                                        (~psf.col("poste").isin("mco_Omni_cliniq", "ssr_Omni_cliniq")),
                                        psf.lit("conso_med_specialistes")).
                                   otherwise(psf.col("poste_semi_ag")))
    # Repêchage des postes semi_ag manquants
    data_rac = data_rac.withColumn("poste_semi_ag",
                                   psf.when(psf.col("poste").isin("Sage_femme_techniq", "Orthophoniste_techniq"),
                                            psf.lit("auxilliaires")).
                                   otherwise(psf.col("poste_semi_ag")))
    data_rac = data_rac.withColumn("poste_semi_ag",
                                   psf.when(psf.col("poste").isin("prevention"), psf.lit("prevention")).
                                   otherwise(psf.col("poste_semi_ag")))
    data_rac = data_rac.withColumn("poste_CNS",
                                   psf.when(psf.col("poste").isin("Sage_femme_techniq"), psf.lit("conso_sages_femmes")).
                                   otherwise(psf.col("poste_CNS")))

    return data_rac


def _correct_geography(path2flat, path2cor_com, path2cor_postx, data_rac, year, spark):
    ir_iba = (spark.read.parquet(path2flat + "single_table/IR_IBA_R")
              .select(["NUM_ENQ", "BEN_RES_DPT", "BEN_RES_COM", "ORG_AFF_BEN"])
              .withColumn("regime", psf.substring(psf.col("ORG_AFF_BEN"), 1, 2))
              .withColumn("dpt", psf.concat_ws("", psf.lit("0"), psf.substring(psf.col("ORG_AFF_BEN"), 4, 2)))
              .withColumn("ncar_benresdpt", psf.length(psf.col("BEN_RES_DPT")))
              .withColumn("code_dpt_corr",
                          psf.when((psf.col("BEN_RES_DPT").isin(['000', '099', '999', '201', '202'])) |
                                   (psf.col("ncar_benresdpt") == 2), psf.col("dpt")).
                          otherwise(psf.col("BEN_RES_DPT")))
              .withColumn("code_com_insee_0",
                          # cas de commune inconnue
                          psf.when(psf.col("BEN_RES_COM").isin(['000', '099', '999']), None).
                          # cas général
                          when((~psf.col("code_dpt_corr").isin(['209'])) &
                               (~psf.substring(psf.col("code_dpt_corr"), 1, 2).isin(['97'])),
                               psf.concat(psf.substring(psf.col("code_dpt_corr"), 2, 3),
                                          psf.col("BEN_RES_COM"))).
                          # Corse
                          when(psf.col('code_dpt_corr') == '209',
                               psf.concat(psf.substring(psf.col("code_dpt_corr"), 1, 2),
                                          psf.col("BEN_RES_COM"))).
                          # DOM pour MSA et RSI
                          when(psf.substring(psf.col("code_dpt_corr"), 1, 2).isin(['97']),
                               psf.concat(psf.substring(psf.col("code_dpt_corr"), 1, 2),
                                          psf.col("BEN_RES_COM"))).
                          otherwise(None)))
    corr = (spark.read.option("header", "true")
            .option("sep", ";")
            .csv(path2cor_com)
            .withColumnRenamed("depcom", "code_com_insee_0")
            .withColumnRenamed("depcom_corr", "code_com_insee_1")
            )

    ir_iba = (ir_iba.join(corr, ["code_com_insee_0"], how="left")
              .withColumn("code_com_insee_1",
                          psf.when(psf.col("code_com_insee_1").isNotNull(), psf.col("code_com_insee_1")).
                          otherwise(psf.col("code_com_insee_0"))))

    postx = (spark.read.option("header", "true")
             .option("sep", ";")
             .csv(path2cor_postx)
             .withColumnRenamed("CODE_JOINTURE", "code_com_insee_0")
             .withColumnRenamed("CODE_INSEE", "code_com_insee_2")
             )
    ir_iba = (ir_iba.join(postx.select(["code_com_insee_0", "code_com_insee_2"]), ["code_com_insee_0"], how="left")
              .withColumn("code_com_insee_2",
                          psf.when(psf.col("code_com_insee_2").isNotNull(), psf.col("code_com_insee_2")).
                          otherwise(psf.col("code_com_insee_1")))
              .withColumn("code_com_insee",
                          psf.when(psf.col("code_com_insee_1").isNotNull(), psf.col("code_com_insee_1")).
                          when(psf.col("code_com_insee_2").isNotNull(), psf.col("code_com_insee_2")).
                          otherwise(psf.col("code_com_insee_0"))))

    data_rac = (data_rac.join(ir_iba.select(['NUM_ENQ', 'code_com_insee_2', 'code_dpt_corr']), ["NUM_ENQ"], how="left")
                .withColumnRenamed("code_com_insee_2", "code_com_insee")
                .withColumnRenamed("code_dpt_corr", "code_res_dpt"))
    return data_rac


def _separate_ald(data_rac, year, spark):
    # Construction de quatre groupes d'ALD
    # Maladies cardio-neurovasculaire
    data_rac = data_rac.withColumn("cardio_neurovasc",
                                   psf.when((psf.col("top_ald") == 1) &
                                            (psf.array_contains(psf.col("NUM_ALD"), '1') |
                                             psf.array_contains(psf.col("NUM_ALD"), '3') |
                                             psf.array_contains(psf.col("NUM_ALD"), '5') |
                                             psf.array_contains(psf.col("NUM_ALD"), '12') |
                                             psf.array_contains(psf.col("NUM_ALD"), '13')), 1).
                                   otherwise(0))
    # Diabète
    data_rac = data_rac.withColumn("diabete",
                                   psf.when((psf.col("top_ald") == 1) &
                                            (psf.array_contains(psf.col("NUM_ALD"), '8')), 1).
                                   otherwise(0))
    # Tumeurs malignes
    data_rac = data_rac.withColumn("tumeurs",
                                   psf.when((psf.col("top_ald") == 1) &
                                            (psf.array_contains(psf.col("NUM_ALD"), '30')), 1).
                                   otherwise(0))
    # Affections psychiatriques de longue durée 
    data_rac = data_rac.withColumn("affection_psy",
                                   psf.when((psf.col("top_ald") == 1) &
                                            (psf.array_contains(psf.col("NUM_ALD"), '23')), 1).
                                   otherwise(0))

    # Ajouter une unique variable indiquant le groupe d'ALD 
    data_rac = data_rac.withColumn("groupe_ALD",
                                   psf.when((psf.col('cardio_neurovasc') == 1) &
                                            (psf.col('diabete') == 0) &
                                            (psf.col('tumeurs') == 0) & (psf.col('affection_psy') == 0),
                                            psf.lit("cardio_neurovasc")).
                                   when((psf.col('cardio_neurovasc') == 0) & (psf.col('diabete') == 1) &
                                        (psf.col('tumeurs') == 0) & (psf.col('affection_psy') == 0),
                                        psf.lit("diabete")).
                                   when((psf.col('cardio_neurovasc') == 0) & (psf.col('diabete') == 0) &
                                        (psf.col('tumeurs') == 1) & (psf.col('affection_psy') == 0),
                                        psf.lit("tumeurs")).
                                   when((psf.col('cardio_neurovasc') == 0) & (psf.col('diabete') == 0) &
                                        (psf.col('tumeurs') == 0) & (psf.col('affection_psy') == 1),
                                        psf.lit("affection_psy")).
                                   when((psf.col('cardio_neurovasc') == 1) | (psf.col('diabete') == 1) |
                                        (psf.col('tumeurs') == 1) | (psf.col('affection_psy') == 1),
                                        psf.lit("polypath_top4")).
                                   when(psf.col("top_ald") == 1, psf.lit("autres_ald")).
                                   otherwise(psf.lit("non_ald")))
    data_rac = data_rac.withColumn("type_ALD",
                                   psf.when((psf.col("top_ald") == 1) & (psf.col("nb_ald") > 1), psf.lit("poly_ald")).
                                   when((psf.col("top_ald") == 1) &
                                        (psf.array_contains(psf.col("NUM_ALD"), '99')),
                                        psf.lit("poly_ald")).
                                   when((psf.col("top_ald") == 1) &
                                        (psf.col("nb_ald") == 1), psf.lit("mono_ald")).
                                   otherwise(psf.lit("non_ald")))
    return data_rac


def correct_indiv_charac(path2flat, path2csv, path2cor_com, path2cor_postx, data_rac, year, spark):
    data_rac = data_rac.withColumn("age", psf.when(psf.col("age").isNull(), year - psf.col("BEN_NAI_ANN"))
                                   .otherwise(psf.col("age")))

    data_rac = data_rac.withColumn("age",
                                   psf.when(psf.col("age") > 115, None).
                                   when(psf.col('age') < 0, None).
                                   otherwise(psf.col("age")))
    data_rac = data_rac.withColumn("classe_age", psf.when(psf.col("age") < 6, "00 - 5 ans").
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

    data_rac = data_rac.withColumn("classe_age_10",
                                   psf.when((psf.col("classe_age").isin(["00 - 5 ans", "06 - 10 ans"])),
                                            "00 - 10 ans").
                                   when((psf.col("classe_age").isin(["11 - 15", "16 - 20 ans"])),
                                        "11 - 20 ans").
                                   when((psf.col("classe_age").isin(["21 - 25 ans", "26 - 30 ans"])),
                                        "21 - 30 ans").
                                   when((psf.col("classe_age").isin(["31 - 35 ans", "36 - 40 ans"])),
                                        "31 - 40 ans").
                                   when((psf.col("classe_age").isin(["41 - 45 ans", "46 - 50 ans"])),
                                        "41 - 50 ans").
                                   when((psf.col("classe_age").isin(["51 - 55 ans", "56 - 60 ans"])),
                                        "51 - 60 ans").
                                   when((psf.col("classe_age").isin(["61 - 65 ans", "66 - 70 ans"])),
                                        "61 - 70 ans").
                                   when((psf.col("classe_age").isin(["71 - 75 ans", "76 - 80 ans"])),
                                        "71 - 80 ans").
                                   when((psf.col("classe_age").isin(["81 - 85 ans", "86 - 90 ans"])),
                                        "81 - 90 ans").
                                   when((psf.col("classe_age").isin(["plus de 91 ans"])),
                                        "plus de 90 ans").
                                   otherwise(None))
    # CMU, ACS
    ir_orc_r = (spark.read.option("sep", ";")
                .option("header", "true")
                .csv(path2csv + 'IR_ORC_R.CSV')
                .withColumn("MLL_CTA_DSD", psf.to_timestamp(psf.col("MLL_CTA_DSD"), "dd/MM/yyyy"))
                .withColumn("MLL_CTA_DSF", psf.to_timestamp(psf.col("MLL_CTA_DSF"), "dd/MM/yyyy"))
                .withColumn("top_acs",
                            psf.when((psf.col("BEN_CTA_TYP").isin([91, 92, 93])) &
                                     (psf.year(psf.col("MLL_CTA_DSD")) <= year) &
                                     ((psf.year(psf.col("MLL_CTA_DSF")) >= year) |
                                      (psf.col("MLL_CTA_DSF").isNull())), 1).otherwise(0))
                .withColumn("top_cmu",
                            psf.when((psf.year(psf.col("MLL_CTA_DSD")) <= year) &
                                     ((psf.year(psf.col("MLL_CTA_DSF")) >= year) |
                                      (psf.col("MLL_CTA_DSF").isNull())) &
                                     (psf.col("BEN_CTA_TYP") == "89"), 1).otherwise(0))
                .withColumnRenamed("top_acs", "ir_orc_r__top_acs")
                .withColumnRenamed("top_cmu", "ir_orc_r__top_cmu")
                .select(["NUM_ENQ", "ir_orc_r__top_acs", "ir_orc_r__top_cmu"])
                .groupBy(psf.col("NUM_ENQ")).agg({"ir_orc_r__top_acs": "max", "ir_orc_r__top_cmu": "max"})
                .withColumnRenamed("max(ir_orc_r__top_acs)", "ir_orc_r__top_acs")
                .withColumnRenamed("max(ir_orc_r__top_cmu)", "ir_orc_r__top_cmu")
                )

    data_rac = (data_rac.join(ir_orc_r, ["NUM_ENQ"], how="left")
                .withColumn("top_acs",
                            psf.when((~psf.col("ir_orc_r__top_acs").isNull()), psf.col("ir_orc_r__top_acs"))
                            .when(~(psf.col("top_acs").isNull()), psf.col("top_acs"))
                            .otherwise(0)))
    data_rac = data_rac.withColumn("top_cmu", psf.when((psf.col("ir_orc_r__top_cmu").isNotNull()),
                                                       psf.col("ir_orc_r__top_cmu")).
                                   when((psf.col("top_cmu").isNotNull()),
                                        psf.col("top_cmu")).
                                   otherwise(0))
    # On remplace la variable top_ald par top_ald_exo_mtf qui est plus précise
    # Et on remplace les null par des 0 
    data_rac = data_rac.withColumn("top_ald_sans_exo_mtf", psf.col("top_ald"))
    data_rac = data_rac.withColumn("top_ald", psf.when(psf.col("top_ald_exo_mtf") == 1, psf.col("top_ald_exo_mtf")).
                                   otherwise(0))
    temp = data_rac.groupBy("NUM_ENQ").agg({"top_ald": "max"}).withColumnRenamed("max(top_ald)", "top_ald")
    data_rac = (data_rac.drop("top_ald")
                .join(temp, ["NUM_ENQ"], how="left"))

    data_rac = _correct_geography(path2flat, path2cor_com, path2cor_postx, data_rac, year, spark)
    data_rac = _separate_ald(data_rac, year, spark)

    return data_rac

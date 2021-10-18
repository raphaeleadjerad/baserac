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
import pyspark.sql.window as psw
from time import time


def _clean_sup_part(col_name):
    """
    Function used in preprocessing NS_PRS_F, to transform ARi_REM_MNT to 0 when TYPE is equal to 10 (where i=1,2,3)
    :param col_name: str, name of the variable for supplementary part, AR1_REM_MNT, AR2_REM_MNT or AR3_REM_MNT
    :return: Spark column
    """
    new_var = psf.when(psf.col(col_name[:-3] + "TYP") == 10, 0).when(psf.col(col_name).isNull(), 0).otherwise(
        psf.col(col_name))
    return new_var


def _divide_dup(col_name):
    """
    Function used in preprocessing NS_PRS_F, to divide amounts by number of duplicates
    :param col_name: str, colnames of the amount to divide by `dup_count`
    :return: Spark column
    """
    new_var = psf.when(psf.col("dup_count") > 0, psf.col(col_name) / psf.col("dup_count")) \
        .otherwise(psf.lit(None))
    return new_var


def _treat_supp_parts(df):
    """
    Function to clean supplementary parts for all variables and create sum of these amounts
    """
    for col_name in ["AR1_REM_MNT", "AR2_REM_MNT", "AR3_REM_MNT"]:
        df = df.withColumn(col_name, _clean_sup_part(col_name))
    # Sum supplementary parts
    df = df.withColumn("ARO_REM_MNT",
                       psf.col("AR1_REM_MNT") +
                       psf.col("AR2_REM_MNT") +
                       psf.col("AR3_REM_MNT"))

    return df


def _count_dup(df):
    w = psw.Window.partitionBy('CLE_DCI_JNT')
    df = df.withColumn("dup_count", psf.count(psf.lit(1)).over(w)).sort(psf.col("dup_count").desc())
    df = df.withColumn('dup_count', psf.col("dup_count").cast(pst.DoubleType()))

    for col_name in ["PRS_PAI_MNT", "TOT_REM_MNT", "TOT_REM_BSE", "AR1_REM_MNT", "AR2_REM_MNT", "AR3_REM_MNT"]:
        df = df.withColumn(col_name, _divide_dup(col_name))
    return df


def _apply_filters(dcirs):
    """
    Applies common filters to DCIRS :
        - reimbursable acts, non free acts
        - filter out part of public hospitals info
        - nature of insurance
        - positive amounts for expenditure

    :param dcirs: Spark DataFrame
    :return: Spark DataFrame
    """
    free_code_acts = 20
    non_reimbursable = 30
    fee_day = 2252

    # Filter free and non reimbursable
    dcirs = (dcirs.filter(psf.col("DPN_QLF") != free_code_acts).filter(
        (psf.col("DPN_QLF") != non_reimbursable) |
        ((psf.col("DPN_QLF") == non_reimbursable) & (psf.col("PRS_NAT_REF") == fee_day)))
    )

    # Filter public hospit
    dcirs = (dcirs
             .filter((psf.col("ETE_IND_TAA") != 1) | (psf.col("ETE_IND_TAA").isNull()))
             )

    # Risks health, maternity, death, invalidity, at/mp
    insurance_nature = [10, 30, 40, 50, 80]
    dcirs = (dcirs
             .filter(psf.col('RGO_ASU_NAT').isin(insurance_nature))
             )

    # Positive expenditure
    dcirs = (dcirs
             .filter(psf.col("PRS_PAI_MNT") > 0)
             )

    return dcirs


def load_preprocess_ns_prs(path2flat, path2nomenclature, actes_cli_tech, year, spark):
    """
    Function that loads NS_PRS_F, joins it with act and medical specialty nomenclature,
    applies some filters and outputs a pyspark DataFrame
    :param actes_cli_tech:
    :param path2flat: str, path to directory where single tables in parquet format are stored
    :param path2nomenclature: str, path to directory where nomenclatures in csv are stored
    :param year: int, indicates the year for the intput and final database
    :param spark: SparkSession, can be made with utils.start_context
    :return: Spark DataFrame
    """
    if year == 2017:
        path_ns_prs = path2flat + str(year)[-2:] + "/single_table/NS_PRS_F/"
    elif year == 2016:
        path_ns_prs = path2flat + str(year)[-2:] + "/single_table/ER_PRS_F/year=" + str(year)
    # Attention : Flat table is called ER_PRS_Fin 2016 but it is NS_PRS_F, -> SCALPEL Flattening
    else:
        raise ValueError("Année non prise en charge")
    dcirs = spark.read.parquet(path_ns_prs)


    cols = ["NUM_ENQ", "BEN_CMU_TOP", "PRS_PAI_MNT", "PRS_DEP_MNT", "TOT_REM_MNT", "TOT_REM_BSE", "RGO_ASU_NAT",
            "PRS_NAT_REF", "ETE_CAT_COD", "PSE_ACT_SPE", "PRS_PPU_SEC", "ETB_EXE_FIN", "EXO_MTF", "ETE_IND_TAA",
            "CLE_DCI_JNT", "EXE_SOI_DTD", "PFS_EXE_NUM", "BSE_REM_MNT", "BSE_REM_BSE", "PRS_ACT_QTE", "PRS_ACT_COG",
            "GRG_AFF_COD", "DPN_QLF", "ETE_TYP_COD", "DDP_COD", "BEN_CTA_TYP", "RGM_COD", "PFS_EXE_NUM", "AR1_REM_MNT",
            "AR2_REM_MNT", "AR3_REM_MNT", "AR1_REM_TYP", "AR2_REM_TYP", "AR3_REM_TYP", "BEN_AMA_COD", "EXE_SOI_DTF",
            "PRS_HOS_DTD", "MDT_COD"]

    dcirs = dcirs.select(cols)

    # Verify schema
    dcirs = (dcirs.withColumn("CLE_DCI_JNT", psf.col("CLE_DCI_JNT").cast(pst.LongType()))
             .withColumn("PRS_NAT_REF", psf.col("PRS_NAT_REF").cast(pst.DoubleType()))
             .withColumn("ETE_IND_TAA", psf.col("ETE_IND_TAA").cast(pst.IntegerType()))
             .withColumn("PRS_PAI_MNT", psf.col("PRS_PAI_MNT").cast(pst.DoubleType())))

    dcirs = _apply_filters(dcirs)

    # Top EXO_MTF for ALD
    dcirs = dcirs.withColumn("top_exo_mtf", psf.when(psf.col("EXO_MTF").isin([41, 42, 43, 44, 45, 46]), 1).otherwise(0))

    # Top lien ALD
    dcirs = dcirs.withColumn("top_lien_ald", psf.when(psf.col("EXO_MTF").isin([42, 44, 46]), 1).otherwise(0))

    dcirs = dcirs.withColumn("BEN_CMU_TOP",
                             psf.when(psf.col("BEN_CMU_TOP").isin(2), 0).otherwise(psf.col("BEN_CMU_TOP")))

    # Import nomenclature for categories
    presta = (spark.read
              .option("header", "true")
              .option("sep", ",")
              .csv(path2nomenclature + "prs_nat_ref_x_presta_r.csv")
              .select(["PRS_NAT_REF", "presta_R"])
              )

    presta = presta.withColumn("PRS_NAT_REF", psf.col("PRS_NAT_REF").cast(pst.DoubleType()))
    presta = presta.filter(psf.col("PRS_NAT_REF").isNotNull())

    # Merge
    dcirs = dcirs.join(presta, ['PRS_NAT_REF'], 'left_outer')

    # Filter acts that are not in Health accounts ("CSBM")
    exclude = ["a_exclure", "prestations_en_espece_et_prevention_IJ_maladie_AT", "non_indiv_ou_pro", "IJ"]
    dcirs = dcirs.filter(~psf.col('presta_R').isin(exclude))

    # Med Specialty pse_act_spe_det
    spe_det = (spark.read
               .option("header", "true")
               .option("sep", ",")
               .csv(path2nomenclature + "pse_act_spe_x_spe_act_det.csv")
               .select(["pse_act_spe", "spe_act_det"])
               )
    spe_det = (spe_det.filter(psf.col("spe_act_det").isNotNull())
               .withColumnRenamed("pse_act_spe", "PSE_ACT_SPE")
               .filter(psf.col("spe_act_det") != "NA"))

    spe_det = spe_det.withColumn("PSE_ACT_SPE", psf.col("PSE_ACT_SPE").cast(pst.IntegerType()))

    # Broadcast join
    dcirs = dcirs.join(psf.broadcast(spe_det), ["PSE_ACT_SPE"], "left")

    # Add top ACS
    acs = [91, 92, 93]
    dcirs = dcirs.withColumn("top_acs", psf.when(psf.col("BEN_CTA_TYP").isin(acs), psf.lit(1))
                             .otherwise(psf.lit(0)))

    dcirs = dcirs.withColumn("top_cmu", psf.when(psf.col("BEN_CTA_TYP").isin(89), psf.lit(1))
                             .otherwise(psf.lit(0)))

    # Add Specialty when unknown based on PRS_NAT_REF  
    nomen_non_med = (spark.read
                     .option("header", "true")
                     .option("sep", ",")
                     .csv(path2nomenclature + "nomenclature_liste_presta_non_med.csv")
                     .select("PRS_NAT_REF", "spe_act_det")
                     .withColumnRenamed("spe_act_det", "spe_act_det_non_med")
                     )
    dcirs = dcirs.join(nomen_non_med, ["PRS_NAT_REF"], how="left")

    dcirs = dcirs.withColumn("spe_act_det", psf.when(
        (psf.col("PSE_ACT_SPE").isin(["0", "99"])) & (psf.col("presta_R").isin(actes_cli_tech))
        & (psf.col("spe_act_det_non_med").isin("Dentiste")), "Dentiste").
                             otherwise(psf.col("spe_act_det")))

    # Mois soins pour clustering
    dcirs = (dcirs.withColumn("EXE_SOI_DTD2", psf.to_timestamp(psf.col("EXE_SOI_DTD"), "dd/MM/yyyy"))
             .withColumn("mois_soins", psf.month(psf.col("EXE_SOI_DTD2")))
             .drop("EXE_SOI_DTD2"))

    return dcirs


def _treat_institutions(df, path2ref, spark):
    # Nomenclature Etab
    ir_cet = (spark.read
              .option("header", "true")
              .option("sep", ",")
              .csv(path2ref + "IR_CET_V.csv")
              .withColumnRenamed("ETB_CAT_COD", "ETE_CAT_COD")
              .withColumn("ETE_CAT_COD", psf.col("ETE_CAT_COD").cast(pst.IntegerType()))
              .select(["ETE_CAT_COD", "ETB_CAT_RG1"])
              .withColumn("sub_ETB_CAT_RG1", psf.substring("ETB_CAT_RG1", 1, 2))
              )

    # Recoding categories
    df = (df.withColumn("presta_R", psf.when(psf.col("presta_R") == "Supplement_hospit", "etab_priv")
                        .otherwise(psf.col("presta_R"))))
    df = (df.withColumn("presta_R",
                        psf.when(psf.col("presta_R") == "Forfait_thermal", "cures_thermales")
                        .otherwise(psf.col("presta_R"))))

    # Filter out social establishments
    code_exclude = ["41", "43", "44", "45", "46"]
    cod2exclude = (ir_cet
                   .select("ETE_CAT_COD")
                   .filter(psf.col("sub_ETB_CAT_RG1").isin(code_exclude))
                   .distinct())
    cod2exclude = [int(row.ETE_CAT_COD) for row in cod2exclude.collect()]

    df = (df.filter((~psf.col("ETE_CAT_COD").isin(cod2exclude)) | (psf.col("ETE_CAT_COD").isNull())))

    # Create variable for location
    # Liberal
    liberal = [21, 22]
    ete_cat_cod_ville = ir_cet.select("ETE_CAT_COD").filter(psf.col("sub_ETB_CAT_RG1").isin(liberal)).distinct()
    ete_cat_cod_ville = [int(row.ETE_CAT_COD) for row in ete_cat_cod_ville.collect()]

    df = (df.withColumn("lieu_ex",
                        psf.when(psf.col("ETB_EXE_FIN") == 0, "Ville")
                        .when((psf.col("ETE_CAT_COD").isin(ete_cat_cod_ville)) &
                              (psf.col("ETE_CAT_COD").isin([696, 697, 698])), "Ville")
                        .when(psf.col("PRS_PPU_SEC") == 1, "Public")
                        .otherwise("Prive")))
    # #### Exceptions for some acts (hospital pharmacy, flat rate, dialysis, etc.)

    #df = (df.withColumn("presta_R", psf.when(psf.col("lieu_ex").isin(["Public", "Prive"]) &
    #                                         psf.col("presta_R").isin(["Pharma_hospit"]),
    #                                         "retrocession")
    #                    .otherwise(psf.col("presta_R"))))

    df = (df.withColumn("presta_R",
                        psf.when(psf.col("lieu_ex").isin(["Public", "Prive"]) &
                                 psf.col("presta_R").isin(["autre_forfait", "forfait_dialyse", "forfait_techniq"]),
                                 "etab_priv")
                        .otherwise(psf.col("presta_R"))))

    # Health center
    code_centre_sante = [125, 130, 132, 133, 134, 142, 223, 224, 228, 230, 268, 269, 289, 297,
                         347, 413, 414, 433, 438, 439, 700]
    df = (df.withColumn("top_csante", psf.when(psf.col("ETE_CAT_COD").isin(code_centre_sante), 1)
                        .otherwise(0)))

    # Exclusion of public acts except for hospital pharmacy / health centers
    df = (df.filter((~psf.col("lieu_ex").isin(["Public"])) |
                    ((psf.col("lieu_ex").isin(["Public"])) & (psf.col("top_csante") == 1)) |
                    ((psf.col("lieu_ex").isin(["Public"])) & (psf.col("presta_R").isin(["retrocession", "retrocession_30", "retrocession_65", "retrocession_100"])))))

    # #### Statute
    df = (df.withColumn("presta_R",
                        psf.when((psf.col("presta_R") == "etab_priv") &
                                 (psf.col("ETE_TYP_COD").isin([4, 5, 9])), "etab_priv_lucratif")
                        .when((psf.col("presta_R") == "etab_priv") &
                              (psf.col("ETE_TYP_COD").isin([6, 7, 8])), "etab_priv_non_lucratif")
                        .when((psf.col("presta_R") == "etab_priv"), "autre_etab_priv")
                        .otherwise(psf.col("presta_R"))))

    etab_priv = ["etab_priv_lucratif", "etab_priv_non_lucratif", "autre_etab_priv"]

    ir_ddp = (spark.read
              .option("header", "true")
              .option("sep", ",")
              .csv(path2ref + "IR_DDP_V.csv")
              .select(["DDP_COD", "DDP_GDE_COD"])
              )

    ssr_ddp = ["4"]
    psy_ddp = ["6"]
    mco_ddp = ["0", "1", "2", "3"]
    had_ete_cat_cod = [127, 422]

    ssr_ddp_cod = ir_ddp.select("DDP_COD").filter(psf.col("DDP_GDE_COD").isin(ssr_ddp)).distinct()
    ssr_ddp_cod = [int(row.DDP_COD) for row in ssr_ddp_cod.collect()]
    psy_ddp_cod = ir_ddp.select("DDP_COD").filter(psf.col("DDP_GDE_COD").isin(psy_ddp)).distinct()
    psy_ddp_cod = [int(row.DDP_COD) for row in psy_ddp_cod.collect()]
    mco_ddp_cod = ir_ddp.select("DDP_COD").filter(psf.col("DDP_GDE_COD").isin(mco_ddp)).distinct()
    mco_ddp_cod = [int(row.DDP_COD) for row in mco_ddp_cod.collect()]
    ght = 2113
    # Separate private establishments based on hospital spe
    df = (df.withColumn("type_specialite_hospit",
                        psf.when(psf.col("DDP_COD").isin(ssr_ddp_cod), "ssr")
                        .when(psf.col("DDP_COD").isin(psy_ddp_cod), "psy")
                        .when((psf.col("DDP_COD").isin(mco_ddp_cod)) &
                              (psf.col("PRS_NAT_REF") != ght), "mco")
                        .when(psf.col("ETE_CAT_COD").isin(had_ete_cat_cod) |
                              (psf.col("PRS_NAT_REF") == ght), "had")
                        .otherwise("autre")))

    df = (df.withColumn("type_etab_prive", psf.concat(psf.col("type_specialite_hospit"),
                                                      psf.lit("_"),
                                                      psf.col("presta_R"))))

    df = (df.withColumn("presta_R",
                        psf.when(psf.col("presta_R").isin(etab_priv), psf.col("type_etab_prive"))
                        .otherwise(psf.col("presta_R"))))

    return df


def agg_ns_prs_non_lpp_dental(dcirs, path2ref, spark, prs_lpp, spe_act_det_dent, actes_cli_tech, agg_dict,
                              agg_var=None):
    """
    Function that aggregates amounts in NS_PRS_F where acts are not dental and not LPP.
    Aggregation by beneficiary, act
    :param agg_var:
    :param agg_dict: dict, links variables in output to aggregation functions
    :param actes_cli_tech: List, name of clinical and technical acts in nomenclature
    :param spe_act_det_dent: List, name of Medical Specialty for dental
    :param prs_lpp: List, name of LPP acts
    :param dcirs: Spark DataFrame
    :param path2ref: str, path where referentials for building the database are stored
    :param spark: SparkSession, can be made with utils.start_context
    :return: Spark DataFrame
    """

    if agg_var is None:
        agg_var = ["NUM_ENQ", "spe_act_x_presta_R"]
    dcirs_non_lpp_dental = dcirs.filter(~psf.col("presta_R").isin(prs_lpp))
    dcirs_non_lpp_dental = (dcirs_non_lpp_dental
                            .filter(~((psf.col("spe_act_det").isin(spe_act_det_dent) &
                                       psf.col("presta_R").isin(actes_cli_tech)) |
                                      (psf.col("spe_act_det").isin(spe_act_det_dent) & psf.col("presta_R").isin(
                                          ["orthodontie"])))))

    dcirs_non_lpp_dental = _treat_institutions(dcirs_non_lpp_dental, path2ref, spark)

    # #### Ventilate clinical and technical acts by Med Specialty

    dcirs_non_lpp_dental = (dcirs_non_lpp_dental
                            .withColumn("spe_act_x_presta_R", psf.concat(psf.col("spe_act_det"),
                                                                         psf.lit("_"),
                                                                         psf.col("presta_R"))))

    dcirs_non_lpp_dental = (dcirs_non_lpp_dental
                            .withColumn("spe_act_x_presta_R",
                                        psf.when(~psf.col("presta_R").isin(actes_cli_tech),
                                                 psf.col("presta_R"))
                                        .otherwise(psf.col("spe_act_x_presta_R"))))

    dcirs_non_lpp_dental = (dcirs_non_lpp_dental
                            .withColumn("spe_act_x_presta_R",
                                        psf.when(~psf.col("presta_R").isin(actes_cli_tech), psf.col("presta_R"))
                                        .when(
                                            (psf.col("presta_R").isin(actes_cli_tech)) & (
                                                psf.col("lieu_ex").isin("Prive")),
                                            psf.concat(psf.col("spe_act_x_presta_R"),
                                                       psf.lit("_priv"))).
                                        otherwise(psf.col("spe_act_x_presta_R"))))

    dcirs_non_lpp_dental = dcirs_non_lpp_dental.withColumn("spe_act_x_presta_R",
                                                           psf.when((psf.col("spe_act_x_presta_R").isin("Labo")) &
                                                                    (psf.col("lieu_ex").isin("Prive")), "Labo_priv")
                                                           .otherwise(psf.col("spe_act_x_presta_R")))

    # Extract DMIP from LPP

    dcirs_non_lpp_dental = (dcirs_non_lpp_dental
                            .withColumn("spe_act_x_presta_R",
                                        psf.when((psf.col("presta_R").isin("LPP_autre")) & (
                                            psf.col("lieu_ex").isin("Prive")),
                                                 psf.concat(psf.col("spe_act_x_presta_R"), psf.lit("_priv")))
                                        .otherwise(psf.col("spe_act_x_presta_R"))))

    # repartition of unknown specialty 
    dcirs_non_lpp_dental = dcirs_non_lpp_dental.withColumn("spe_act_x_presta_R",
                                                           psf.when(psf.col("spe_act_x_presta_R")
                                                                    .isin("Non_med_cliniq", "Non_med_techniq"),
                                                                    psf.concat(psf.col("spe_act_det_non_med"),
                                                                               psf.lit("_"),
                                                                               psf.col("presta_R")))
                                                           .otherwise(psf.col("spe_act_x_presta_R")))

    dcirs_non_lpp_dental = _treat_supp_parts(dcirs_non_lpp_dental)

    dcirs_non_lpp_dental = dcirs_non_lpp_dental.filter(psf.col("spe_act_x_presta_R").isNotNull())

    # expenditure linked/not linked with ALD

    dcirs_non_lpp_dental = dcirs_non_lpp_dental.withColumn("dep_lien_ald",
                                                           psf.col("top_lien_ald") * psf.col("PRS_PAI_MNT"))
    dcirs_non_lpp_dental = dcirs_non_lpp_dental.withColumn("mnt_rem_lien_ald",
                                                           psf.col("top_lien_ald") * psf.col("TOT_REM_MNT"))
    dcirs_non_lpp_dental = dcirs_non_lpp_dental.withColumn("aro_rem_lien_ald",
                                                           psf.col("top_lien_ald") * psf.col("ARO_REM_MNT"))

    # #### Aggregation
    rac_nonlppdental_ag = (dcirs_non_lpp_dental
                           .groupBy(agg_var)
                           .agg(agg_dict))

    return rac_nonlppdental_ag


def agg_ns_prs_dental(dcirs, path2ref, path_ns_cam, path2nomenclature, spark,
                      spe_act_det_dent, actes_cli_tech, agg_dict, agg_dict_dent):
    """
    Function that aggregates amounts in NS_PRS_F where acts are dental by importing NS_CAM_F
    Aggregation by beneficiary, act

    :param agg_dict_dent:  dict, links variables in output to aggregation functions
    :param agg_dict: dict, links variables in output to aggregation functions
    :param actes_cli_tech: List, name of clinical and technical acts in nomenclature
    :param spe_act_det_dent: List, name of Medical Specialty for dental
    :param path2nomenclature: str, path to directory where nomenclatures in csv are stored
    :param path2ref: str, path where referentials for building the database are stored
    :param dcirs: Spark DataFrame
    :param path_ns_cam: str, path where NS_CAM_F single table in parquet format is stored
    :param spark: SparkSession, built with utils.start_context()
    :return: Spark DataFrame
    """
    ns_cam_f = (spark
                .read.parquet(path_ns_cam)
                .select(["CLE_DCI_JNT", "CAM_PRS_IDE", "CAM_ACT_QSN"]))
    ns_cam_f = ns_cam_f.withColumn("CLE_DCI_JNT", psf.col("CLE_DCI_JNT").cast(pst.LongType()))

    # #### Import ccam nomenclature

    ccam_nom = (spark.read
                .option("header", "true")
                .option("sep", ",")
                .csv(path2ref + "CCAM_V53.csv")
                )
    ccam_nom = (ccam_nom
                .drop(psf.col("_c0"))
                .drop(psf.col("Texte"))
                .withColumnRenamed("Code", "CAM_PRS_IDE")
                .withColumnRenamed("Regroupement", "GROUP_CCAM")
                .filter((psf.col("CAM_PRS_IDE").isNotNull()) & (psf.col("GROUP_CCAM").isNotNull())))

    # Left join NS_CCAM_F and ccam_nom
    ns_cam_f = ns_cam_f.join(psf.broadcast(ccam_nom), ["CAM_PRS_IDE"], how="left")

    # #### Regrouping codes CCAM

    ccam_group = (spark.read
                  .option("header", "true")
                  .option("sep", ",")
                  .csv(path2nomenclature + "classif_ccam_dentaire.txt")
                  .withColumnRenamed("Code de regroupement CCAM dentaire", "GROUP_CCAM")
                  .withColumnRenamed("Définition des codes de regroupement CCAM dentaire", "lib_GROUP_CCAM")
                  )

    # Left join with group ccam nomenclature for dental
    ns_cam_f = ns_cam_f.join(psf.broadcast(ccam_group), ["GROUP_CCAM"], how="left")

    ns_cam_f = (ns_cam_f
                .groupBy(["CLE_DCI_JNT", "GROUP_CCAM", "lib_GROUP_CCAM"])
                .agg({"CAM_ACT_QSN": "sum"})
                .filter(psf.col("GROUP_CCAM").isNotNull()))

    # Join central table with CCAM table
    ns_prs_dental = (dcirs
                     .filter(psf.col("spe_act_det").isin(spe_act_det_dent) &
                             psf.col("presta_R").isin(actes_cli_tech + ["orthodontie"])))
    ns_prs_dental = ns_prs_dental.coalesce(500)
    ns_cam_f = ns_cam_f.coalesce(500)

    # Left join
    ns_prs_dental = ns_prs_dental.join(ns_cam_f, ["CLE_DCI_JNT"], how="left")

    # Deal with duplicates
    ns_prs_dental = _count_dup(ns_prs_dental)

    # Supplementary parts
    ns_prs_dental = _treat_supp_parts(ns_prs_dental)

    # institutions
    ns_prs_dental = _treat_institutions(ns_prs_dental, path2ref, spark)

    # expenditure linked/not linked to ald
    ns_prs_dental = ns_prs_dental.withColumn("dep_lien_ald", psf.col("top_lien_ald") * psf.col("PRS_PAI_MNT"))
    ns_prs_dental = ns_prs_dental.withColumn("mnt_rem_lien_ald", psf.col("top_lien_ald") * psf.col("TOT_REM_MNT"))
    ns_prs_dental = ns_prs_dental.withColumn("aro_rem_lien_ald", psf.col("top_lien_ald") * psf.col("ARO_REM_MNT"))

    rac_dental = (ns_prs_dental
                  .filter(psf.col("lib_GROUP_CCAM").isNotNull()))
    rac_dental = rac_dental.coalesce(200)

    rac_dental_ag = (rac_dental
                     .groupBy(psf.col("NUM_ENQ"), psf.col("lib_GROUP_CCAM"))
                     .agg(agg_dict_dent)
                     .withColumnRenamed("lib_GROUP_CCAM", "poste"))

    # Dental, but None for group ccam acts
    rac_dental = (ns_prs_dental
                  .filter(psf.col("lib_GROUP_CCAM").isNull())
                  .withColumn("poste", psf.concat(psf.col("spe_act_det"),
                                                  psf.lit("_"),
                                                  psf.col("presta_R"))))


    rac_dental_nocode_ag = (rac_dental
                            .groupBy(psf.col("NUM_ENQ"), psf.col("poste"))
                            .agg(agg_dict)
                            .withColumn("poste",
                                        psf.when(psf.col("poste").isin(["chir_dent_stom_orthodontie"]),
                                                 "chir_dent_stom_techniq")
                                        .when(
                                            psf.col("poste").isin(["Dentiste_orthodontie_priv"]),
                                            "Dentiste_orthodontie")
                                        .when(
                                            psf.col("poste").isin(["Dentiste_techniq_priv"]), "Dentiste_techniq")
                                        .when(psf.col("poste").isin("Dentiste_cliniq_priv"), "Dentiste_cliniq")
                                        .otherwise(psf.col("poste")))
                            )

    return rac_dental_ag, rac_dental_nocode_ag


def agg_ns_prs_lpp(dcirs, path2nomenclature, path_ns_tip, path2ref, spark, prs_lpp, agg_dict_lpp):
    """
    Function that aggregates amounts in NS_PRS_F where acts are LPP by importing NS_TIP_F
    Aggregation by beneficiary, act

    :param path2ref:
    :param agg_dict_lpp:: dict, links variables in output to aggregation functions
    :param prs_lpp: List, name of LPP acts
    :param path2nomenclature: str, path to directory where nomenclatures in csv are stored
    :param dcirs: Spark DataFrame
    :param path_ns_tip: str, path where NS_TIP_F single table in parquet format is stored
    :param spark: SparkSession, built with utils.start_context()
    :return: Spark DataFrame
    """
    ns_tip_f = (spark.read
                .parquet(path_ns_tip)
                .select(["CLE_DCI_JNT", "TIP_PRS_IDE", "TIP_ACT_QSN"]))

    ns_tip_f = ns_tip_f.withColumn("TIP_ACT_QSN", psf.col("TIP_ACT_QSN").cast(pst.DoubleType()))

    # Sum Quantity by key, should already be the case
    ns_tip_f = ns_tip_f.groupBy(["CLE_DCI_JNT", "TIP_PRS_IDE"]).agg({"TIP_ACT_QSN": "sum"})

    # Import personal nomenclature table
    tip_prs_ide = (spark.read
                   .option("header", "true")
                   .option("sep", ";")
                   .csv(path2nomenclature + "tip_prs_ide_x_detail_presta.txt")
                   )

    tip_prs_ide = (tip_prs_ide
                   .groupBy(psf.col("tip_prs_ide"))
                   .agg({"detail_presta": "first"})
                   .withColumnRenamed("tip_prs_ide", "TIP_PRS_IDE")
                   .withColumn("TIP_PRS_IDE", psf.col("TIP_PRS_IDE").cast(pst.IntegerType())))

    # tip_prs_ide.count() # 114 lines -> broadcast join

    ns_tip_f = ns_tip_f.coalesce(500)
    ns_tip_f = (ns_tip_f.join(psf.broadcast(tip_prs_ide), ["TIP_PRS_IDE"], how="left")
                .withColumnRenamed("first(detail_presta)", "detail_presta"))

    # Filter central act table
    ns_prs_lpp = dcirs.filter(psf.col("presta_R").isin(prs_lpp))
    ns_prs_lpp = ns_prs_lpp.repartition(300)

    # Merge with specialized table
    ns_prs_lpp = ns_prs_lpp.join(ns_tip_f, ["CLE_DCI_JNT"], how="left")
    ns_prs_lpp = _count_dup(ns_prs_lpp)
    ns_prs_lpp = _treat_supp_parts(ns_prs_lpp)
    ns_prs_lpp = _treat_institutions(ns_prs_lpp, path2ref, spark)
    ns_prs_lpp = (ns_prs_lpp.withColumn("dep_lien_ald", psf.col("top_lien_ald") * psf.col("PRS_PAI_MNT"))
                  .withColumn("mnt_rem_lien_ald", psf.col("top_lien_ald") * psf.col("TOT_REM_MNT"))
                  .withColumn("aro_rem_lien_ald", psf.col("top_lien_ald") * psf.col("ARO_REM_MNT")))

    rac_lpp = ns_prs_lpp.filter(psf.col("detail_presta").isNotNull())
    rac_lpp = rac_lpp.coalesce(200)

    rac_lpp = rac_lpp.withColumn("BEN_CMU_TOP", psf.col("BEN_CMU_TOP").cast(pst.IntegerType()))

    rac_lpp_ag = (rac_lpp.groupBy(psf.col("NUM_ENQ"), psf.col("detail_presta"))
                  .agg(agg_dict_lpp)
                  .withColumnRenamed("detail_presta", "poste")
                  )

    # Unknown regrouping codes
    rac_lpp = (ns_prs_lpp.filter(psf.col("detail_presta").isNull())
               .withColumn("poste", psf.col("presta_R"))
               .withColumn("BEN_CMU_TOP", psf.col("BEN_CMU_TOP").cast(pst.IntegerType()))
               )
    rac_lpp = rac_lpp.coalesce(30)

    rac_lpp_nocode_ag = rac_lpp.groupBy(psf.col("NUM_ENQ"), psf.col("poste")).agg(agg_dict_lpp)
    rac_lpp_nocode_ag = rac_lpp_nocode_ag.coalesce(30)

    return rac_lpp_ag, rac_lpp_nocode_ag


def clean_output(rac_nonlppdental_ag, rac_dental_ag, rac_dental_nocode_ag, rac_lpp_ag, rac_lpp_nocode_ag):
    """
    Function that renames all variables in aggregated Spark DataFrames
    :param rac_nonlppdental_ag: Spark DataFrame
    :param rac_dental_ag: Spark DataFrame
    :param rac_dental_nocode_ag: Spark DataFrame
    :param rac_lpp_ag: Spark DataFrame
    :param rac_lpp_nocode_ag: Spark DataFrame
    :return: Spark DataFrames
    """
    output_var = ['acte_quantite', 'poste', 'PRS_PAI_MNT', 'TOT_REM_MNT', 'ARO_REM_MNT', 'top_cmu', 'top_acs',
                  'EXO_MTF', 'TOT_REM_BSE', 'top_exo_mtf', 'BEN_AMA_COD', 'mnt_rem_lien_ald',
                  'aro_rem_lien_ald', 'dep_lien_ald']
    input_var = ['sum(PRS_ACT_QTE)', 'spe_act_x_presta_R', 'sum(PRS_PAI_MNT)', 'sum(TOT_REM_MNT)', 'sum(ARO_REM_MNT)',
                 'max(top_cmu)', 'max(top_acs)', 'first(EXO_MTF)', 'sum(TOT_REM_BSE)', 'max(top_exo_mtf)',
                 'max(BEN_AMA_COD)', 'sum(mnt_rem_lien_ald)', 'sum(aro_rem_lien_ald)', 'sum(dep_lien_ald)']
    mapping = dict(zip(input_var, output_var))
    rac_nonlppdental_ag = rac_nonlppdental_ag.select(
        [psf.col(c).alias(mapping.get(c, c)) for c in rac_nonlppdental_ag.columns])
    rac_dental_nocode_ag = rac_dental_nocode_ag.select(
        [psf.col(c).alias(mapping.get(c, c)) for c in rac_dental_nocode_ag.columns])

    input_var[0] = "sum(sum(CAM_ACT_QSN))"
    mapping = dict(zip(input_var, output_var))
    rac_dental_ag = rac_dental_ag.select([psf.col(c).alias(mapping.get(c, c)) for c in rac_dental_ag.columns])
    input_var[0] = "sum(sum(TIP_ACT_QSN))"
    mapping = dict(zip(input_var, output_var))
    rac_lpp_ag = rac_lpp_ag.select([psf.col(c).alias(mapping.get(c, c)) for c in rac_lpp_ag.columns])
    rac_lpp_nocode_ag = rac_lpp_nocode_ag.select(
        [psf.col(c).alias(mapping.get(c, c)) for c in rac_lpp_nocode_ag.columns])

    return rac_nonlppdental_ag, rac_dental_ag, rac_dental_nocode_ag, rac_lpp_ag, rac_lpp_nocode_ag


def save_parquet(rac_nonlppdental_ag, rac_dental_ag, rac_dental_nocode_ag, rac_lpp_ag, rac_lpp_nocode_ag, path2output,
                 write=None):
    """
    Function that saves all Spark DataFrames to parquet in path2output
    :param write:
    :param path2output: str, path to store output in parquet format
    :param rac_nonlppdental_ag: Spark DataFrame
    :param rac_dental_ag: Spark DataFrame
    :param rac_dental_nocode_ag: Spark DataFrame
    :param rac_lpp_ag: Spark DataFrame
    :param rac_lpp_nocode_ag: Spark DataFrame
    :return: None
    """
    if write is None:
        write = ["nonlppdental", "dental", "dental_nocode_ag", "lpp", "lpp_nocode_ag"]
    start_parquet_time = time()
    if "nonlppdental" in write:
        rac_nonlppdental_ag = rac_nonlppdental_ag.coalesce(500)
        rac_nonlppdental_ag.write.parquet(path2output + "rac_nonlppdental_ag")
    if "dental" in write:
        rac_dental_ag = rac_dental_ag.coalesce(100)
        rac_dental_ag.write.parquet(path2output + "rac_dental_ag")
    if "dental_nocode_ag" in write:
        rac_dental_nocode_ag = rac_dental_nocode_ag.coalesce(50)
        rac_dental_nocode_ag.write.parquet(path2output + "rac_dental_nocode_ag")
    if "lpp" in write:
        rac_lpp_ag = rac_lpp_ag.coalesce(100)
        rac_lpp_ag.write.parquet(path2output + "rac_lpp_ag")
    if "lpp_nocode_ag" in write:
        rac_lpp_nocode_ag = rac_lpp_nocode_ag.coalesce(30)
        rac_lpp_nocode_ag.write.parquet(path2output + "rac_lpp_nocode_ag")
    end_parquet_time = time()

    m, s = divmod(end_parquet_time - start_parquet_time, 60)
    print("Total elapsed time : {:.1f} m and {:.1f} seconds".format(m, s))
    return None

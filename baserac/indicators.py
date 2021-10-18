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
import pyspark.ml.feature as pml
import pandas as pd
from .utils import remove_string


def global_indicators(path2data_rac, path2output, date, spark, type_indicator=None):
    if type_indicator is None:
        type_indicator = ["age", "aides", "expenditure"]
    data_rac = spark.read.parquet(path2data_rac)
    if "age" in type_indicator:
        print("Age")
        age = (data_rac.groupBy("NUM_ENQ").agg({"classe_age": "first"})
               .withColumnRenamed("first(classe_age)", "classe_age")
               .cube("classe_age").count().sort("count", ascending=False)
               .toPandas())
        age.to_csv(path2output + "pyramide_age_" + date + ".csv", sep=";", encoding="utf-8", index=False)

        age = (data_rac.groupBy("NUM_ENQ").agg({"classe_age": "first", "BEN_SEX_COD": "first"})
               .withColumnRenamed('first(classe_age)', 'classe_age')
               .withColumnRenamed('first(BEN_SEX_COD)', 'BEN_SEX_COD'))

        for i in [1, 2]:
            temp = (age.filter(psf.col("BEN_SEX_COD") == i).cube("classe_age").count().sort("count", ascending=False)
                    .toPandas())
            temp.to_csv(path2output + "pyramide_classe_age_" + str(i) + "_" + date + ".csv", sep=";", encoding="utf-8",
                        index=False)
    stat_age = (data_rac.groupBy("NUM_ENQ").agg({"age": "first", "BEN_SEX_COD": "first"})
                .withColumnRenamed('first(age)', 'age')
                .withColumnRenamed('first(BEN_SEX_COD)', 'BEN_SEX_COD'))
    for i in [1, 2]:
        temp = stat_age.filter(psf.col("BEN_SEX_COD") == i).cube("age").count().sort("count",
                                                                                     ascending=False).toPandas()
        temp.to_csv(path2output + "pyramide_age_" + str(i) + "_" + date + ".csv", sep=";", encoding="utf-8",
                    index=False)

    # Counts
    if "aides" in type_indicator:
        print("Aides")
        cmu = (data_rac.groupBy("NUM_ENQ").agg({"top_cmu": "max"})
               .filter(psf.col("max(top_cmu)") == 1).count())
        acs = (data_rac.groupBy("NUM_ENQ").agg({"top_acs": "max"}).alias("top_acs")
               .filter(psf.col("max(top_acs)") == 1).count())
        ald = (data_rac.groupBy("NUM_ENQ").agg({"top_ald": "max"}).alias("top_ald")
               .filter(psf.col("max(top_ald)") == 1).count())

        (pd.DataFrame({'compte_ald': ald, "compte_acs": acs,
                       "compte_cmu": cmu}, index=[0])
         .to_csv(path2output + "aides_" + date + ".csv", sep=";", encoding="utf-8", index=False))

    # Expenditure
    if "expenditure" in type_indicator:
        print("Depense")

        temp = (data_rac.withColumn("part_sup", psf.col("rac") - psf.col("rac_amo_sup"))
                .withColumn("mnt_tot_rem", psf.col("remb_am") + psf.col("part_sup")))

        temp = (temp.groupBy("poste").agg(
            {"dep_tot": "sum", "poste_ag": "first", "poste_racine": "first", "Poste_cns": "first",
             "separation_sdv_hospit": "first", "tot_rem_bse": "sum", "remb_am": "sum",
             "mnt_tot_rem": "sum"}))
        temp = redefine_columns(temp)
        (temp
         .toPandas()
         .to_csv(path2output + "depense_poste_" + date + ".csv", sep=";",
                 encoding="utf-8", index=False)
         )
        temp = (temp.groupBy("poste_ag").agg(
            {"dep_tot": "sum", "tot_rem_bse": "sum", "remb_am": "sum",
             "mnt_tot_rem": "sum"}))
        temp = redefine_columns(temp)
        (temp
         .toPandas()
         .to_csv(path2output + "depense_poste_ag_" + date + ".csv", sep=";",
                 encoding="utf-8", index=False)
         )
    return None


def redefine_columns(df):
    input_var = df.columns
    output_var = [remove_string(c) for c in df.columns]
    mapping = dict(zip(input_var, output_var))
    df = df.select([psf.col(c).alias(mapping.get(c, c)) for c in df.columns])
    return df


def ventilated_indicators(path2data_rac, path2output, agg_dict, agg_dict_amounts, date, spark,
                          type_indicator=None):
    if type_indicator is None:
        type_indicator = ["poste", "analyse_ald", "groups"]
    data_rac = spark.read.parquet(path2data_rac)

    data_rac = data_rac.withColumn("poste_semi_ag",
                                   psf.when(psf.col("poste").isin("Sage_femme_techniq", "Orthophoniste_techniq"),
                                            psf.lit("auxilliaires")).
                                   otherwise(psf.col("poste_semi_ag")))
    data_rac = data_rac.withColumn("poste_semi_ag",
                                   psf.when(psf.col("poste").isin("prevention"), psf.lit("prevention")).
                                   otherwise(psf.col("poste_semi_ag")))

    for categ in ["poste_ag", "poste_racine", "poste_semi_ag"]:

        indiv_rac_categ = data_rac.groupBy("NUM_ENQ", categ).agg(agg_dict)
        indiv_rac_categ = redefine_columns(indiv_rac_categ)
        indiv_rac_categ = indiv_rac_categ.withColumn("top_sup_65_ans", psf.when(psf.col("age") >= 65, 1).otherwise(0))

        if "poste" in type_indicator and categ in ["poste_ag", "poste_racine"]:
            print("Depense/Rac par " + categ)
            agg_dict_temp = agg_dict.copy()
            agg_dict_temp.pop("classe_age", None)
            age_rac_categ = indiv_rac_categ.groupBy("classe_age", categ).agg(agg_dict_temp)
            age_rac_categ = redefine_columns(age_rac_categ)
            age_rac_categ.toPandas().to_csv(path2output + "depense_rac_age_" + categ + "_" + date + ".csv",
                                            sep=";", encoding="utf-8", index=False)

            agg_dict_temp = agg_dict.copy()
            agg_dict_temp.pop("classe_age", None)
            agg_dict_temp.pop("top_ald", None)
            age_ald_rac_categ = indiv_rac_categ.groupBy("classe_age", "top_ald", categ).agg(agg_dict_temp)
            age_ald_rac_categ = redefine_columns(age_ald_rac_categ)
            age_ald_rac_categ.toPandas().to_csv(path2output + "depense_rac_age_ald_" + categ + "_" + date + ".csv",
                                                sep=";", encoding="utf-8", index=False)

        if "analyse_ald" in type_indicator and categ in ["poste_ag", "poste_racine", "poste_semi_ag"]:
            for var_ald in ["groupe_ALD", "type_ALD"]:
                print("Depense/Rac par " + var_ald + "_" + categ)
                agg_dict_temp = agg_dict.copy()
                agg_dict_temp.pop("classe_age", None)
                agg_dict_temp.pop(var_ald, None)
                age_ald_rac_categ = indiv_rac_categ.groupBy("classe_age", var_ald, categ).agg(agg_dict_temp)
                age_ald_rac_categ = redefine_columns(age_ald_rac_categ)
                age_ald_rac_categ.toPandas().to_csv(
                    path2output + "depense_rac_age_" + var_ald + "_" + categ + "_" + date + ".csv",
                    sep=";", encoding="utf-8", index=False)
                effectifs_ald = (indiv_rac_categ
                                 .groupBy("classe_age", var_ald)
                                 .agg(psf.countDistinct(psf.col("NUM_ENQ")))
                                 .withColumnRenamed("count(DISTINCT NUM_ENQ)", "effectif"))
                effectifs_ald.toPandas().to_csv(
                    path2output + "effectif_age_" + var_ald.lower() + "_" + categ + "_" + date + ".csv",
                    sep=";", encoding="utf-8", index=False)

                agg_dict_temp = agg_dict.copy()
                agg_dict_temp.pop(var_ald, None)
                ald_rac_categ = indiv_rac_categ.groupBy(var_ald, categ).agg(agg_dict_temp)
                ald_rac_categ = redefine_columns(ald_rac_categ)
                ald_rac_categ.toPandas().to_csv(
                    path2output + "depense_rac_" + var_ald + "_" + categ + "_" + date + ".csv",
                    sep=";", encoding="utf-8", index=False)

                ald_rac = indiv_rac_categ.groupBy(var_ald).agg(agg_dict_temp)
                ald_rac = redefine_columns(ald_rac)
                ald_rac.toPandas().to_csv(path2output + "depense_rac_" + var_ald + "_" + date + ".csv",
                                          sep=";", encoding="utf-8", index=False)
                effectifs_ald = (indiv_rac_categ
                                 .groupBy(var_ald)
                                 .agg(psf.countDistinct(psf.col("NUM_ENQ")))
                                 .withColumnRenamed("count(DISTINCT NUM_ENQ)", "effectif"))
                effectifs_ald.toPandas().to_csv(path2output + "effectif_" + var_ald.lower() + "_" + date + ".csv",
                                                sep=";", encoding="utf-8", index=False)

    if "groups" in type_indicator:
        grouping_var = [["top_ald"], ["top_ald", "top_sup_65_ans"], ["classe_age", "top_ald"]]
        for group in grouping_var:
            print("Depense/Rac par " + " ".join(group))

            (data_rac
             .withColumn("top_sup_65_ans", psf.when(psf.col("age") >= 65, 1).otherwise(0)).groupBy(group).agg(
                psf.countDistinct(psf.col("NUM_ENQ")))
             .withColumnRenamed("count(DISTINCT NUM_ENQ)", "effectif").toPandas()
             .to_csv(path2output + "effectifs_" + "_".join(group) + "_" + date + ".csv", sep=";", encoding="utf-8",
                     index=False))

            if "classe_age" not in group:
                group = group + ["separation_sdv_hospit"]

            rac_ald = (data_rac.withColumn("top_sup_65_ans", psf.when(psf.col("age") >= 65, 1).otherwise(0))
                       .groupBy(group).agg(agg_dict_amounts))
            rac_ald = redefine_columns(rac_ald)
            (rac_ald.toPandas()
             .to_csv(path2output + "comparaison_hcaam_" + "_".join(group) + "_" + date + ".csv", sep=";",
                     encoding="utf-8",
                     index=False))

    return None


def _compute_quantiles(df, nb_bucket=10):
    if 'decile_rac' in df.columns:
        print("Nothing was done: decile_rac already in df")
        return df

    df_indiv = df.groupBy("NUM_ENQ").agg({"rac_amo_sup": "sum"}).withColumnRenamed("sum(rac_amo_sup)", "rac_amo_sup")
    discretizer = pml.QuantileDiscretizer(numBuckets=nb_bucket, inputCol="rac_amo_sup", outputCol="decile_rac",
                                          relativeError=0)
    df_indiv = discretizer.fit(df_indiv).transform(df_indiv)
    df = df.join(df_indiv.select(["NUM_ENQ", "decile_rac"]), ["NUM_ENQ"], how="left")
    return df


def quantiles_indicators(data_rac, path2output, agg_dict_amounts, date, spark, sub_df="", list_categ = None):
    if list_categ == None:
        list_categ = ["poste_ag", "poste_racine"]
    print("Calculating deciles ..." + sub_df)
    data_rac = _compute_quantiles(data_rac)
    print("Done calculating deciles")

    # Rac by deciles
    (data_rac.groupBy("decile_rac", "separation_sdv_hospit").agg({"rac_amo_sup": "sum"})
     .withColumnRenamed("sum(rac_amo_sup)", "rac_amo_sup").toPandas()
     .to_csv(path2output + "moy_decile_" + sub_df + date + ".csv", sep=";", encoding="utf-8", index=False))

    (data_rac.groupBy("decile_rac").agg(psf.countDistinct(psf.col("NUM_ENQ")).alias("effectif")).toPandas()
     .to_csv(path2output + "effectifs_deciles_" + sub_df + date + ".csv", sep=";", encoding="utf-8", index=False))

    # Rac last centile
    print("Calculating last centile..." + sub_df)
    last_decile = (data_rac.filter(psf.col("decile_rac") == 9).drop("decile_rac"))
    last_decile = _compute_quantiles(last_decile)
    print("Done calculating centile")
    last_centile = last_decile.filter(psf.col("decile_rac") == 9)
    (last_centile.groupBy("separation_sdv_hospit").agg({"rac_amo_sup": "sum"})
     .withColumnRenamed("sum(rac_amo_sup)", "rac_amo_sup")
     .toPandas().to_csv(path2output + "rac_somme_dernier_centile_" + sub_df + date + ".csv",
                        sep=";",
                        encoding="utf-8", index=False))

    (last_centile.agg(psf.countDistinct(psf.col("NUM_ENQ")).alias("effectif")).toPandas()
     .to_csv(path2output + "effectifs_centile_" + sub_df + date + ".csv", sep=";",
             encoding="utf-8", index=False))

    # Quantile indicators by category
    for categ in list_categ:
        print("Amounts by deciles/centiles and " + categ)
        rac_dec_categ = data_rac.groupBy("decile_rac", categ).agg(agg_dict_amounts)
        rac_dec_categ = redefine_columns(rac_dec_categ)
        rac_dec_categ.toPandas().to_csv(path2output + "rac_decile_" + sub_df + categ + "_" + date + ".csv", sep=";",
                                        encoding="utf-8", index=False)

        rac_cent_categ = last_centile.groupBy(categ).agg(agg_dict_amounts)
        rac_cent_categ = redefine_columns(rac_cent_categ)
        rac_cent_categ.toPandas().to_csv(path2output + "rac_centile_" + sub_df + categ + "_" + date + ".csv", sep=";",
                                         encoding="utf-8", index=False)
    return None


def advanced_quantiles_indicators(path2data_rac, path2output, date, spark, liste_categ=None):
    if isinstance(path2data_rac, str):
        data_rac = spark.read.parquet(path2data_rac)
    else:
        data_rac = path2data_rac
    if liste_categ is None:
        liste_categ = [["poste_ag"], ["poste_racine"], ["poste_ag", "classe_age_10"], ["classe_age_10"]]
    rac_med = (data_rac.groupBy("NUM_ENQ").agg({"rac_amo_sup": "sum"})
               .withColumnRenamed('sum(rac_amo_sup)', 'rac_amo_sup')
               .agg(psf.expr('percentile_approx(rac_amo_sup, 0.5, 100000)').alias("median")))

    (rac_med.toPandas()
     .to_csv(path2output + "rac_median_" + date + ".csv", sep=";",
             encoding="utf-8", index=False))

    rac_min_dernier_centile = (data_rac.groupBy("NUM_ENQ").agg({"rac_amo_sup": "sum"})
                               .withColumnRenamed('sum(rac_amo_sup)', 'rac_amo_sup')
                               .agg(psf.expr('percentile_approx(rac_amo_sup, 0.99, 100000)').alias("min_last_cent")))

    (rac_min_dernier_centile.toPandas()
     .to_csv(path2output + "rac_min_dernier_centile_" + date + ".csv", sep=";",
             encoding="utf-8", index=False))

    for categ in liste_categ:
        rac_poste = (data_rac.groupBy(["NUM_ENQ"] + categ).agg({"rac_amo_sup": "sum"})
                     .withColumnRenamed('sum(rac_amo_sup)', 'rac_amo_sup'))

        rac_poste_mean = rac_poste.groupBy(categ).agg(psf.mean(psf.col("rac_amo_sup")).alias("rac_amo_sup_moy"))
        rac_poste_q1 = rac_poste.groupBy(categ).agg(
            psf.expr('percentile_approx(rac_amo_sup, 0.25, 100000)').alias("Q1"))
        rac_poste_q2 = rac_poste.groupBy(categ).agg(psf.expr('percentile_approx(rac_amo_sup, 0.5, 100000)').alias("Q2"))
        rac_poste_q3 = rac_poste.groupBy(categ).agg(
            psf.expr('percentile_approx(rac_amo_sup, 0.75, 100000)').alias("Q3"))

        quant_rac_poste = rac_poste_mean.join(rac_poste_q1, categ, how="left")
        quant_rac_poste = quant_rac_poste.join(rac_poste_q2, categ, how="left")
        quant_rac_poste = quant_rac_poste.join(rac_poste_q3, categ, how="left")
        print("Done calculating quartiles for" + " ".join(categ))
        (quant_rac_poste.toPandas()
         .to_csv(path2output + "quantiles_rac_" + "".join(categ) + "_" + date + ".csv", sep=";",
                 encoding="utf-8", index=False))

    return None


def apply_quantiles_indicators_sub_df(path2data_rac, path2output, date, agg_dict_amounts, spark, n_part=None):
    if isinstance(path2data_rac, str):
        data_rac = spark.read.parquet(path2data_rac)
    else:
        data_rac = path2data_rac
    if n_part is None:
        n_part = [50, 150]
    data_rac_ald = data_rac.filter(psf.col("top_ald") == 1)
    data_rac_ald = data_rac_ald.coalesce(n_part[0])
    data_rac_noald = data_rac.filter(psf.col("top_ald") == 0)
    data_rac_noald = data_rac_noald.coalesce(n_part[1])
    quantiles_indicators(data_rac_ald, path2output, agg_dict_amounts, date, spark, sub_df="ald_")
    quantiles_indicators(data_rac_noald, path2output, agg_dict_amounts, date, spark, sub_df="non_ald_")
    return None


def taux_recours(path2data_rac, path2output, date, agg_dict_amounts, spark):
    data_rac = spark.read.parquet(path2data_rac)

    data_rac = data_rac.withColumn("poste_semi_ag",
                                   psf.when(psf.col("poste").isin("Sage_femme_techniq", "Orthophoniste_techniq"),
                                            psf.lit("auxilliaires")).
                                   otherwise(psf.col("poste_semi_ag")))
    data_rac = data_rac.withColumn("poste_semi_ag",
                                   psf.when(psf.col("poste").isin("prevention"), psf.lit("prevention")).
                                   otherwise(psf.col("poste_semi_ag")))

    for categ in [["poste_ag"], ["poste_ag", "classe_age"], ["poste_ag", "top_ald"],
                  ["poste_ag", "classe_age", "top_ald"],
                  ["poste_semi_ag", "classe_age", "top_ald"],
                  ["poste_racine"], ["poste_racine", "classe_age"], ["poste_racine", "top_ald"],
                  ["poste_racine", "classe_age", "top_ald"],
                  ["poste_ag", "groupe_ALD"], ["poste_ag", "type_ALD"],
                  ["poste_racine", "groupe_ALD"], ["poste_racine", "type_ALD"],
                  ["poste_semi_ag", "groupe_ALD"], ["poste_semi_ag", "type_ALD"]]:
        print("Ventilation taux recours par " + "_".join(categ))
        effectifs_postag = (data_rac.groupBy(categ).agg(psf.countDistinct(psf.col("NUM_ENQ")))
                            .withColumnRenamed("count(DISTINCT NUM_ENQ)", "effectif")
                            .toPandas())
        effectifs_postag.to_csv(path2output + "taux_recours_" + "_".join(categ) + "_" + date + ".csv", sep=";",
                                encoding="utf-8")
    return None


def analyse_groupe_ald(path2data_rac, path2output, date, agg_dict_amounts, spark, type_indicator=None):
    if type_indicator is None:
        type_indicator = ["decile", "tercile"]
    data_rac = spark.read.parquet(path2data_rac)
    data_rac_ald = data_rac.filter(psf.col("top_ald") == 1)
    data_rac_ald = _compute_quantiles(data_rac_ald)

    if "tercile" in type_indicator:
        dep_rac_ald_tercile = _compute_quantiles(data_rac_ald.drop("decile_rac"), nb_bucket=3)
        dep_rac_ald_tercile = dep_rac_ald_tercile.withColumnRenamed("decile_rac", "tercile_rac")

        (dep_rac_ald_tercile
         .groupBy("tercile_rac")
         .agg({"rac_amo_sup": "sum"})
         .withColumnRenamed("sum(rac_amo_sup)", "rac_amo_sup")
         .toPandas()
         .to_csv(path2output + "moy_tercile_ald_" + date + ".csv", sep=";", encoding="utf-8",
                 index=False))

        (dep_rac_ald_tercile
         .groupBy("tercile_rac")
         .agg(psf.countDistinct(psf.col("NUM_ENQ")).alias("effectif"))
         .toPandas()
         .to_csv(path2output + "effectifs_terciles_ald_" + date + ".csv", sep=";", encoding="utf-8",
                 index=False))

    for var_ald in ["groupe_ALD", "type_ALD"]:

        rac_ald_dec = data_rac_ald.groupBy("NUM_ENQ", var_ald).agg(
            {"dep_tot": "sum", "rac": "sum", "rac_amo_sup": "sum", "rac_opposable": "sum",
             "rac_lien_ald": "sum", "dep_lien_ald": "sum", "depass_c": "sum",
             "separation_sdv_hospit": "first", "decile_rac": "first"})
        rac_ald_dec = redefine_columns(rac_ald_dec)

        if "decile" in type_indicator:
            print("Deciles par " + var_ald)
            moy_rac_ald_dec = (rac_ald_dec.groupBy("decile_rac", var_ald)
                               .agg({"dep_tot": "sum", "rac": "sum", "rac_amo_sup": "sum",
                                     "rac_opposable": "sum", "rac_lien_ald": "sum", "dep_lien_ald": "sum",
                                     "depass_c": "sum",
                                     "separation_sdv_hospit": "first", "NUM_ENQ": "count"}))
            moy_rac_ald_dec = redefine_columns(moy_rac_ald_dec)
            moy_rac_ald_dec = moy_rac_ald_dec.withColumnRenamed("NUM_ENQ", "effectif")
            moy_rac_ald_dec.toPandas().to_csv(path2output + "deciles_rac_" + var_ald + "_" + date + ".csv", sep=";",
                                              encoding="utf-8", index=False)

            tot_ben_ald_centile = data_rac_ald.filter(psf.col("decile_rac") == 9).drop("decile_rac")
            tot_ben_ald_centile = _compute_quantiles(tot_ben_ald_centile)
            print("Done calculating centile")
            tot_ben_ald_centile = tot_ben_ald_centile.filter(psf.col("decile_rac") == 9)

            rac_ald_cent = tot_ben_ald_centile.groupBy("NUM_ENQ", var_ald).agg(
                {"dep_tot": "sum", "rac": "sum", "rac_amo_sup": "sum", "rac_opposable": "sum",
                 "rac_lien_ald": "sum", "dep_lien_ald": "sum", "depass_c": "sum",
                 "separation_sdv_hospit": "first", "decile_rac": "first"})
            rac_ald_cent = redefine_columns(rac_ald_cent)

            moy_rac_ald_cent = (rac_ald_cent.groupBy("decile_rac", var_ald)
                                .agg({"dep_tot": "sum", "rac": "sum", "rac_amo_sup": "sum",
                                      "rac_opposable": "sum", "rac_lien_ald": "sum", "dep_lien_ald": "sum",
                                      "depass_c": "sum",
                                      "separation_sdv_hospit": "first", "NUM_ENQ": "count"}))
            moy_rac_ald_cent = redefine_columns(moy_rac_ald_cent)
            moy_rac_ald_cent = moy_rac_ald_cent.withColumnRenamed("NUM_ENQ", "effectif")
            moy_rac_ald_cent.toPandas().to_csv(path2output + "dernier_centile_rac_" + var_ald + "_" + date + ".csv",
                                               sep=";", encoding="utf-8", index=False)

    return None


def enumerate_type_ald(path2data_rac, path2output, date, spark):
    data_rac = spark.read.parquet(path2data_rac)
    liste_ald = list(range(0, 33))
    liste_ald = [str(i) for i in liste_ald]
    liste_ald.append("99")
    for i in liste_ald:
        #print(i)
        data_rac = data_rac.withColumn("contains_ald" + i,
                                       psf.array_contains(psf.col("NUM_ALD"), i))
    data_rac = data_rac.withColumn("contains_ald" + '99',
                                   psf.array_contains(psf.col("NUM_ALD"), '99'))
    effectif_ald = list([0] * 33)
    for i in range(0, 33):
        #print(i) 
        effectif_ald[i] = data_rac.filter(psf.col("contains_ald" + str(i)) == True).select("NUM_ENQ").distinct().count()
    effectif_ald = pd.DataFrame(effectif_ald)
    effectif_ald.to_csv(path2output + "prevalence_type_ald_" + date + ".csv", sep=";", encoding="utf-8")
    return None


def effectifs_groupe_ald(path2data_rac, path2output, date, spark):
    data_rac = spark.read.parquet(path2data_rac)

    # Ajouter une unique variable indiquant le groupe d'ALD - groupes non mutuellement exclusifs
    data_rac = data_rac.withColumn("groupe_ALD_non_excl",
                                   psf.when(psf.col('cardio_neurovasc') == 1, psf.lit("cardio_neurovasc")).
                                   when(psf.col('diabete') == 1, psf.lit("diabete")).
                                   when(psf.col('tumeurs') == 1, psf.lit("tumeurs")).
                                   when(psf.col('affection_psy') == 1, psf.lit("affection_psy")).
                                   when(psf.col("top_ald") == 1, psf.lit("autres_ald")).
                                   otherwise(psf.lit("non_ald")))
    (data_rac.groupBy("groupe_ALD_non_excl").agg(psf.countDistinct(psf.col("NUM_ENQ")))
     .withColumnRenamed("count(DISTINCT NUM_ENQ)", "effectif")
     .toPandas()
     .to_csv(path2output + "prevalence_groupe_ald_non_exclusifs_" + date + ".csv", sep=";",
             encoding="utf-8", index=False))
    return None


def effectifs_ald_rg(path2data_rac, path2flat, path2output, year, date, spark):
    data_rac = spark.read.parquet(path2data_rac)
    ir_ben = (spark.read
              .parquet(path2flat + "single_table/IR_BEN_R")
              .select(["NUM_ENQ", "ORG_AFF_BEN"])
              )
    ir_ben = ir_ben.withColumn("RG", psf.when(psf.substring(psf.col("ORG_AFF_BEN"), 1, 2) == "01", psf.lit(1)).
                               otherwise(psf.lit(0)))

    # Fusionner avec les données et rechercher les ALD au RG
    data_rac = (data_rac.join(ir_ben.select(["NUM_ENQ", "RG"]), ["NUM_ENQ"], how="left")
                        .withColumn("RG", psf.when(psf.col("RG").isNull(), psf.lit(0)).otherwise(psf.col("RG")))
                        .groupBy("NUM_ENQ").agg({"top_ald": "max", "RG": "max"})
                        .withColumnRenamed("max(top_ald)", "top_ald").withColumnRenamed("max(RG)", "RG"))
    (data_rac.groupBy("top_ald", "RG").agg(psf.countDistinct(psf.col("NUM_ENQ")))
     .withColumnRenamed("count(DISTINCT NUM_ENQ)", "effectif")
     .toPandas()
     .to_csv(path2output + "effectifs_ald_RG_" + date + ".csv", sep=";", encoding="utf-8", index=False))
    return None

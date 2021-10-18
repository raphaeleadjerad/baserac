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
from .indicators import redefine_columns
from .indicators import _compute_quantiles

def global_indicators(data_rac, path2output, date, spark, type_indicator=None):
    if type_indicator is None:
        type_indicator = ["age", "age_bb", "aides", "expenditure"]
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

    if "age_bb" in type_indicator:
        print("Age_bb")
        age = (data_rac.groupBy("NUM_ENQ").agg({"classe_age_bb": "first"})
               .withColumnRenamed("first(classe_age_bb)", "classe_age_bb")
               .cube("classe_age_bb").count().sort("count", ascending=False)
               .toPandas())
        age.to_csv(path2output + "pyramide_age_nourrissons_" + date + ".csv", sep=";", encoding="utf-8", index=False)

        age = (data_rac.groupBy("NUM_ENQ").agg({"classe_age_bb": "first", "BEN_SEX_COD": "first"})
               .withColumnRenamed('first(classe_age_bb)', 'classe_age_bb')
               .withColumnRenamed('first(BEN_SEX_COD)', 'BEN_SEX_COD'))

        for i in [1, 2]:
            temp = (age.filter(psf.col("BEN_SEX_COD") == i).cube("classe_age_bb").count().sort("count", ascending=False)
                    .toPandas())
            temp.to_csv(path2output + "pyramide_classe_age_nourrissons_" + str(i) + "_" + date + ".csv", sep=";",
                        encoding="utf-8", index=False)
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

        temp0 = (data_rac.withColumn("part_sup", psf.col("rac") - psf.col("rac_amo_sup"))
                 .withColumn("mnt_tot_rem", psf.col("remb_am") + psf.col("part_sup")))

        # poste
        temp = (temp0.groupBy("poste").agg(
            {"dep_tot": "sum", "poste_ag": "first",
             "poste_hospit": "first", "poste_hospit2": "first", "poste_hospit3": "first",
             "poste_racine_hospit": "first", "poste_racine": "first", "Poste_cns": "first",
             "tot_rem_bse": "sum", "remb_am": "sum", "mnt_tot_rem": "sum"}))
        temp = redefine_columns(temp)
        (temp
         .toPandas()
         .to_csv(path2output + "depense_poste_" + date + ".csv", sep=";",
                 encoding="utf-8", index=False)
         )

        for type_poste in ["poste_hospit", "poste_hospit2", "poste_hospit3", "poste_racine_hospit"]:
            temp = (temp0.groupBy(type_poste).agg(
                {"dep_tot": "sum", "tot_rem_bse": "sum", "remb_am": "sum",
                 "mnt_tot_rem": "sum"}))
            temp = redefine_columns(temp)
            (temp
             .toPandas()
             .to_csv(path2output + "depense_" + type_poste + "_" + date + ".csv", sep=";",
                     encoding="utf-8", index=False)
             )

    return None


def ventilated_indicators(data_rac, path2output, agg_dict, agg_dict_amounts, date, spark,
                          type_indicator=None):
    if type_indicator is None:
        type_indicator = ["poste", "analyse_ald", "groups"]
    for categ in ["poste_ag", "poste_hospit", "poste_hospit2", "poste_hospit3", "poste_racine_hospit",
                  "poste_racine_hospit_bis", "poste_racine", "poste_tous_es", "poste_tous_es_bis"]:

        indiv_rac_categ = data_rac.groupBy("NUM_ENQ", categ).agg(agg_dict)
        indiv_rac_categ = redefine_columns(indiv_rac_categ)
        indiv_rac_categ = indiv_rac_categ.withColumn("top_sup_65_ans", psf.when(psf.col("age") >= 65, 1).otherwise(0))

        if "poste" in type_indicator and categ in ["poste_racine_hospit_bis"]:
            print("Depense/Rac par " + categ + " age")
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

            print("Depense/Rac par " + categ + " age_bb")
            agg_dict_temp = agg_dict.copy()
            agg_dict_temp.pop("classe_age_bb", None)
            age_rac_categ = indiv_rac_categ.groupBy("classe_age_bb", categ).agg(agg_dict_temp)
            age_rac_categ = redefine_columns(age_rac_categ)
            age_rac_categ.toPandas().to_csv(path2output + "depense_rac_age_nourrissons_" + categ + "_" + date + ".csv",
                                            sep=";", encoding="utf-8", index=False)

            agg_dict_temp = agg_dict.copy()
            agg_dict_temp.pop("classe_age_bb", None)
            agg_dict_temp.pop("top_ald", None)
            age_ald_rac_categ = indiv_rac_categ.groupBy("classe_age_bb", "top_ald", categ).agg(agg_dict_temp)
            age_ald_rac_categ = redefine_columns(age_ald_rac_categ)
            age_ald_rac_categ.toPandas().to_csv(
                path2output + "depense_rac_age_nourrissons_ald_" + categ + "_" + date + ".csv",
                sep=";", encoding="utf-8", index=False)

    if "groups" in type_indicator:
        grouping_var = [["top_ald"], ["classe_age", "top_ald"]]
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

            rac_ald = (data_rac.groupBy(group).agg(agg_dict_amounts))
            rac_ald = redefine_columns(rac_ald)
            (rac_ald.toPandas()
             .to_csv(path2output + "comparaison_hcaam_" + "_".join(group) + "_" + date + ".csv", sep=";",
                     encoding="utf-8",
                     index=False))

    return None




def taux_recours(data_rac, path2output, date, agg_dict_amounts, spark):
    for categ in [["poste_ag"], ["poste_ag", "classe_age"], ["poste_ag", "classe_age_bb"], ["poste_ag", "top_ald"],
                  ["poste_ag", "classe_age", "top_ald"], ["poste_ag", "classe_age_bb", "top_ald"],
                  ["poste_hospit"], ["poste_hospit", "classe_age"], ["poste_hospit", "classe_age_bb"],
                  ["poste_hospit", "top_ald"],
                  ["poste_hospit", "classe_age", "top_ald"], ["poste_hospit", "classe_age_bb", "top_ald"],
                  ["poste_hospit2"], ["poste_hospit2", "classe_age"], ["poste_hospit2", "classe_age_bb"],
                  ["poste_hospit2", "top_ald"],
                  ["poste_hospit2", "classe_age", "top_ald"], ["poste_hospit2", "classe_age_bb", "top_ald"],
                  ["poste_hospit3"], ["poste_hospit3", "classe_age"], ["poste_hospit3", "classe_age_bb"],
                  ["poste_hospit3", "top_ald"],
                  ["poste_hospit3", "classe_age", "top_ald"], ["poste_hospit3", "classe_age_bb", "top_ald"],
                  ["poste_racine_hospit"], ["poste_racine_hospit", "classe_age"],
                  ["poste_racine_hospit", "classe_age_bb"], ["poste_racine_hospit", "top_ald"],
                  ["poste_racine_hospit", "classe_age", "top_ald"], ["poste_racine_hospit", "classe_age_bb", "top_ald"],
                  ["poste_racine_hospit_bis"], ["poste_racine_hospit_bis", "classe_age"],
                  ["poste_racine_hospit_bis", "classe_age_bb"], ["poste_racine_hospit_bis", "top_ald"],
                  ["poste_racine_hospit_bis", "classe_age", "top_ald"],
                  ["poste_racine_hospit_bis", "classe_age_bb", "top_ald"],
                  ["poste_racine"], ["poste_racine", "classe_age"], ["poste_racine", "classe_age_bb"],
                  ["poste_racine", "top_ald"],
                  ["poste_racine", "classe_age", "top_ald"], ["poste_racine", "classe_age_bb", "top_ald"],
                  ["poste_tous_es"], ["poste_tous_es", "classe_age"], ["poste_tous_es", "classe_age_bb"],
                  ["poste_tous_es", "top_ald"],
                  ["poste_tous_es", "classe_age", "top_ald"], ["poste_tous_es", "classe_age_bb", "top_ald"],
                  ["poste_tous_es_bis"], ["poste_tous_es_bis", "classe_age"], ["poste_tous_es_bis", "classe_age_bb"],
                  ["poste_tous_es_bis", "top_ald"],
                  ["poste_tous_es_bis", "classe_age", "top_ald"], ["poste_tous_es_bis", "classe_age_bb", "top_ald"]]:
        print("Ventilation taux recours par " + "_".join(categ))
        effectifs_postag = (data_rac.groupBy(categ).agg(psf.countDistinct(psf.col("NUM_ENQ")))
                            .withColumnRenamed("count(DISTINCT NUM_ENQ)", "effectif")
                            .toPandas())
        effectifs_postag.to_csv(path2output + "taux_recours_" + "_".join(categ) + "_" + date + ".csv", sep=";",
                                encoding="utf-8")
    return None


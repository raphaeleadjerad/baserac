# -*- coding: utf-8 -*-


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

# Modules
from baserac import indicators
from baserac import utils
from time import time
import pyspark.sql.functions as psf

# Parameters
year = 2017
date = "20201130"

path2flat = "/home/commun/echange/flattening_dcirs" + str(year)[-2:] + "/"
path2output = "/home/commun/echange/raphaele_adjerad/results/indicators_" + str(year) + "/"
path2data_rac = "/home/commun/echange/raphaele_adjerad/results/rac_chainee_" + str(year)
agg_dict = {"dep_tot": "sum", "rac": "sum", "rac_amo_sup": "sum", "rac_opposable": "sum",
            "classe_age": "first", "top_ald": "max", "age": "first", "separation_sdv_hospit": "first",
            "rac_lien_ald": "sum", "dep_lien_ald": "sum", "depass_c": "sum", "groupe_ALD": "first",
            "type_ALD": "first"}
agg_dict_amounts = {"dep_tot": "sum", "rac": "sum", "rac_amo_sup": "sum", "rac_opposable": "sum",
                    "rac_lien_ald": "sum", "dep_lien_ald": "sum", "depass_c": "sum",
                    "separation_sdv_hospit": "first"}

spark = utils.start_context(cores=20, partitions=200, mem='50g', name_app="indicators")

data_rac = spark.read.parquet(path2data_rac)
data_rac = data_rac.withColumn("Poste_cns",
                               psf.when(psf.col("poste") == "rip_sejour", "consos_soins_hospitaliers_publics")
                               .when(psf.col("poste") == "Orthophoniste_techniq", "conso_autres_soins_auxi")
                               .when(psf.col("poste") == "Orthophoniste_cliniq", "conso_autres_soins_auxi")
                               .when(psf.col("poste") == "prevention", "prevention")
                               .when(psf.col("poste") == "Sage_femme_techniq", "conso_sages_femmes")
                               .otherwise(psf.col("Poste_cns")))
data_rac = data_rac.withColumn("poste", psf.when((psf.col("poste").isNull()) & (psf.col("poste_ag") == "dentaire"),
                                                 "actes de chirurgie").
                               otherwise(psf.col("poste")))


def get_quantile(q, acc=100000):
    indiv = data_rac.groupBy("NUM_ENQ").agg({"rac_amo_sup": "sum"})
    indiv = indiv.withColumnRenamed("sum(rac_amo_sup)", "rac_amo_sup")
    q_val = indiv.agg(psf.expr('percentile_approx(rac_amo_sup, ' + str(q) + ', ' + str(acc) + ')').alias("q_val"))
    return q_val


t1 = time()
data_rac.write.parquet(path2data_rac + "_c")
t2 = time()
print("Write time : {}".format((t2 - t1) / 60))  # 18 min

path2data_rac = path2data_rac + "_c"

t1 = time()
indicators.global_indicators(path2data_rac, path2output, date, spark, type_indicator=["expenditure"])
t2 = time()
print("Write time : {}".format((t2 - t1) / 60))
t1 = time()
indicators.ventilated_indicators(path2data_rac, path2output, agg_dict, agg_dict_amounts, date, spark,
                                 type_indicator="analyse_ald")
t2 = time()
print("Write time : {} min".format((t2 - t1) / 60))

t1 = time()
data_rac = spark.read.parquet(path2data_rac)
indicators.quantiles_indicators(data_rac, path2output, agg_dict_amounts, date, spark)
t2 = time()
print("Write time : {}".format((t2 - t1) / 60))

t1 = time()
indicators.advanced_quantiles_indicators(path2data_rac, path2output, date, spark)
t2 = time()
print("Write time : {}".format((t2 - t1) / 60))

t1 = time()
indicators.apply_quantiles_indicators_sub_df(path2data_rac, path2output, date,
                                             agg_dict_amounts, spark)
t2 = time()
print("Write time : {}".format((t2 - t1) / 60))

t1 = time()
indicators.taux_recours(path2data_rac, path2output, date, agg_dict_amounts, spark)
t2 = time()
print("Write time : {}".format((t2 - t1) / 60))

t1 = time()
indicators.analyse_groupe_ald(path2data_rac, path2output, date, agg_dict_amounts, spark, type_indicator=["decile"])
t2 = time()
print("Write time : {}".format((t2 - t1) / 60))

t1 = time()
indicators.enumerate_type_ald(path2data_rac, path2output, date, spark)
t2 = time()
print("Write time : {}".format((t2 - t1) / 60))

t1 = time()
indicators.effectifs_groupe_ald(path2data_rac, path2output, date, spark)
t2 = time()
print("Write time : {}".format((t2 - t1) / 60))

t1 = time()
indicators.effectifs_ald_rg(path2data_rac, path2flat, path2output, year, date, spark)
t2 = time()
print("Write time : {}".format((t2 - t1) / 60))

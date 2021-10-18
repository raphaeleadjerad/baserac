
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


def _load_ucd(path2_ucd, spark):
    ucd = (spark.read.option("header", "true")
           .option("sep", ";")
           .csv(path2_ucd)
           .withColumn("date_insc", psf.to_timestamp(psf.col("date_insc"), "yyyy-MM-dd"))
           .withColumn("date_rad", psf.to_timestamp(psf.col("date_rad"), "yyyy-MM-dd"))
           .withColumn("prix_HT", psf.regexp_replace(psf.col("prix_HT"), ',', '.'))
           .withColumn("prix_TTC", psf.regexp_replace(psf.col("prix_TTC"), ',', '.'))
           .withColumn("prix_HT", psf.col("prix_HT").cast(pst.DoubleType()))
           .withColumn("prix_TTC", psf.col("prix_TTC").cast(pst.DoubleType()))
           )
    ucd = ucd.coalesce(1)
    return ucd


def _compare_ucd(df, path2_ucd, year, spark):
    # Importer la table des médicaments en SUS
    ucd = _load_ucd(path2_ucd, spark)
    df = df.withColumnRenamed("UCD_UCD_COD", "UCD_13").join(ucd, ["UCD_13"], how="left")
    # Creer un top LES (liste en SUS) : 1 si dans la liste, 0 sinon
    df = df.withColumn("top_LES",
                       # Gestion des medicaments qui ne changent pas de statut au cours de l'année
                       psf.when((psf.col("date_insc") > str(year) + "-12-31") |
                                (psf.col("date_insc").isNull()), 0).  # non inscrits avant année
                       when(psf.col("date_rad") < str(year) + "-01-01", 0).  # radiés avant année
                       # Gestion des médicaments radiés en cours d'année
                       when((psf.col("ann_rad") == psf.col("DAT_ADM_ANN")) &
                            (psf.col("ADM_MOIS") > psf.col("mois_rad")), 0).
                       # Gestion des médicaments inscrits en cours d'année
                       when((psf.col("ann_insc") == psf.col("DAT_ADM_ANN")) &
                            (psf.col("ADM_MOIS") < psf.col("mois_insc")), 0).
                       otherwise(1)
                       )

    # Supprimer medicaments hors liste en sus
    df = df.filter(psf.col("top_LES") != 0)
    df = df.drop(psf.col("top_LES"))
    return df


def _load_ir_orc(path2_ir_orc, year, spark):
    ir_orc = (spark.read
              .option("header", "true")
              .option("sep", ";")
              .csv(path2_ir_orc + str(year) + "/REF/IR_ORC_R.CSV")
              .select(["NUM_ENQ", "BEN_CMU_ORG", "BEN_CTA_TYP", "MLL_CTA_DSD", "MLL_CTA_DSF"])
              .withColumn("MLL_CTA_DSD", psf.to_timestamp(psf.col("MLL_CTA_DSD"), "dd/MM/yyyy"))
              .withColumn("MLL_CTA_DSF", psf.to_timestamp(psf.col("MLL_CTA_DSF"), "dd/MM/yyyy"))
              .filter((psf.year(psf.col("MLL_CTA_DSD")) <= year) &
                      ((psf.year(psf.col("MLL_CTA_DSF")) >= year) | (psf.col("MLL_CTA_DSF").isNull())) &
                      (psf.col("BEN_CTA_TYP") == "89"))
              .withColumn("top_cmu", psf.lit(1))
              .select(["NUM_ENQ", "top_cmu"]).dropDuplicates()
              )
    return ir_orc


def _load_ir_imb_unity(path2flat, path2ref, year, spark):
    filtre_prise_en_charge = [41, 43, 45]
    ir_cim = (spark.read
              .option("header", "true")
              .option("sep", ";")
              .csv(path2ref + "IR_CIM_V.csv")
              .withColumnRenamed("CIM_COD", "MED_MTF_COD")
              )
    ir_imb = (spark.read
              .parquet(path2flat + "single_table/IR_IMB_R")
              .select(["NUM_ENQ", "IMB_ALD_NUM", "MED_MTF_COD", "IMB_ALD_DTD", "IMB_ALD_DTF", "IMB_ETM_NAT"])
              .withColumn("IMB_ALD_DTD", psf.to_timestamp(psf.col("IMB_ALD_DTD"), "dd/MM/yyyy"))
              .withColumn("IMB_ALD_DTF", psf.to_timestamp(psf.col("IMB_ALD_DTF"), "dd/MM/yyyy"))
              .filter((psf.year(psf.col("IMB_ALD_DTD")) <= year) &
                      ((psf.year(psf.col("IMB_ALD_DTF")) >= year) | (psf.col("IMB_ALD_DTF").isNull()) | (
                              psf.year(psf.col("IMB_ALD_DTF")) == 1600)) &
                      (psf.col("IMB_ETM_NAT").isin(filtre_prise_en_charge)))
              .join(ir_cim.select(["MED_MTF_COD", "ALD_030_COD"]), ["MED_MTF_COD"], how="left")
              .withColumn("NUM_ALD",
                          psf.when((psf.col("IMB_ALD_NUM") > 0) & (psf.col("IMB_ALD_NUM") <= 32),
                                   psf.col("IMB_ALD_NUM")).
                          when((psf.col("ALD_030_COD") > 0) & (psf.col("ALD_030_COD") <= 30), psf.col("ALD_030_COD")).
                          when(psf.col("IMB_ETM_NAT") == 43, psf.lit(31)).
                          when(psf.col("IMB_ETM_NAT") == 45, psf.lit(
                              32)).
                          otherwise(psf.lit(99)))
              )

    ir_imb_unity_ct = (ir_imb.groupBy(psf.col("NUM_ENQ")).agg(psf.countDistinct(psf.col("NUM_ALD")))
                       .withColumnRenamed("count(DISTINCT NUM_ALD)", "nb_ald")
                       .withColumnRenamed("count(NUM_ALD)", "nb_ald")) # depend renommage
    ir_imb_unity = (ir_imb.groupBy(psf.col("NUM_ENQ")).agg({"NUM_ALD": "collect_set"})
                    .withColumnRenamed("collect_set(NUM_ALD)", "NUM_ALD")
                    .join(ir_imb_unity_ct, ["NUM_ENQ"], how="inner")
                    .withColumn("top_ald",
                                psf.when(psf.col("nb_ald") > 0, psf.lit(1)).
                                otherwise(psf.lit(0)))
                    )
    return ir_imb_unity


def union_hospit(path2output, year, n_partitions, *args):
    base_rac_hospit = union_all(*args)
    print("Start writing parquet ...")
    t1 = time()
    base_rac_hospit = base_rac_hospit.repartition(n_partitions)
    base_rac_hospit.write.parquet(path2output)
    t2 = time()
    print("Done in {} min".format((t2 - t1) / 60))
    return None


def add_month_stay(path2flat, path2csv_ssr, year, spark):
    if year not in [2016, 2017]:
        raise ValueError("Année non prise en charge")
    mco_c = (spark.read
             .parquet(path2flat + "single_table/MCO_C/year=" + str(year))
             .select(["NUM_ENQ", "EXE_SOI_DTD", "EXE_SOI_DTF"])
             .withColumn("mois_sej_deb", psf.when(psf.year(psf.col("EXE_SOI_DTD")) < year, 1).
                         otherwise(psf.month(psf.col("EXE_SOI_DTD"))))
             .withColumn("mois_sej_fin", psf.when(psf.year(psf.col("EXE_SOI_DTF")) > year, 12).
                         otherwise(psf.month(psf.col("EXE_SOI_DTF"))))
             )
    had_c = (spark.read
             .parquet(path2flat + "single_table/HAD_C/year=" + str(year))
             .select(["NUM_ENQ", "EXE_SOI_DTD", "EXE_SOI_DTF"])
             .withColumn("mois_sej_deb", psf.when(psf.year(psf.col("EXE_SOI_DTD")) < year, 1).
                         otherwise(psf.month(psf.col("EXE_SOI_DTD"))))
             .withColumn("mois_sej_fin", psf.when(psf.year(psf.col("EXE_SOI_DTF")) > year, 12).
                         otherwise(psf.month(psf.col("EXE_SOI_DTF"))))
             )
    rip_c = (spark.read
             .parquet(path2flat + "single_table/RIP_C/year=" + str(year))
             .select(["NUM_ENQ", "EXE_SOI_DTD", "EXE_SOI_DTF"])
             .withColumn("mois_sej_deb", psf.when(psf.year(psf.col("EXE_SOI_DTD")) < year, 1).
                         otherwise(psf.month(psf.col("EXE_SOI_DTD"))))
             .withColumn("mois_sej_fin", psf.when(psf.year(psf.col("EXE_SOI_DTF")) > year, 12).
                         otherwise(psf.month(psf.col("EXE_SOI_DTF"))))
             )
    ssr_c = (spark.read
             .parquet(path2flat + "single_table/SSR_C/year=" + str(year))
             .select(["NUM_ENQ", "EXE_SOI_DTD", "EXE_SOI_DTF"])
             .withColumn("mois_sej_deb", psf.when(psf.year(psf.col("EXE_SOI_DTD")) < year, 1).
                         otherwise(psf.month(psf.col("EXE_SOI_DTD"))))
             .withColumn("mois_sej_fin", psf.when(psf.year(psf.col("EXE_SOI_DTF")) > year, 12).
                         otherwise(psf.month(psf.col("EXE_SOI_DTF"))))
             )
    etab_pub = union_all(mco_c, had_c, rip_c, ssr_c)
    etab_pub = etab_pub.groupBy(psf.col("NUM_ENQ"), psf.col("mois_sej_deb"), psf.col("mois_sej_fin")).agg(
        {"NUM_ENQ": "COUNT"})

    ace_c = (spark.read
             .parquet(path2flat + "single_table/MCO_CSTC/year=" + str(year))
             .select(["NUM_ENQ", "ETA_NUM", "SEQ_NUM", "EXE_SOI_DTD", "EXE_SOI_DTF"])
             .withColumnRenamed("SEQ_NUM", "RSA_NUM")
             )
    fbstc = (spark.read
             .parquet(path2flat + "single_table/MCO_FBSTC/year=" + str(year))
             .select(["ETA_NUM", "SEQ_NUM", "ACT_COD", "AMC_MNR", "AMO_MNR",
                      "HON_MNT", "NUM_FAC", "REM_BAS", "REM_TAU"])
             .withColumnRenamed("SEQ_NUM", "RSA_NUM")
             .select(["ETA_NUM", "RSA_NUM", "ACT_COD"])
             .withColumn("ATU",
                         psf.when(psf.col("ACT_COD") == "ATU", 1).otherwise(0))
             .groupBy(["ETA_NUM", "RSA_NUM"]).agg({"ATU": "max"})
             .withColumnRenamed("max(ATU)", "ATU")
             )

    # Merge avec ace_c et creation d'une variable indiquant s'il s'agit d'un passage aux urgences
    ace_c = (ace_c.join(fbstc, ["ETA_NUM", "RSA_NUM"], how="left")
             .withColumn("ATU",
                         psf.when(psf.col("ATU") == 1, psf.col("ATU")).
                         otherwise(0))
             .withColumn("poste_racine", psf.when(psf.col("ATU") == 1, "urgences").
                         otherwise("autres_soins_ville"))
             .withColumn("mois_soins", psf.month(psf.col("EXE_SOI_DTD")))
             .select(['NUM_ENQ', "poste_racine", "mois_soins"]))

    ace_ssr = (spark.read
               .parquet(path2flat + "single_table/SSR_CSTC/year=" + str(year))
               .select(["NUM_ENQ", "ETA_NUM", "SEQ_NUM", "EXE_SOI_DTD", "EXE_SOI_DTF"])
               .withColumnRenamed("SEQ_NUM", "RHA_NUM")
               )

    if year == 2016:
        chemin_csv_fbstc = path2csv_ssr + "fbstc.csv"
    elif year == 2017:
        chemin_csv_fbstc = path2csv_ssr + "FBSTC.CSV"
    else:
        raise ValueError("Erreur année non prise en charge")
    fbstc = (spark.read
             .option("header", "true")
             .option("sep", ";")
             .csv(chemin_csv_fbstc)
             .select(
              ["ETA_NUM", "SEQ_NUM", "ACT_COD", "AMC_MNR", "AMO_MNR", "HON_MNT", "NUM_FAC", "REM_BAS", "REM_TAU"])
             .withColumnRenamed("SEQ_NUM", "RHA_NUM")
             .select(["ETA_NUM", "RHA_NUM", "ACT_COD"])
             .withColumn("ATU",
                         psf.when(psf.col("ACT_COD") == "ATU", 1).
                         otherwise(0))
             .groupBy(["ETA_NUM", "RHA_NUM"]).agg({"ATU": "max"})
             .withColumnRenamed("max(ATU)", "ATU")
             )
    ace_ssr = (ace_ssr.join(fbstc, ["ETA_NUM", "RHA_NUM"], how="left")
               .withColumn("ATU", psf.when(psf.col("ATU") == 1, psf.col("ATU")).
                           otherwise(0))
               .withColumn("poste_racine", psf.when(psf.col("ATU") == 1, "urgences").
                           otherwise("autres_soins_ville"))
               .withColumn("mois_soins", psf.month(psf.col("EXE_SOI_DTD")))
               .select(['NUM_ENQ', "poste_racine", "mois_soins"]))
    ace = (ace_ssr.union(ace_c)
           .groupBy(psf.col("NUM_ENQ"), psf.col("poste_racine"), psf.col("mois_soins"))
           .agg({"NUM_ENQ": "COUNT"}))

    return {"etab_pub": etab_pub, "ace": ace}

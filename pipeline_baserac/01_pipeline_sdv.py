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
from baserac import sdv
from baserac import agg_sdv
from baserac import utils
from time import time

# Parameters
year = 2017
start_time = time()
path2flat = "/home/commun/echange/flattening_dcirs"
path2nomenclature = "/home/commun/echange/raphaele_adjerad/tables_correspondance/"
path2ref = "/home/commun/référentiels/SNIIRAM/"
path2output = "/home/commun/echange/raphaele_adjerad/results/temp_" + str(year) + "/"
if year == 2017:
    path_ns_cam = path2flat + str(year)[-2:] + "/single_table/NS_CAM_F/"
    path_ns_tip = path2flat + str(year)[-2:] + "/single_table/NS_TIP_F/"
elif year == 2016:
    path_ns_cam = path2flat + str(year)[-2:] + "/single_table/ER_CAM_F/year=" + str(year)
    path_ns_tip = path2flat + str(year)[-2:] + "/single_table/ER_TIP_F/year=" + str(year)
else:
    raise ValueError("Année non prise en charge")
prs_lpp = ["optique_autre",
           "audio_autre"]
spe_act_det_dent = ["Dentiste", "chir_dent_stom"]
actes_cli_tech = ["cliniq", "techniq"]
agg_dict = {"PRS_PAI_MNT": "sum", "TOT_REM_BSE": "sum", "TOT_REM_MNT": "sum", "ARO_REM_MNT": "sum",
            "PRS_ACT_QTE": "sum",
            "top_cmu": "max", "EXO_MTF": "first", "top_acs": "max", "top_exo_mtf": "max", "BEN_AMA_COD": "max",
            "dep_lien_ald": "sum", "mnt_rem_lien_ald": "sum", "aro_rem_lien_ald": "sum"}
agg_dict_dent = {"PRS_PAI_MNT": "sum", "TOT_REM_BSE": "sum", "TOT_REM_MNT": "sum", "ARO_REM_MNT": "sum",
                 "sum(CAM_ACT_QSN)": "sum", "top_cmu": "max", "EXO_MTF": "first", "top_acs": "max",
                 "top_exo_mtf": "max", "BEN_AMA_COD": "max",
                 "dep_lien_ald": "sum", "mnt_rem_lien_ald": "sum", "aro_rem_lien_ald": "sum"}
agg_dict_lpp = {"PRS_PAI_MNT": "sum", "TOT_REM_BSE": "sum", "TOT_REM_MNT": "sum", "ARO_REM_MNT": "sum",
                "sum(TIP_ACT_QSN)": "sum", "top_cmu": "max", "EXO_MTF": "first", "top_acs": "max",
                "top_exo_mtf": "max", "BEN_AMA_COD": "max",
                "dep_lien_ald": "sum", "mnt_rem_lien_ald": "sum", "aro_rem_lien_ald": "sum"}

spark = utils.start_context(cores=30)
dcirs = sdv.load_preprocess_ns_prs(path2flat, path2nomenclature, actes_cli_tech, year, spark)
rac_nonlppdental_ag = sdv.agg_ns_prs_non_lpp_dental(dcirs, path2ref, spark, prs_lpp, spe_act_det_dent,
                                                    actes_cli_tech,
                                                    agg_dict)
rac_dental_ag, rac_dental_nocode_ag = sdv.agg_ns_prs_dental(dcirs, path2ref, path_ns_cam,
                                                            path2nomenclature, spark,
                                                            spe_act_det_dent, actes_cli_tech,
                                                            agg_dict, agg_dict_dent)
rac_lpp_ag, rac_lpp_nocode_ag = sdv.agg_ns_prs_lpp(dcirs, path2nomenclature, path_ns_tip, path2ref,
                                                   spark, prs_lpp, agg_dict_lpp)
rac_nonlppdental_ag, rac_dental_ag, rac_dental_nocode_ag, rac_lpp_ag, rac_lpp_nocode_ag = \
    sdv.clean_output(rac_nonlppdental_ag, rac_dental_ag, rac_dental_nocode_ag, rac_lpp_ag, rac_lpp_nocode_ag)

t1 = time()
sdv.save_parquet(rac_nonlppdental_ag, rac_dental_ag, rac_dental_nocode_ag,
                 rac_lpp_ag, rac_lpp_nocode_ag, path2output)
t2 = time()
print("Write time : {}".format((t2 - t1) / 60)),

audio = ["audio_appareil", "audio_entretien_et_réparations", "audio_autre",
         "audio_implants_cochleaires"]
optical = ["optique_complexe", "lentilles", "montures", "optique_simple", "optique_tres_complexe",
           "optique_autre"]
agg_dict = {"PRS_PAI_MNT": "sum", "TOT_REM_BSE": "sum", "TOT_REM_MNT": "sum", "ARO_REM_MNT": "sum",
            "acte_quantite": "sum", "top_cmu": "max", "EXO_MTF": "first", "top_acs": "max",
            "poste_ag": "first", "top_exo_mtf": "max", "BEN_AMA_COD": "max",
            "dep_lien_ald": "sum", "mnt_rem_lien_ald": "sum", "aro_rem_lien_ald": "sum"}
private_institutions = ["ssr_etab_priv_non_lucratif", "ssr_etab_priv_lucratif", "ssr_autre_etab_priv",
                        "pharma_liste_sus", "psy_etab_priv_non_lucratif",
                        "psy_etab_priv_lucratif", "mco_etab_priv_non_lucratif", "mco_etab_priv_lucratif",
                        "mco_autre_etab_priv", "autre_etab_priv_non_lucratif",
                        "autre_etab_priv_lucratif", "autre_autre_etab_priv", "had_etab_priv_lucratif",
                        "had_etab_priv_non_lucratif",
                        "LPP_autre_priv", "Labo_priv"]
path2data_sdv = "/home/commun/echange/raphaele_adjerad/results/"
spark = utils.start_context(cores=28, partitions=500, mem='100g', name_app="base_rac")
rac_sdv = agg_sdv.union_sdv(path2output, agg_dict, audio, optical, spark, verbose=False)
rac_sdv = agg_sdv.add_person_info(rac_sdv, year, path2flat, path2ref, spark)
rac_sdv = agg_sdv.correct_amounts(rac_sdv)
rac_sdv = agg_sdv.correct_categ(rac_sdv, private_institutions)
rac_sdv = agg_sdv.define_age(rac_sdv, year, spark)

t1 = time()
rac_sdv.write.parquet(path2data_sdv + "rac_final_soindeville_" + str(year))
t2 = time()
print("Write time : {}".format((t2 - t1) / 60))  # 18 min

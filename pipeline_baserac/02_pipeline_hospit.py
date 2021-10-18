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
import baserac.hospit as hospit
from baserac.utils import start_context
from baserac import mco, had, ssr, rip

# Parameters
year = 2017
path2flat = "/home/commun/echange/flattening_dcirs" + str(year)[-2:] + "/"
path2csv = "/home/data2/SNDS/DCIRS/DCIRS_" + str(year) + "/PMSI_" + str(year)
path2ref = "/home/commun/référentiels/SNIIRAM/"
if year == 2017:
    path2csv_mco = path2csv + "/T_MCO" + str(year)[-2:]
    path2csv_had = path2csv + "/T_HAD" + str(year)[-2:]
    path2csv_ssr = path2csv + "/T_SSR" + str(year)[-2:]
    path2csv_rip = path2csv + "/T_RIP" + str(year)[-2:]
elif year == 2016:
    path2csv_mco = path2csv + "/t_mco" + str(year)[-2:]
    path2csv_had = path2csv + "/t_had" + str(year)[-2:]
    path2csv_ssr = path2csv + "/t_ssr" + str(year)[-2:]
    path2csv_rip = path2csv + "/t_rip" + str(year)[-2:]
else:
    raise ValueError("Erreur année non prise en charge")

path2_ir_orc = "/home/data2/SNDS/DCIRS/DCIRS_"
path2_ucd = "/home/commun/echange/noemie_courtejoie/histo_SUS_" + str(year) + ".csv"
path2nomenclature = "/home/commun/echange/raphaele_adjerad/tables_correspondance/20191211_pse_act_spe_x_spe_act_det.csv"
path2output = "/home/commun/echange/raphaele_adjerad/results/rac_hospit/base_RAC" + str(year)
path2aberr = "data/raw/mnt_aberr.txt" # only for rip
list_output_var = ["NUM_ENQ", "poste_ag", "poste", "nb_sejour", "NBSEJ_CPLT_LONG", "NBSEJ_CPLT_COURT",
                   "NBSEJ_PARTL", "NBJ_CPLT", "NBJ_PARTL", "dep_tot", "remb_am", "rac", "rac_amo_sup", "MNT_TM",
                   "MNT_PF", "MNT_FJ", "dep_lien_ald", "rac_lien_ald"]
list_output_var_had = ["NUM_ENQ", "poste_ag", "poste", "nb_sejour", "NBSEJ_PARTL_COURT", "NBSEJ_PARTL_LONG",
                       "NBJ_PARTL", "dep_tot", "remb_am", "rac", "rac_amo_sup", 
                       "MNT_TM", "MNT_PF", "dep_lien_ald", "rac_lien_ald"]
list_output_var_ssr = ["NUM_ENQ", "poste_ag", "poste", "nb_sejour", "NBSEJ_CPLT_COURT", "NBSEJ_CPLT_LONG",
                       "NBSEJ_PARTL_COURT", "NBSEJ_PARTL_LONG", "NBJ_CPLT", "NBJ_PARTL", "dep_tot", "remb_am",
                       "rac",  "rac_amo_sup", "MNT_TM", "MNT_PF", "MNT_FJ", "dep_lien_ald", "rac_lien_ald"]
list_output_var_rip = ["NUM_ENQ", "poste_ag", "poste", "nb_sejour", "NBSEJ_CPLT_COURT", "NBSEJ_CPLT_LONG",
                       "NBSEJ_PARTL_COURT", "NBSEJ_PARTL_LONG", "NBJ_CPLT", "NBJ_PARTL", "dep_tot", "remb_am",
                       "rac", "rac_amo_sup", "MNT_TM", "MNT_PF", "MNT_FJ", "dep_lien_ald", "rac_lien_ald"]

filter_etab = ['600100093', '600100101', '620100016', '640790150', '640797098', '750100018', '750806226',
               '750100356', '750802845', '750801524', '750100067', '750100075', '750100042', '750805228',
               '750018939', '750018988', '750100091', '750100083', '750100109', '750833345', '750019069',
               '750803306', '750019028', '750100125', '750801441', '750019119', '750100166', '750100141',
               '750100182', '750100315', '750019648', '750830945', '750008344', '750803199', '750803447',
               '750100216', '750100208', '750833337', '750000358', '750019168', '750809576', '750100299',
               '750041543', '750100232', '750802258', '750803058', '750803454', '750100273', '750801797',
               '750803371', '830100012', '830009809', '910100015', '910100031', '910100023', '910005529',
               '920100013', '920008059', '920100021', '920008109', '920100039', '920100047', '920812930',
               '920008158', '920100054', '920008208', '920100062', '920712551', '920000122', '930100052',
               '930100037', '930018684', '930812334', '930811294', '930100045', '930011408', '930811237',
               '930100011', '940018021', '940100027', '940100019', '940170087', '940005739', '940100076',
               '940100035', '940802291', '940100043', '940019144', '940005788', '940100050', '940802317',
               '940100068', '940005838', '950100024', '950100016', '130808231', '130809775', '130782931',
               '130806003', '130783293', '130804305', '130790330', '130804297', '130783236', '130796873',
               '130808520', '130799695', '130802085', '130808256', '130806052', '130808538', '130802101',
               '130796550', '130014558', '130784234', '130035884', '130784259', '130796279', '130792856',
               '130017239', '130792534', '130793698', '130792898', '130808546', '130789175', '130780521',
               '130033996', '130018229', '90787460', '690007422', '690007539', '690784186', '690787429',
               '690783063', '690007364', '690787452', '690007406', '690787486', '690784210', '690799416',
               '690784137', '690007281', '690799366', '690784202', '690023072', '690787577', '690784194',
               '690007380', '690784129', '690029194', '690806054', '690029210', '690787767', '690784178',
               '690783154', '690799358', '690787817', '690787742', '690784152', '690784145', '690783121',
               '690787478', '690007455', '690787494', '830100558', '830213484']

spark = start_context(30, 10, '50g', name_app="base_rac_hospit")
rac_mco = mco.agg_mco(path2flat, path2csv_mco, path2_ir_orc, year, list_output_var, spark, filter_etab)


spark = start_context(20, 1, '50g', name_app="base_rac_hospit") 
mco_public_sus = mco.agg_med(path2flat, path2csv_mco, path2_ucd, rac_mco, year, list_output_var, spark)
rac_ace = mco.agg_ace(path2flat, path2csv_mco, path2nomenclature,  path2_ir_orc, filter_etab, 
                      year, spark, list_output_var)
ace_sus = mco.agg_med_ace(path2flat, path2_ucd, rac_ace, path2csv_mco, year, list_output_var, spark)
hospit.union_hospit(path2output + "_MCO", year, 3, rac_mco, mco_public_sus, rac_ace, ace_sus)

spark = start_context(20, 5, '10g', name_app="base_rac_hospit")
rac_had = had.agg_had(path2flat, path2csv_had, path2_ir_orc, filter_etab, year, list_output_var_had, spark)
had_public_sus = had.agg_med(path2flat, path2csv_had, path2_ucd, rac_had, year, list_output_var_had, spark)
hospit.union_hospit(path2output + "_HAD", year, 3, rac_had, had_public_sus)

spark = start_context(20, 5, '10g', name_app="base_rac_hospit")
rac_ssr = ssr.agg_ssr(path2flat, path2csv_ssr, path2_ir_orc, year, filter_etab, list_output_var_ssr, spark)
ssr_public_sus = ssr.agg_med(path2flat, path2csv_ssr, rac_ssr, year, list_output_var_ssr, spark)
rac_ace = ssr.agg_ace(path2flat, path2csv_ssr, path2nomenclature, path2_ir_orc, filter_etab, year, 
                      spark, list_output_var_ssr)
hospit.union_hospit(path2output + "_SSR", year, 3, rac_ssr, ssr_public_sus, rac_ace)

spark = start_context(20, 5, '10g', name_app="base_rac_hospit")
rac_rip = rip.agg_rip(path2flat, path2csv_rip, path2_ir_orc, path2ref, year, spark, filter_etab, 
                      list_output_var_rip, path2aberr)
hospit.union_hospit(path2output + "_RIP", year, 3, rac_rip)

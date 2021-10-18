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
from baserac import chaining
from baserac import utils
from time import time

# Parameters
year = 2017
path2output = "/home/commun/echange/raphaele_adjerad/results/"
path2data_sdv = "/home/commun/echange/raphaele_adjerad/results/rac_final_soindeville_" + str(year) + "/"
path2data_hospit = "/home/commun/echange/raphaele_adjerad/results/rac_hospit/base_RAC" + str(year)
path2flat = "/home/commun/echange/flattening_dcirs" + str(year)[-2:] + "/"
path2_ir_orc = "/home/data2/SNDS/DCIRS/DCIRS_"
path2ref = "/home/commun/référentiels/SNIIRAM/"
path2health_account_categ = \
    '/home/commun/echange/raphaele_adjerad/tables_correspondance/poste_base_rac_x_poste_cns.csv'
list_output_var = ['NUM_ENQ',
                   'BEN_RES_DPT',
                   'BEN_RES_COM',
                   'BEN_NAI_ANN',
                   'BEN_SEX_COD',
                   'poste',
                   'poste_ag',
                   'dep_tot',
                   'remb_am',
                   "tot_rem_bse",
                   "acte_quantite",
                   "rac_opposable",
                   'rac',
                   'depass_c',
                   'age',
                   'classe_age',
                   'top_acs',
                   'top_cmu',
                   'top_ald',
                   'top_ald_exo_mtf',
                   'rac_amo_sup',
                   'MNT_TM',
                   'MNT_PF',
                   'MNT_FJ',
                   'nb_sejour',
                   'NBSEJ_CPLT_COURT',
                   'NBSEJ_CPLT_LONG',
                   'NBSEJ_PARTL_COURT',
                   'NBSEJ_PARTL_LONG',
                   'NBJ_CPLT',
                   'NBJ_PARTL',
                   'NUM_ALD',
                   'nb_ald',
                   "dep_lien_ald",
                   "rac_lien_ald"]
path2result = path2data_sdv + "nomenclatures/"
path2csv = '/home/data2/SNDS/DCIRS/DCIRS_' + str(year) + '/REF/'
path2cor_com = "/home/commun/echange/noemie_courtejoie/correction_codes_communes/CORRECTIONS_COM2012_NEW.csv"
path2cor_postx = "/home/commun/echange/noemie_courtejoie/correction_codes_communes/T_FIN_GEO_LOC_FRANCE.csv"


spark = utils.start_context(30, 200, '50g', name_app="chaining")
data_rac = chaining.import_sdv_hospit(path2flat, path2data_sdv, path2data_hospit, path2_ir_orc, path2ref,
                                      year, list_output_var, spark)
data_rac = chaining.define_unique_ald(path2flat, path2ref, data_rac, year, spark)
data_rac = chaining.redefine_categ_health_accounts(data_rac, path2health_account_categ, path2result, spark)
data_rac = chaining.correct_indiv_charac(path2flat, path2csv, path2cor_com, path2cor_postx,
                                         data_rac, year, spark)

t1 = time()
data_rac.write.parquet(path2output + "rac_chainee_" + str(year))
t2 = time()
print("Write time : {}".format((t2 - t1) / 60))

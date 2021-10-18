
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

import os
import pytest
import pyspark.sql.functions as psf
from baserac import utils
from baserac import rip
os.getcwd()

spark = utils.start_context(cores=1, partitions=2, mem='1g', name_app="base_rac", 
                            master="local[5]")

def test_rip():
    path2flat = "data/raw/flattening_dcirs17/"
    path2csv_rip = "data/raw/RIP_"
    path2_ir_orc = "data/raw/DCIRS_"
    path2ref = "data/nomenclatures/"
    year = 2017
    filter_etab = ['600100093', '600100101']
    list_output_var_rip = ["NUM_ENQ", "poste_ag", "poste", "nb_sejour", "NBSEJ_CPLT_COURT", "NBSEJ_CPLT_LONG",
                       "NBSEJ_PARTL_COURT", "NBSEJ_PARTL_LONG", "NBJ_CPLT", "NBJ_PARTL", "dep_tot", "remb_am",
                       "rac", "rac_amo_sup", "MNT_TM", "MNT_PF", "MNT_FJ", "dep_lien_ald", "rac_lien_ald"]
    path2aberr = "data/raw/mnt_aberr.txt"
    rac_rip = rip.agg_rip(path2flat, path2csv_rip, path2_ir_orc, path2ref, year, spark, filter_etab, 
                      list_output_var_rip, path2aberr)

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
import pyspark.sql.functions as psf
import pytest
from baserac import utils
from baserac import hospit
os.getcwd()

spark = utils.start_context(cores=1, partitions=2, mem='1g', name_app="base_rac", 
                            master="local[5]")


def test_ir_orc_r():
    path2_ir_orc = "data/raw/DCIRS_"
    year = 2017
    ir_orc = hospit._load_ir_orc(path2_ir_orc, year, spark)
    assert ir_orc.count() != 0
    assert ir_orc.filter(psf.col("BEN_CTA_TYP") != "89").count() == 0
    assert ir_orc.filter(psf.col("top_cmu") == 1).count() == ir_orc.count() 
    

def test_load_ir_imb_unity():
    path2flat = "data/raw/flattening_dcirs17/"
    path2ref = "data/nomenclatures/"
    year = 2017
    ir_imb_unity = hospit._load_ir_imb_unity(path2flat, path2ref, year, spark)
    assert ir_imb_unity.count() != 0
    assert ir_imb_unity.columns == ['NUM_ENQ', 'NUM_ALD', 'nb_ald', 'top_ald']
    assert ir_imb_unity.filter(psf.col("top_ald").isin([0, 1, None])).count() == ir_imb_unity.count()
    
def test_load_ucd():
    path2_ucd = "data/raw/UCD.csv"
    ucd = hospit._load_ucd(path2_ucd, spark)
    
def test_add_month_stay():
    path2flat = "data/raw/flattening_dcirs17/"
    path2csv_ssr = "data/raw/SSR_"
    year = 2017
    dict_res = hospit.add_month_stay(path2flat, path2csv_ssr, year, spark)
    year = 2015
    with pytest.raises(ValueError):
        dict_res = hospit.add_month_stay(path2flat, path2csv_ssr, year, spark)
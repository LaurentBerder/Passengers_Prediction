 #-*- coding: utf-8 -*-

# -------------------------------------------------------------------------------
# Name:        Optimode / treat_sources_scope
# Purpose:     Look at all the external sources' websites and check for which year_month their latest available data is.
#
# Author:      berder
#
# Created:     28/07/2017
# Copyright:   (c) Arsynet 2015
# Licence:     Tous droits réservés
# -------------------------------------------------------------------------------

import os
import argparse
import time
import logging
import logging.handlers
import urllib
import locale
from datetime import datetime
import re
import sys
sys.path.append('../')
from optidb.model import *
from utils.logging_utils import BackupFileHandler
from utils import YearMonth

__version__ = 'V1.0.1'


class External_Segment_Tmp(Model):
    __collection__ = 'external_segment_laurent_tests'


class Provider(Model):
    __collection__ = 'provider'


def latest_available(providers):


    # USA:
    page = urllib.urlopen("http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=293").read()
    string = 'Latest Available Data: '
    position = page.find(string)
    date_text = page[position + len(string): position + len(string) + 20].split('<')[0]
    # Make sure the months are expressed in English
    locale.setlocale(locale.LC_TIME, "en_US.utf8"):
    date = datetime.strptime(date_text, '%B %Y')
    year_month = str(date.year) + '-' + format(date.month, '02d')

    # Colombia:
    spanish_months = {'Enero': '01', 'Febrero': '02', 'Marzo': '03', 'Abril': '04', 'Mayo': '05', 'Junio': '06'
                      'Julio': '07'}
    page = urllib.urlopen("http://www.aerocivil.gov.co/atencion/estadisticas-de-las-actividades-aeronauticas/Paginas/bases-de-datos.aspx").read()
    string = 'xlsx'
    position = page.find(string)
    date_text = page[position - 20: position].split('- ')[1]


  -
 marzo - March
 abril - April
 mayo - May
 junio - June
 julio - July
 agosto - August
 septiembre - September
 octubre - October
 noviembre - November
 diciembre - December




 if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Updating "latest year_month" for each external data provider')
    parser.add_argument('year_months', type=str, nargs='+', help='Year_month(s) to download ([YYYY-MM, YYYY-MM...]')
    parser.add_argument('to_process', type=bool, default=False,
                        help='Restrict to the list of providers that are processed')

    p = parser.parse_args()

    logging_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=logging_format)
    handler = BackupFileHandler(filename='latest_available_ym_per_source.log', mode='w', backupCount=20)
    formatter = logging.Formatter(logging_format)
    handler.setFormatter(formatter)
    main_log = logging.getLogger()  # le root handler
    main_log.addHandler(handler)
    log = logging.getLogger('latest_available_ym_per_source')
    log.info('Updating "latest year_month" for each external data provider, version %s - %r', __version__, p)

    Model.init_db(def_w=True)
    if p.to_process:
        providers = [prov.provider for prov in Provider.find({'import_process': True})]
    else:
        providers = [prov.provider for prov in Provider.find({})]

    latest_integrated = External_Segment_Tmp.aggregate([{'$unwind': "$year_month"},
                                                        {'$group': {'_id': "$provider",
                                                                    'latest': {'$max': "$year_month"}}}
                                                        ])

    latest_available(providers)

 #-*- coding: utf-8 -*-

# -------------------------------------------------------------------------------
# Name:        Optimode / Load_files_from_all_sources
# Purpose:     For a given year_month, launch all the programs to download external sources' files
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
import sys
sys.path.append('../')
from optidb.model import *
from utils.logging_utils import BackupFileHandler
from utils import YearMonth

__version__ = 'V1.0.1'


class Provider(Model):
    __collection__ = 'provider'


def launch_import(year_months, selected_providers):
    providers = {
        'USA': 'load_files_from_USA.py',
        'Brazil': 'load_files_from_Brazil.py',
        'Ireland': 'load_files_from_Ireland.py',
        'Colombia': 'load_files_from_Colombia.py',
        'Mexico': 'load_files_from_Mexico.py'
    }
    for prov in providers:
        if prov in selected_providers:
            os.system('python %s %s' % providers.get(prov), YearMonth(year_months))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Launching "load_files_from" programs for year_month')
    parser.add_argument('year_months', type=str, nargs='+', help='Year_month(s) to download ([YYYY-MM, YYYY-MM...]')
    parser.add_argument('to_process', type=bool, default=False, help='Restrict to the list of providers that are processed')

    p = parser.parse_args()

    logging_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=logging_format)
    handler = BackupFileHandler(filename='load_files_from_all_sources.log', mode='w', backupCount=20)
    formatter = logging.Formatter(logging_format)
    handler.setFormatter(formatter)
    main_log = logging.getLogger()  # le root handler
    main_log.addHandler(handler)
    log = logging.getLogger('Load_files_from_all_sources')
    log.info('Launching "load_files_from" programs for year_month, version %s - %r',  __version__, p)

    year_months = p.year_months
    if p.to_process:
        selected_providers = Provider.find({'import_process': True})
    else:
        selected_providers = Provider.find({})

    start_time = time.time()
    Model.init_db(def_w=True)

    launch_import(year_months, selected_providers)
    log.info("\n\n--- %s seconds to load files from %d sources ---", time.time() - start_time, len(selected_providers))
    log.info('End')




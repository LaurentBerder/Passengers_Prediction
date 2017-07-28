 #-*- coding: utf-8 -*-

# -------------------------------------------------------------------------------
# Name:        Optimode / Update Provider Table
# Purpose:     Change the confidence index to providers
#
# Author:      berder
#
# Created:     13/07/2017
# Copyright:   (c) Arsynet 2015
# Licence:     Tous droits réservés
# -------------------------------------------------------------------------------


from __future__ import print_function
import sys
sys.path.append('../')
import logging
import logging.handlers
from optidb.model import *
from utils import utcnow
from utils.logging_utils import BackupFileHandler

__version__ = 'V1.0.1'

logging_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=logging_format)
handler = BackupFileHandler(filename='update_provider_table.log', mode='w', backupCount=5)
formatter = logging.Formatter(logging_format)
handler.setFormatter(formatter)
main_log = logging.getLogger()  # le root handler
main_log.addHandler(handler)
log = logging.getLogger('Updating the provider table, version %s' % __version__)


def open_db():
    Model.init_db(def_w=True)


class Provider(Model):
    __collection__ = 'provider'


def update(new_data):

    def log_bulk(self):
        log.info('store provider: %r', self.nresult)

    with Provider.unordered_bulk(2, execute_callback=log_bulk) as bulk:
        for prov in new_data:
            query = {'provider': prov['provider']}
            bulk.find(query).upsert().update_one({'$set': prov})


def main():
    open_db()
    new_data = [{'provider': 'Mexico', 'index': {'ym_start': '2002-01', 'confidence': 20}},
                {'provider': 'USA', 'index': {'ym_start': '2002-01', 'confidence': 38}},
                {'provider': 'Brazil', 'index': {'ym_start': '2002-01', 'confidence': 43}},
                {'provider': 'Colombia', 'index': {'ym_start': '2002-01', 'confidence': 49}},
                {'provider': 'India', 'index': {'ym_start': '2015-04', 'confidence': 15}}]
    update(new_data)


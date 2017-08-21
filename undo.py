# -*- coding: utf-8 -*-

u"""
undo

Purpose: 

Project:         
Author: Berder
Created: 21/8/17
Copyright: (c) Milanamos 2016
Licence: Tous droits réservés
"""

from __future__ import print_function, division

import argparse
from datetime import datetime, timedelta
import sys
sys.path.append('../')

from optidb.model import *
from utils import YearMonth
from utils.logging_utils import BackupFileHandler
import logging
from utils.threads import ThreadPool


def undo(year_month, date):
    log.info('undo %s', year_month)
    with NewSegmentInitialData.ordered_bulk(1000) as bulk:
        query_update = {'updated.on': {'$gte': date, '$lt': date + timedelta(days=1)},
                        'year_month': year_month}
        cursor = NewSegmentInitialData.find(query_update)
        nb_recs = cursor.count() / 100

        for i, seg in enumerate(cursor, 1):
            if i % 1000 == 0:
                log.info('undo updates: processed %dk records (%.1f%%)', i // 1000, i / nb_recs)
            while seg.updated:
                last_update = seg.updated.pop()
                if last_update.get('data_type') != 'updated_by_external_source':
                    break
                bulk.find(seg.__id_dict__).update_one({'$set': last_update['initial_record'],
                                                       '$pop': dict(updated=1)})
    log.info('undo - update: %r', bulk.nresult)

    log.info('Removing created records')
    query_remove = dict(record_ok=True,
                        source='external_source',
                        year_month=year_month,
                        loaded_from_date={'$gte': date, '$lt': date + timedelta(days=1)},
                        updated={'$in': [None, []]})

    result = NewSegmentInitialData.remove(query_remove)

    log.info('undo - remove: %r', result)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Undo data import updates (data created and/or updated on a specific day for a specific year_month)')
    parser.add_argument('ym', type=YearMonth, help='YYYY-MM, the year_month affected by the updates')
    parser.add_argument('--update_date', type=str, help='YYYY/MM/DD, the date at which the updates were made')

    p = parser.parse_args()

    year_month = str(p.ym)
    update_date = datetime.strptime(p.update_date, '%Y/%m/%d')

    logging_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=logging_format)
    handler = BackupFileHandler(filename='undo_%s.log' % (year_month),
                                mode='w', backupCount=5)
    formatter = logging.Formatter(logging_format)
    handler.setFormatter(formatter)

    main_log = logging.getLogger()  # le root handler
    main_log.addHandler(handler)

    log = logging.getLogger('undo')
    log.setLevel(logging.DEBUG)

    Model.init_db(def_w=True)

    with ThreadPool(5) as pool:
        pool.add_task(undo, year_month, update_date)

    log.info('The end...')
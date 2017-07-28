 #-*- coding: utf-8 -*-

# -------------------------------------------------------------------------------
# Name:        Optimode / load_files_from_CRK
# Purpose:     Load data from file with and passengers details on origin/destination/airline/year_month, both_ways
#
# Author:      berder
#
# Created:     27/07/2017
# Copyright:   (c) Arsynet 2015
# Licence:     Tous droits réservés
# -------------------------------------------------------------------------------


from __future__ import print_function, division
import pandas as pd
import sys
sys.path.append('../')
import logging
from optidb.model import *
from utils import utcnow, YearMonth
from utils.logging_utils import BackupFileHandler
import determine_airline_ref_code as ref_code


logging_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=logging_format)

handler = BackupFileHandler(filename='load_CRK.log', mode='w', backupCount=5)
formatter = logging.Formatter(logging_format)
handler.setFormatter(formatter)

main_log = logging.getLogger()  # le root handler
main_log.addHandler(handler)

log = logging.getLogger('load_CRK')
log.info('Starting to get data from CRK')

unknown_airports = pd.DataFrame(columns=['code', 'passengers'])
unknown_airlines = pd.DataFrame(columns=['code', 'passengers'])


class External_Segment_Tmp(Model):
    __collection__ = 'external_segment_laurent_tests'


def get_airports_codes():
    """
    Get a dictionary of all airport codes for reference throughout import algorithm
    :return:
    """
    airports_codes = Airport.find({'code': {"$ne": None}},
                                  {'code': 1, 'iata_code': 1, 'name': 1, '_id': 0})
    return dict((i.code, i) for i in airports_codes if i.code)


def get_airlines_codes():
    """
    Get a dictionary of all airline codes for reference throughout import algorithm
    :return:
    """
    airlines_codes = Company.find({'code': {"$ne": None}},
                                  {'code': 1, 'iata_code': 1, 'name': 1, '_id': 0})
    return dict((i.code, i) for i in airlines_codes if i.code)


def check_airport(airport, pax, airports_codes):
    global unknown_airports
    # Check the airport code exists in Mongo. If not, skip line
    if airport not in airports_codes:
        if airport in unknown_airports['code'].values:
            unknown_airports.loc[unknown_airports['code'] == airport, 'pax'] += pax
        else:
            info = pd.Series({'code': airport, 'pax': pax})
            unknown_airports = unknown_airports.append(info, ignore_index=True)
        return False
    else:
        return True


def check_airline(airline, pax, airlines_codes):
    global unknown_airlines
    # Check the airport code exists in Mongo. If not, skip line
    if airline not in airlines_codes:
        if airline in unknown_airlines['code'].values:
            unknown_airlines.loc[unknown_airlines['code'] == airline, 'pax'] += pax
        else:
            info = pd.Series({'code': airline, 'pax': pax})
            unknown_airlines = unknown_airlines.append(info, ignore_index=True)
        return False
    else:
        return True


def get_data():
    """
    Populate the database with data extract in xlsx files. One line per year_month.
    Back/Forth routes in rows, one column per way.
    :param xlsx_files: dict of file names
    :return:
    """
    def log_bulk(self):
        log.info('  store external_segment: %r', self.nresult)

    provider = "CRK"
    tmp_dir = "/home/laurent"
    xlsx_f = "CRK.xlsx"
    ref_code.init_cache()

    airports_codes = get_airports_codes()
    airlines_codes = get_airlines_codes()

    log.info('******************** processing Excel file: %s', xlsx_f)
    xl = pd.ExcelFile(tmp_dir + "/" + xlsx_f)
    for tab in xl.sheet_names:
        log.info('************ processing tab %s', tab)
        xls = xl.parse(sheetname=tab, header=1)
        all_rows = len(xls.index)
        row_nb = 0

        with External_Segment_Tmp.unordered_bulk(1000, execute_callback=log_bulk) as bulk:
            for row_index, full_row in xls.iterrows():  # loop through each row (origin, destination, airline, ym) in file
                row_nb += 1
                # Skip empty rows (no text in Origin column, or year Total = 0)
                if pd.isnull(full_row['ORIGIN']):
                    continue
                else:
                    total_pax = int(full_row['TRAFFIC'])
                year = int(full_row['YEAR'])
                month = int(full_row['MONTH'])
                year_month = str(year) + '-' + format(month, '02d')
                airline = full_row['AIRLINE']
                origin = full_row['ORIGIN']
                destination = full_row['DESTINATION']

                if not check_airport(origin, total_pax, airports_codes):
                    continue
                if not check_airport(destination, total_pax, airports_codes):
                    continue
                if not check_airline(airline, total_pax, airlines_codes):
                    continue
                airline_ref_code = ref_code.get_airline_ref_code(airline, origin,
                                                                 destination,
                                                                 YearMonth(year_month))
                dic = dict(provider=provider,
                           data_type='airport',
                           airline=[airline],
                           airline_ref_code=[airline_ref_code],
                           origin=[origin],
                           destination=[destination],
                           year_month=[year_month],
                           total_pax=total_pax,
                           overlap=[],
                           raw_rec=dict(full_row),
                           both_ways=True,
                           from_line=row_nb,
                           from_filename=xlsx_f)
                now = utcnow()
                query = dict((k, dic[k]) for k in ('origin', 'destination', 'year_month', 'provider',
                                                      'data_type', 'airline'))
                bulk.find(query).upsert().update_one({'$set': dic, '$setOnInsert': dict(inserted=now)})

                if row_nb % 100 == 0:
                    print('{0:.3g}'.format(row_nb / all_rows * 100) + '%')
        log.info('stored: %r', bulk.nresult)

if __name__ == '__main__':
    Model.init_db(def_w=True)
    get_data()
    if len(unknown_airports) > 0:
        log.warning('%d unrecognized airports:')
        log.warning(unknown_airports)
    if len(unknown_airlines) > 0:
        log.warning('%d unrecognized airlines:')
        log.warning(unknown_airlines)

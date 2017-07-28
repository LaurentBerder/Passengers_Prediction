 #-*- coding: utf-8 -*-

# -------------------------------------------------------------------------------
# Name:        Optimode / load_files_from_RUN
# Purpose:     Load data from an excel file with Arrivals & Departures passengers on same line
#
# Author:      berder
#
# Created:     24/07/2017
# Copyright:   (c) Arsynet 2015
# Licence:     Tous droits réservés
# -------------------------------------------------------------------------------

from __future__ import print_function, division
import sys
sys.path.append('../')
import determine_airline_ref_code as ref_code
import logging
from optidb.model import *
from utils import utcnow, YearMonth
from utils.logging_utils import BackupFileHandler
import pandas as pd


logging_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=logging_format)

handler = BackupFileHandler(filename='load_Eurostat.log', mode='w', backupCount=5)
formatter = logging.Formatter(logging_format)
handler.setFormatter(formatter)

main_log = logging.getLogger()  # le root handler
main_log.addHandler(handler)

log = logging.getLogger('load_RUN')
log.info('Starting to get data from RUN')

wrong_airports = pd.DataFrame(columns=['code', 'passengers'])


class External_Segment_Tmp(Model):
    __collection__ = 'external_segment_laurent_tests'


def get_airports_codes():
    """
    Get a dictionary of all airport codes for reference throughout import algorithm
    :return:
    """
    airports_codes = Airport.find({'code': {"$ne": None}},
                                  {'code': 1, 'iata_code': 1, 'city': 1, 'name': 1, '_id': 0})
    return dict((i.code, i) for i in airports_codes if i.code)


def check_airport(airport, pax):
    global wrong_airports
    global airports_codes
    # Check the airport code exists in Mongo. If not, skip line
    if airport not in airports_codes:
        if airport in wrong_airports['code'].values:
            wrong_airports.loc[wrong_airports['code'] == airport, 'passengers'] += pax
        else:
            info = pd.Series({'code': airport, 'passengers': pax})
            wrong_airports = wrong_airports.append(info, ignore_index=True)
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

    tmp_dir = "/home/laurent"
    xlsx_f = "RUN.xlsx"
    print('******************** processing Excel file:', xlsx_f)
    xl = pd.ExcelFile(tmp_dir + "/" + xlsx_f)
    xls = xl.parse(header=0)
    xls.columns = ['ORI', 'Airline', 'Arrivals', 'Departures', 'Month', 'Year']

    provider = "RUN"
    all_rows = len(xls.index)
    airport1 = 'RUN'

    with External_Segment_Tmp.unordered_bulk(1000, execute_callback=log_bulk) as bulk:
        for row in range(0, len(xls)):  # loop through each row (origin, destination) in file
            full_row = xls.iloc[row]
            year = full_row['Year']
            month = '%02d' % full_row['Month']
            year_month = YearMonth(str(year) + '-' + str(month))
            # Skip empty rows (no text in Origin column, or year Total = 0)
            if pd.isnull(full_row['Arrivals']) and pd.isnull(full_row['Departures']):
                continue

            airline = full_row['Airline']
            # Some rows (airline 'SE') don't hold specific destination, so replace the value with the existing ones:
            if full_row['ORI'] == "France":
                airport2 = ['MRS', 'LYS']
            else:
                if check_airport(full_row['ORI'], full_row['Arrivals'] + full_row['Departures']):
                    airport2 = [full_row['ORI']]
                else:
                    continue
            airline_ref_code = [ref_code.get_airline_ref_code(airline, airport1, airport2[0], year_month)]


            # First save data from city 1 to city 2
            if not pd.isnull(full_row['Arrivals']):
                dic_to = dict(provider=provider,
                              data_type='airport',
                              airline=[airline],
                              airline_ref_code=airline_ref_code,
                              origin=sorted(airport2),
                              destination=[airport1],
                              year_month=[str(year_month)],
                              total_pax=int(full_row['Arrivals']),
                              overlap=[],
                              raw_rec=dict(full_row),
                              both_ways=False,
                              from_line=row,
                              from_filename=xlsx_f)
                now = utcnow()
                query = dict((k, dic_to[k]) for k in ('origin', 'destination', 'year_month', 'provider',
                                                      'data_type', 'airline'))
                bulk.find(query).upsert().update_one({'$set': dic_to, '$setOnInsert': dict(inserted=now)})

            # Then save data from city 2 to city 1
            if not pd.isnull(full_row['Departures']):
                dic_from = dict(provider=provider,
                                data_type='airport',
                                airline=[airline],
                                airline_ref_code=airline_ref_code,
                                origin=[airport1],
                                destination=sorted(airport2),
                                year_month=[str(year_month)],
                                total_pax=int(full_row['Departures']),
                                overlap=[],
                                raw_rec=dict(full_row),
                                both_ways=False,
                                from_line=row,
                                from_filename=xlsx_f)
                now = utcnow()
                query = dict((k, dic_from[k]) for k in ('origin', 'destination', 'year_month', 'provider',
                                                        'data_type', 'airline'))
                bulk.find(query).upsert().update_one({'$set': dic_from, '$setOnInsert': dict(inserted=now)})
            if row % 100 == 0:
                print('{0:.3g}'.format(row / all_rows * 100) + '%')
    log.info('stored: %r', bulk.nresult)

if __name__ == '__main__':
    Model.init_db(def_w=True)
    airports_codes = get_airports_codes()
    get_data()

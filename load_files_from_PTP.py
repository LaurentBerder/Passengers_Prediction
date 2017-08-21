 #-*- coding: utf-8 -*-

# -------------------------------------------------------------------------------
# Name:        Optimode / load_files_from_PTP
# Purpose:     Load data from file with only year, month and passengers data, no distinction between Arrivals/Departures
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
import logging
from optidb.model import *
from utils import utcnow, YearMonth
from utils.logging_utils import BackupFileHandler
import pandas as pd


logging_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=logging_format)

handler = BackupFileHandler(filename='load_PTP.log', mode='w', backupCount=5)
formatter = logging.Formatter(logging_format)
handler.setFormatter(formatter)

main_log = logging.getLogger()  # le root handler
main_log.addHandler(handler)

log = logging.getLogger('load_PTP')
log.info('Starting to get data from PTP')

wrong_airports = pd.DataFrame(columns=['code', 'passengers'])


class External_Segment_Tmp(Model):
    __collection__ = 'external_segment_laurent_tests'


def get_data():
    """
    Populate the database with data extract in xlsx files. One line per year_month.
    Back/Forth routes in rows, one column per way.
    :param xlsx_files: dict of file names
    :return:
    """
    def log_bulk(self):
        log.info('  store external_segment: %r', self.nresult)

    now = utcnow()
    tmp_dir = "/home/laurent"
    xlsx_f = "PTP.xlsx"
    print('******************** processing Excel file:', xlsx_f)
    xl = pd.ExcelFile(tmp_dir + "/" + xlsx_f)
    xls = xl.parse(header=None)
    # Year_month based on the renamed file. List months of the quarter for the case of international files
    xls.columns = ['Year', 'Month_name', 'Pax']

    provider = "PTP"
    all_rows = len(xls.index)
    airport1 = 'PTP'

    months = {"JAN": '01', "FEB": '02', "MAR": '03', "APR": '04', "MAY": '05', "JUN": '06', "JUL": '07',
              "AUG": '08', "SEP": '09', "OCT": '10', "NOV": '11', "DEC": '12'}

    with External_Segment_Tmp.unordered_bulk(1000, execute_callback=log_bulk) as bulk:
        for row in range(0, len(xls)):  # loop through each row (origin, destination) in file
            full_row = xls.iloc[row]
            year = full_row['Year']
            month = months.get(full_row['Month_name'])
            year_month = str(year) + '-' + month
            # Skip empty rows (no text in Origin column, or year Total = 0)
            if pd.isnull(full_row['Pax']):
                continue
            else:
                total_pax = int(full_row['Pax'])
            dic = dict(provider=provider,
                          data_type='airport',
                          airline=['*'],
                          airline_ref_code=['*'],
                          origin=[airport1],
                          destination=['*'],
                          year_month=[year_month],
                          total_pax=total_pax,
                          overlap=[],
                          raw_rec=dict(full_row),
                          both_ways=True,
                          from_line=row,
                          from_filename=xlsx_f)
            query = dict((k, dic[k]) for k in ('origin', 'destination', 'year_month', 'provider',
                                                  'data_type', 'airline'))
            bulk.find(query).upsert().update_one({'$set': dic, '$setOnInsert': dict(inserted=now)})

            if row % 100 == 0:
                print('{0:.3g}'.format(row / all_rows * 100) + '%')
    log.info('stored: %r', bulk.nresult)

if __name__ == '__main__':
    Model.init_db(def_w=True)
    get_data()

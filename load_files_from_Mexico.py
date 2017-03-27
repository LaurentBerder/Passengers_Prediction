 #-*- coding: utf-8 -*-

# -------------------------------------------------------------------------------
# Name:        Optimode / load_files_from_Mexico
# Purpose:     Load data from files from Mexico Government website
#
# Author:      berder
#
# Created:     22/10/2016
# Copyright:   (c) Arsynet 2015
# Licence:     Tous droits réservés
# -------------------------------------------------------------------------------

from __future__ import print_function
import sys
import time
import logging
import logging.handlers
import os
import urllib2
from bs4 import BeautifulSoup
from urllib import urlopen, urlretrieve, quote
from optidb.model import *
from utils import utcnow
import pandas as pd
import numpy as np
from unidecode import unidecode
import re
sys.path.append('../')

provider = 'Mexico'
provider_tag = 'query_providers.%s' % provider
__version__ = 'V1.0.0'
unknown_airports = set()
tmp_dir = '/tmp/mexico'
base_url = 'http://www.sct.gob.mx/'
end_url = 'transporte-y-medicina-preventiva/aeronautica-civil/5-estadisticas/53-estadistica-operacional-de-aerolineas-air-carrier-operational-statistics/'

logging.basicConfig(level=logging.DEBUG, format=0)
log = logging.getLogger('load_colombia')
log.setLevel(logging.DEBUG)

log.info("Updating db with new file contents from Mexico's government website, version %s...", __version__)


def open_db():
    #config['ming.url'] = 'mongodb://localhost/'     # connect to local database instead of Optimode
    Model.init_db()


class External_Segment(Model):
    __collection__ = 'external_segment'


class External_Segment_Tmp(Model):
    __collection__ = 'external_segment_laurent_tests'


def download_files():
    """
    Get files from the web site
    The files are downloaded to 'tmp_dir' directory
    :return:
    """
    log.info('Getting files on the web')
    u = urlopen(base_url + end_url)
    try:
        html = u.read().decode('utf-8')
    finally:
        u.close()

    soup = BeautifulSoup(html, "html.parser")

    if not os.path.isdir(tmp_dir):
        os.mkdir(tmp_dir)

    # Select all A elements with href attributes containing URLs starting with http://
    for link in soup.select('a[href^="fileadmin"]'):
        href = link.get('href')
        # Make sure it has one of the correct extensions
        filename = tmp_dir + "/" + href.rsplit('/', 1)[-1]
        # Download Origin/Destination files later than 2001 not already downloaded
        if "sase" in filename and not os.path.exists(filename):
            urlretrieve(base_url + href, filename)
            print('File', filename, 'downloaded')

    xlsx_files = os.listdir(tmp_dir)

    return xlsx_files


def find_airport_by_name(airport_name, tab_name):
    """
    This function looks up the name of an airport or city in the Excel file based on the Mexico-specific
    field of "provider_query", or on "query_names".
    Failures of this function are reported at the end of the algorithm to enrich (manually) the "provider_query" with
    the help of submit_query_providers() function.
    :param airport_name: an upper case string
    :param tab_name: name of the excel file's tab (indication of international or mexican-only airports)
    :return: airport code (or None)
    """
    airport_clean = airport_name.lower().replace('.', '').split('-')[0].split('/')[0].split(',')[0].strip()
    if 'NAC' in tab_name:
        airport = Airport.find({provider_tag: airport_name.strip(), 'code_type': 'airport', 'country': 'MX'},
                               {'_id': 0, 'code': 1, 'name': 1, 'city': 1}).first()
        if airport is None:
            airport = Airport.find({'query_names': airport_clean, 'code_type': 'airport', 'country': 'MX'},
                                   {'_id': 0, 'code': 1, 'name': 1, 'segment_origins': 1, 'city': 1}).\
                sort([{'segment_origins', -1}]).first()
    else:
        airport = Airport.find({provider_tag: airport_name.strip(), 'code_type': 'airport'},
                               {'_id': 0, 'code': 1, 'name': 1, 'city': 1}).first()
        if airport is None:
            airport = Airport.find({'query_names': airport_clean, 'code_type': 'airport'},
                                   {'_id': 0, 'code': 1, 'name': 1, 'segment_origins': 1, 'city': 1}).\
                sort([{'segment_origins', -1}]).first()
    return airport.code if airport else None


def format_file(xls):
    """
    Files are not all in the same format depending on the year. We need to remove columns (concerning number of flights
    and cargo weight), and standardize column names.
    :param xls: Excel file's tab
    :return xls: Excel file's tab in standard format, with only passenger data
    """
    columns_to_drop = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40]
    xls.drop(xls.columns[columns_to_drop], axis=1, inplace=True)
    new_columns = xls.columns.values
    new_columns[[i for i, item in enumerate(new_columns) if "origen" in item.lower()]] = "Origin"
    new_columns[[i for i, item in enumerate(new_columns) if "destino" in item.lower()]] = "Destination"
    new_columns[[i for i, item in enumerate(new_columns) if "ene" in item.lower()]] = "January"
    new_columns[[i for i, item in enumerate(new_columns) if "feb" in item.lower()]] = "February"
    new_columns[[i for i, item in enumerate(new_columns) if "mar" in item.lower()]] = "March"
    new_columns[[i for i, item in enumerate(new_columns) if "abr" in item.lower()]] = "April"
    new_columns[[i for i, item in enumerate(new_columns) if "may" in item.lower()]] = "May"
    new_columns[[i for i, item in enumerate(new_columns) if "jun" in item.lower()]] = "June"
    new_columns[[i for i, item in enumerate(new_columns) if "jul" in item.lower()]] = "July"
    new_columns[[i for i, item in enumerate(new_columns) if "ago" in item.lower()]] = "August"
    new_columns[[i for i, item in enumerate(new_columns) if "sep" in item.lower()]] = "September"
    new_columns[[i for i, item in enumerate(new_columns) if "oct" in item.lower()]] = "October"
    new_columns[[i for i, item in enumerate(new_columns) if "nov" in item.lower()]] = "November"
    new_columns[[i for i, item in enumerate(new_columns) if "dic" in item.lower()]] = "December"
    new_columns[[i for i, item in enumerate(new_columns) if "total" in item.lower()]] = "Total"
    xls.columns = new_columns

    return xls


# def submit_query_providers():
#     """
#     Save new query names identified from previous data save failures to improve future imports
#     """
#     print('Saving new airports codes...')
#     airport_replacement = {'SJO': 'SAN JOSE, COSTA RICA', 'VLN': 'VALENCIA, VENEZUELA', 'SJC': 'SAN JOSE, CALIFORNIA',
#                            'LIR': 'LIBERIA', 'YYJ': 'VICTORIA, COLUMBIA',
#                            'DAV': 'PANAMA', 'CDG': 'PARIS, CHARLES DE GAULLE', 'HKG': 'HONG KONG , Chek Lap Kok',
#                            'DWC': 'Jabel Ali, Emirates Arabes Unidos', 'ZAZ': 'ZARAGOZA, ESPANA',
#                            'GOT': 'GOTEMBURGO,SUECIA', 'PCA': 'PORTAGE CREEK', 'SJD': "ST. JHON'S", 'NKG': 'NANJING, JIANGSU'}
#     with Airport.unordered_bulk() as bulk:
#         for airport in airport_replacement:
#             name = airport_replacement.get(airport)
#             log.info('airport: %s', airport)
#             bulk.find(dict(code=airport, code_type='airport')).upsert().update_one(
#                 {'$addToSet': {provider_tag: name}})
#     log.info('load_airports_names: %r', bulk.nresult)


def get_data(xlsx_files):
    """
    Populate the database with data extract in xlsx files. 4 different tabs, for distinction of national/international
    and scheduled/charter flights. Routes in rows, months in columns.
    :param xlsx_files: dict of file names
    :return:
    """
    global provider
    months = {"January": "01", "February": "02", "March": "03", "April": "04", "May": "05", "June": "06",
              "July": "07", "August": "08", "September": "09", "October": "10", "November": "11", "December": "12"}

    def log_bulk(self):
        log.info('  store external_segment: %r', self.nresult)

    for xlsx_f in xlsx_files:  # loop through each file
        print('******************** processing Excel file:', xlsx_f)
        xl = pd.ExcelFile(tmp_dir + "/" + xlsx_f)
        # Create a data frame to save data line after line, so we can check later on and add values to each other
        previous_data = pd.DataFrame(columns=['origin', 'destination', 'year_month', 'passengers'])

        for tab in xl.sheet_names:  # loop in all sheets of the excel file
            xls = xl.parse(tab)
            year = xls.iloc[0, 2].split('/')[1].split(',')[1].strip()    # Look for the year in cell C3
            header = np.where(xls.loc[:, :] == "PAR DE CIUDADES / CITY PAIR")[0] + 3  # Look for line with column names
            xls = xl.parse(tab, header=header)   # Re-load file with headers
            xls = format_file(xls)

            with External_Segment_Tmp.unordered_bulk(1000, execute_callback=log_bulk) as bulk:

                for row in range(0, len(xls)):  # loop through each row (origin, destination) in file
                    full_row = xls.iloc[row]
                    # Skip empty rows (no text in Origin column, or year Total = 0)
                    if isinstance(full_row['Origin'], float) or full_row['Total'] == 0:
                        continue
                    # Stop at the end of the table (indicated by "T O T A L")
                    if "".join(full_row['Origin'].split(" ")).upper() == "TOTAL":
                        break
                    origin = unidecode(full_row['Origin']).upper()
                    destination = unidecode(full_row['Destination']).upper()
                    airport_origin = find_airport_by_name(origin, tab)
                    airport_destination = find_airport_by_name(destination, tab)
                    if airport_origin is None:
                        unknown_airports.add(origin)
                        continue
                    if airport_destination is None:
                        unknown_airports.add(destination)
                        continue

                    for col in range(2, len(xls.columns)-1):   # loop through rows (except for Origin, Dest, and Total)
                        # skip cells with no pax
                        if np.isnan(full_row[col]) or full_row[col] == "" or int(full_row[col]) == 0:
                            continue

                        total_pax = int(full_row[col])

                        year_month = year + "-" + months.get(xls.columns[col])

                        if ((previous_data['origin'] == airport_origin) &
                                (previous_data['destination'] == airport_destination) &
                                (previous_data['year_month'] == year_month)).any():
                            new_row = False
                            # Add to Excel file's total_pax the "passenger" integer you get from filtering previous_data on other columns
                            total_pax += int(previous_data['passengers'][
                                                 (previous_data['origin'] == airport_origin) &
                                                 (previous_data['destination'] == airport_destination) &
                                                 (previous_data['year_month'] == year_month)])
                        else:
                            new_row = True

                        dic = dict(provider=provider,
                                   data_type='airport',
                                   airline=None,
                                   origin=airport_origin,
                                   destination=airport_destination,
                                   year_month=year_month,
                                   total_pax=total_pax,
                                   raw_rec=full_row.to_json(),
                                   both_ways=False,
                                   from_line=row,
                                   from_filename=xlsx_f,
                                   url=base_url+end_url)

                        new_data = pd.Series({'origin': airport_origin, 'destination': airport_destination,
                                              'year_month': year_month, 'passengers': total_pax}).to_frame()
                        if new_row:
                            previous_data = previous_data.append(new_data.T, ignore_index=True)
                        else:
                            # Update the previous_data data frame with the new passengers count
                            previous_data['passengers'][
                                (previous_data['origin'] == airport_origin) &
                                (previous_data['destination'] == airport_destination) &
                                (previous_data['year_month'] == year_month)] = total_pax

                        now = utcnow()
                        query = dict((k, dic[k]) for k in ('origin', 'destination', 'year_month', 'provider',
                                                           'data_type', 'airline'))
                        bulk.find(query).upsert().update_one({'$set': dic, '$setOnInsert': dict(inserted=now)})
            log.info('stored: %r', bulk.nresult)


def main():
    log.info('Starting to get data')
    start_time = time.time()
    open_db()
    # submit_query_providers()   # update "provider_query" tags with previously unidentified airports
    # xlsx_files = download_files()
    xlsx_files = os.listdir(tmp_dir)
    get_data(xlsx_files)
    log.info("\n\n--- %s seconds to populate db with %d files---" % ((time.time() - start_time), len(xlsx_files)))
    if len(unknown_airports) > 0:
        print("\n", len(unknown_airports), "unknown airports (check the reasons why): ", unknown_airports)
    log.info('End')

if __name__ == '__main__':
    main()

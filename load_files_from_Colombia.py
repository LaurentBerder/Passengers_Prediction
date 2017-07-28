 #-*- coding: utf-8 -*-

# -------------------------------------------------------------------------------
# Name:        Optimode / load_files_from_Colombia
# Purpose:     Load data from files from Colombia "Aeronautica Civil"
#
# Author:      berder
#
# Created:     22/10/2016
# Copyright:   (c) Arsynet 2015
# Licence:     Tous droits réservés
# -------------------------------------------------------------------------------

from __future__ import print_function
import argparse
import time
from bs4 import BeautifulSoup
from urllib import urlopen, urlretrieve
import os
import logging
import logging.handlers
import sys
sys.path.append('../')
from optidb.model import *
from utils import utcnow, YearMonth
from utils.logging_utils import BackupFileHandler
import determine_airline_ref_code as ref_code
import pandas as pd
import numpy as np
import re

provider = 'Colombia'
__version__ = 'V1.0.0'
AIRPORTS_CODES = dict()
AIRLINES_BY_ICAO = dict()
unknown_airports = pd.DataFrame(columns=['code', 'city', 'file_country', 'Optimode_country', 'info_type', 'passengers'])
unknown_airlines = pd.DataFrame(columns=['code', 'name', 'passengers'])
tmp_dir = '/tmp/colombia'
full_url = 'http://www.aerocivil.gov.co/atencion/estadisticas-de-las-actividades-aeronauticas/Paginas/bases-de-datos.aspx'


class External_Segment(Model):
    __collection__ = 'external_segment'


class External_Segment_Tmp(Model):
    __collection__ = 'external_segment_laurent_tests'


def get_airports_codes():
    """
    Get a dictionary of all airport codes for reference throughout import algorithm
    :return:
    """
    airports_codes = Airport.find({'iata_code': {"$ne": None}},
                                  {'code': 1, 'iata_code': 1, 'icao_code': 1, 'country': 1, 'city': 1,
                                   'name': 1, '_id': 0})
    return dict((i.iata_code, i) for i in airports_codes if i.iata_code)


def check_airport(airport, city, country, pax):
    global unknown_airports
    global airport_codes

    # Check the airport code exists in Mongo. If not, skip line
    if airport not in airport_codes:
        if airport in unknown_airports['code'].values:
            unknown_airports.loc[unknown_airports['code'] == airport, 'passengers'] += pax
        else:
            info = pd.Series({'code': airport, 'city': city, 'file_country': country, 'Optimode_country':None,
                              'info_type': 'missing', 'passengers': pax})
            unknown_airports = unknown_airports.append(info, ignore_index=True)
        return False

    # Check that airports are in the same country as in the database
    # (would require translating country names from spanish)
    # if not country == airport_codes.get(airport).get('country'):
    #     if airport in unknown_airports['code'].values:
    #         unknown_airports.loc[unknown_airports['code'] == airport, 'passengers'] += pax
    #     else:
    #         info = pd.Series({'code': airport, 'city': city, 'file_country': country,
    #                           'Optimode_country': airport_codes.get(airport).get('country'),
    #                           'info_type': 'country', 'passengers': pax})
    #         unknown_airports = unknown_airports.append(info, ignore_index=True)
    return True


def get_airline_codes():
    """
    Get a dictionary of all airline codes for reference throughout import algorithm
    :return:
    """
    airlines = Company.find({'icao_code': {"$ne": None}},
                            {'iata_code': 1, 'icao_code': 1, 'name': 1, '_id': 0})
    return dict((a.icao_code, a) for a in airlines if a.icao_code)


def get_airline_by_icao(airline_icao, airline_name, pax):
    """
    Look-up an ICAO code to return a IATA code. Here, we specify the airport name for traceability in
    the "unknown_airline_codes" which is printed at the end of the algorithm.
    :param airline_icao: a string for an airline's ICAO
    :param airline_name: a string name
    :return: the same airline's iata_code
    """
    global unknown_airlines
    t = AIRLINES_BY_ICAO.get(airline_icao)

    if t is None:
        if airline_icao in unknown_airlines['code'].values:
            unknown_airlines.loc[unknown_airlines['code'] == airline_icao, 'passengers'] += pax
        else:
            info = pd.Series({'code': airline_icao, 'name': airline_name, 'passengers': pax})
            unknown_airlines = unknown_airlines.append(info, ignore_index=True)
        return None
    return t['iata_code']


def download_files(year):
    log.info('Getting files on the web')
    for y in year:
        download_file(y)
    xlsx_files = os.listdir(tmp_dir)
    return xlsx_files


def download_file(year):
    """
    Get files from the web site
    The files are downloaded to 'tmp_dir' directory
    :return:
    """
    end_name = "Colombia-%s.xlsx" % (year)
    u = urlopen(full_url)
    try:
        html = u.read().decode('utf-8')
    finally:
        u.close()

    soup = BeautifulSoup(html, "html.parser")

    if not os.path.isdir(tmp_dir):
        os.mkdir(tmp_dir)

    # Download the href element linking to an Excel document containing the correct year and the words "Origen - Destino"
    # Stop as soon as one file has been downloaded
    for link in soup.select('a[href^="http://"]'):
        if "Destino" in link.get('href') and int(re.findall('\d+', link.get('href'))[0]) == year:
            href = link.get('href')
            filename = tmp_dir + "/" + href.rsplit('/', 1)[-1]
            urlretrieve(href, tmp_dir + "/" + end_name)
            log.info("%s downloaded", end_name)
            break


def format_columns(xls):
    """
    Files are not all in the same format depending on the year. We need to remove columns standardize column names and
    calculate year_month.
    :param xls: Excel file's tab
    :return xls: Excel file's tab in standard format
    """
    new_columns = xls.columns.values
    new_columns[[i for i, item in enumerate(new_columns) if "pasajero" in item.lower()]] = "Passengers"
    new_columns[[i for i, item in enumerate(new_columns) if u"año" in item.lower()]] = "Year"
    new_columns[[i for i, item in enumerate(new_columns) if u"mes" in item.lower()]] = "Month"
    new_columns[[i for i, item in enumerate(new_columns) if "fecha" in item.lower()]] = "Date"
    new_columns[[i for i, item in enumerate(new_columns) if
                 "apto" in item.lower() and "destino" in item.lower()]] = "Airport_Destination"
    new_columns[[i for i, item in enumerate(new_columns) if
                 "apto" in item.lower() and "origen" in item.lower()]] = "Airport_Origin"
    new_columns[[i for i, item in enumerate(new_columns) if "sigla" in item.lower()]] = "Airline"
    new_columns[[i for i, item in enumerate(new_columns) if "nombre" in item.lower()]] = "Airline_Name"
    xls.columns = new_columns

    #  Calculate year_month
    if 'Month' in xls.columns.values:
        if xls['Month'][0] > 12:  # Some files have inverted Month and Year columns
            xls['Year_Month'] = xls.Month.map(str) + "-" + xls.Year.map("{:02}".format)
        else:
            xls['Year_Month'] = xls.Year.map(str) + "-" + xls.Month.map("{:02}".format)
    else:
        xls['Year_Month'] = [_.to_pydatetime().strftime("%Y-%m") for _ in xls['Date']]
    return xls


def get_data(xlsx_files):
    """
    Populate the database with data extract in xlsx files
    :return:
    """
    global provider
    global airport_codes
    ref_code.init_all()
    airport_codes = get_airports_codes()
    airport_replacement = {"BUE": "EZE", "RIO": "GIG", "LMC": "LMC", "LMA": "MCJ", "VGP": "VGZ", "PTL": "PTX",
                           "MIL": "MXP", "LON": "LHR", "SAO": "CGH", "BSL": "BSL", "TRP": "TCD", "RLB": "LIR",
                           "NYC": "JFK", "GTK": "FRS", "AWH": "USH", "STO": "ARN", "WAS": "IAD", "BHZ": "PLU"}

    def log_bulk(self):
        log.info('  store external_segment: %r', self.nresult)

    for xlsx_f in xlsx_files:  # loop through each file
        print('******************** processing Excel file:', xlsx_f)
        xls = pd.read_excel(tmp_dir + "/" + xlsx_f)
        header = np.where(xls.loc[:, :] == "Pasajeros")[0] + 1  # Look for column names
        xls = pd.read_excel(tmp_dir + "/" + xlsx_f, header=header)  # Re-load file with headers
        xls = format_columns(xls)
        # Create a dataframe to save data line after line, so we can check later on
        previous_data = pd.DataFrame(columns=['origin', 'destination', 'year_month', 'airline', 'airline_ref_code', 'passengers'])

        with External_Segment_Tmp.unordered_bulk(1000, execute_callback=log_bulk) as bulk:
            for row in range(0, len(xls)):    # loop through each row (origin, destination) in file
                full_row = xls.iloc[row]
                # skip rows with no pax
                if np.isnan(full_row['Passengers']) or full_row['Passengers'] == "" or int(full_row['Passengers']) == 0:
                    continue

                year_month = full_row['Year_Month']
                # Stop if year_month not in the requested list
                if year_month not in p.year_months:
                    continue

                total_pax = int(full_row['Passengers'])

                row_airline = get_airline_by_icao(full_row['Airline'], full_row['Airline_Name'], total_pax)
                if row_airline is None:
                    continue

                airport_origin = full_row['Origen']
                airport_destination = full_row['Destino']
                if airport_origin in airport_replacement:  # correct the wrong codes
                    airport_origin = airport_replacement.get(airport_origin)
                if airport_destination in airport_replacement:  # correct the wrong codes
                    airport_destination = airport_replacement.get(airport_destination)

                if not check_airport(airport_origin, full_row['Ciudad Origen'],
                                     full_row['Pais Origen'], total_pax):
                    continue
                if not check_airport(airport_destination, full_row['Ciudad Destino'],
                                     full_row['Pais Destino'], total_pax):
                    continue

                airline_ref_code = ref_code.get_airline_ref_code(row_airline, airport_origin,
                                                                 airport_destination,
                                                                 YearMonth(year_month))

                # if External_Segment_Tmp.find_one({'year_month': year_month, 'provider': provider}):
                #     log.warning("This year_month (%s) already exists for provider %s", year_month, provider)

                if ((previous_data['origin'] == airport_origin) &
                        (previous_data['destination'] == airport_destination)
                        & (previous_data['year_month'] == year_month)
                        & (previous_data['airline'] == row_airline)
                        & (previous_data['airline_ref_code'] == airline_ref_code)).any():
                    new_row = False
                    # Add to Excel file's total_pax the "passenger" integer you get from filtering previous_data on other columns
                    total_pax += int(previous_data['passengers'][
                        (previous_data['origin'] == airport_origin) &
                        (previous_data['destination'] == airport_destination)
                        & (previous_data['year_month'] == year_month)
                        & (previous_data['airline'] == row_airline)
                        & (previous_data['airline_ref_code'] == airline_ref_code)])
                else:
                    new_row = True

                dic = dict(provider=provider,
                           data_type='airport',
                           airline=[row_airline],
                           airline_ref_code=[airline_ref_code],
                           origin=[airport_origin],
                           destination=[airport_destination],
                           year_month=[year_month],
                           total_pax=total_pax,
                           overlap=[],
                           raw_rec=dict(full_row),
                           both_ways=False,
                           from_line=row,
                           from_filename=xlsx_f,
                           url=full_url)

                new_data = pd.Series({'origin': airport_origin, 'destination': airport_destination,
                                      'year_month': year_month, 'airline': row_airline,
                                      'airline_ref_code': airline_ref_code, 'passengers': total_pax}).to_frame()
                if new_row:
                    previous_data = previous_data.append(new_data.T, ignore_index=True)
                else:
                    # Update the previous_data data frame with the new passengers count
                    previous_data['passengers'][
                        (previous_data['origin'] == airport_origin) &
                        (previous_data['destination'] == airport_destination) &
                        (previous_data['airline'] == row_airline) &
                        (previous_data['airline_ref_code'] == airline_ref_code) &
                        (previous_data['year_month'] == year_month)] = total_pax

                now = utcnow()
                query = dict((k, dic[k]) for k in ('origin', 'destination', 'year_month', 'provider',
                                                   'data_type', 'airline'))
                bulk.find(query).upsert().update_one({'$set': dic, '$setOnInsert': dict(inserted=now)})
                if row % 1000 == 0:
                    print('{0:.3g}'.format(float(row) / float(len(xls)) * 100) + '%')
        log.info('stored: %r', bulk.nresult)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Load data from Colombia')
    parser.add_argument('year_months', type=str, nargs='+', help='Year_month(s) to download ([YYYY-MM, YYYY-MM...]')

    p = parser.parse_args()

    logging_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=logging_format)

    handler = BackupFileHandler(filename='load_Colombia.log', mode='w', backupCount=5)
    formatter = logging.Formatter(logging_format)
    handler.setFormatter(formatter)

    main_log = logging.getLogger()  # le root handler
    main_log.addHandler(handler)

    log = logging.getLogger('load_Colombia')

    log.info("Updating db with new file contents from Aeronáutica Civil de Colombia website, version %s - %r",
             __version__, p)

    start_time = time.time()
    Model.init_db(def_w=True)
    year = list(set([ym[0:4] for ym in p.year_months]))
    xlsx_files = download_files(year)
    # xlsx_files = os.listdir(tmp_dir)
    AIRLINES_BY_ICAO.update(get_airline_codes())
    get_data(xlsx_files)
    log.info("\n\n--- %s seconds to populate db from Aeronautica Civil de Colombia---", (time.time() - start_time))
    global unknown_airports
    global unknown_airlines
    if len(unknown_airports.index) > 0:
        unknown_airports = unknown_airports.sort_values('passengers', ascending=False)
        log.warning("%s wrong or unknown airports (check the reasons why): \n%s", len(unknown_airports.index),
                    unknown_airports)
    if len(unknown_airlines.index) > 0:
        unknown_airlines = unknown_airlines.sort_values('passengers', ascending=False)
        log.warning("%s wrong or unknown airlines (check the reasons why): \n%s", len(unknown_airlines.index),
                    unknown_airlines)
    log.info("End")
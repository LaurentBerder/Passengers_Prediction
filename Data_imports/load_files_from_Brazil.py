 #-*- coding: utf-8 -*-

# -------------------------------------------------------------------------------
# Name:        Optimode / load_files_from_Brazil
# Purpose:     Load data from files from Brazil "Agência Nacional De Aviação Civil"
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
import urllib2
from bs4 import BeautifulSoup
import logging
import logging.handlers
import os
import sys
sys.path.append('../')
from optidb.model import *
from utils import utcnow, YearMonth
from utils.logging_utils import BackupFileHandler
import csv
import pandas as pd
import determine_airline_ref_code as ref_code

provider = 'Brazil'
__version__ = 'V1.0.0'
tmp_dir = '/tmp/brazil'
if not os.path.isdir(tmp_dir):
     os.mkdir(tmp_dir)
full_url = 'http://www.anac.gov.br/assuntos/dados-e-estatisticas/dados-estatisticos/dados-estatisticos'


class External_Segment(Model):
    __collection__ = 'external_segment'


class External_Segment_Tmp(Model):
    __collection__ = 'external_segment_laurent_tests'


def get_airports_codes():
    """
    Get a dictionary of all airport codes for reference throughout import algorithm
    :return:
    """
    airports_codes = Airport.find({'icao_code': {"$ne": None}},
                                  {'code': 1, 'iata_code': 1, 'icao_code': 1, 'country': 1,
                                   'city': 1, 'name': 1, '_id': 0})
    return dict((i.icao_code, i) for i in airports_codes if i.icao_code)


def get_airport_by_icao(airport_icao_code, airport_name, pax):
    """
    Look-up an ICAO code to return a IATA code. Here, we specify the airport name for traceability in
    the "unknown_icao_codes" which is printed at the end of the algorithm.
    :param airport_icao_code: a string for an airport's ICAO
    :param airport_name: a string name
    :return: the same airport's iata_code
    """
    global unknown_icao_codes
    p = AIRPORTS_CODES.get(airport_icao_code)
    if p is None:
        # If airport was already identified, add the current passengers to the registered ones in the dataframe
        if airport_icao_code in unknown_icao_codes['code'].values:
            unknown_icao_codes.loc[unknown_icao_codes['code'] == airport_icao_code, 'passengers'] += pax
        # If airport is identified for the first time, save it in the dataframe
        else:
            info = pd.Series({'code': airport_icao_code, 'name': airport_name, 'passengers': pax})
            unknown_icao_codes = unknown_icao_codes.append(info, ignore_index=True)
        return None
    return p['iata_code']


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
    global unknown_airline_codes
    t = AIRLINES_BY_ICAO.get(airline_icao)
    if t is None:
        # If airline was already identified, add the current passengers to the registered ones in the dataframe
        if airline_icao in unknown_airline_codes['code'].values:
            unknown_airline_codes.loc[unknown_airline_codes['code'] == airline_icao, 'passengers'] += pax
        # If airport is identified for the first time, save it in the dataframe
        else:
            info = pd.Series({'code': airline_icao, 'name': airline_name, 'passengers': pax})
            unknown_airline_codes = unknown_airline_codes.append(info, ignore_index=True)
        return None
    return t['iata_code']


def download_files(year):
    """
    Get all files from the website
    :param year: an integer, or a list of integers
    :return: list of csv files
    """
    log.info('Getting files on the web')
    xlsx_files = []
    for y in year:
        xlsx_files.append(download_single_file(y))
    return xlsx_files


def download_single_file(year):
    """
    Get a year's file from the web site
    The file is downloaded to 'tmp_dir' directory, then converted to csv format from the existing xlsb
    :param year: an integer
    """
    end_name = 'Brazil_%s.csv' % year
    if end_name not in os.listdir(tmp_dir):
        response = urllib2.urlopen(full_url)
        page = response.read()

        # get all files' download urls
        soup = BeautifulSoup(page, 'html.parser')
        files_urls = [href for href in [a.get('href') for a in soup.find_all('a')] if href and href.find('xlsb') >= 0]

        base, _ = os.path.split(files_urls[0])
        files_urls = [url for url in files_urls if url.find(base) == 0 >= 0]

        if not files_urls:
            log.warning('Cannot find any file')

        # Out of all the files available on the website, identify the correct year
        url = [x for x in files_urls if str(year) in x][0]

        # download all xlsb files
        f_in = urllib2.urlopen('%s' % url)
        _, filename = os.path.split(url)
        with open(os.path.join(tmp_dir, filename), 'wb') as f_out:
            f_out.write(f_in.read())
        os.system("export HOME=/tmp && libreoffice --headless --convert-to csv"
                  " %s/%s --outdir %s --infilter=CSV:44,34,UTF8"
                  % (tmp_dir, filename, tmp_dir))  # convert from xlsb to csv format with UTF-8 encoding
        os.system("rm -f %s/%s" % (tmp_dir, filename))  # delete original xlsb files
        csv_name = max([tmp_dir + "/" + f for f in os.listdir(tmp_dir)], key=os.path.getctime)
        os.rename(os.path.join(tmp_dir, csv_name), os.path.join(tmp_dir, end_name))
        log.info('File %s downloaded and converted', end_name)
    return end_name


def get_data(csv_files, year_months):
    """
    Populate the database with data extract in csv files
    :return:
    """
    now = utcnow()
    ref_code.init_cache()
    airport_replacement = {"SBCD": "SSCC", "SWUY": "SBUY", "SBJI": "SWJI", "RJNN": "RJNA", "SBPM": "SBPJ",
                           "SEQU": "SEQM", "SNQY": "SBJU", "SJDB": "SBDB", "SWJH": "SIZX", "SNNG": "SJNP",
                           "SDFR": "SDDN", "1AON": "SDOW", "SMPB": "SMJP", "2NHT": "SBTC", "SWIQ": "SBMC",
                           "SWKK": "SSKW", "SAIG": "SARI", "SBER": "SWEI"}
    airport_exclusions = ["SBNT", "SUPE", "6ASO", "SAMQ"]
    airline_replacements = {"VIP": "FPG", "BLC": "TAM"}

    def log_bulk(self):
        log.info('  store external_segment: %r', self.nresult)

    for csv_f in csv_files:  # loop through each file
            print('******************** processed csv:  ', csv_f)
            with open('%s/%s' % (tmp_dir, csv_f)) as csv_file:
                dict_reader = csv.DictReader(csv_file)
                all_rows = len(list(csv.DictReader(open('%s/%s' % (tmp_dir, csv_f)))))
                row_nb = 0
                previous_data = pd.DataFrame(columns=['origin', 'destination', 'year_month', 'airline', 'ref_code', 'passengers'])

                with External_Segment_Tmp.unordered_bulk(1000, execute_callback=log_bulk) as bulk:

                    for row in dict_reader:  # loop through each row (origin, destination) in file
                        row_nb += 1
                        for key, value in row.items():
                            if value == ':':
                                row[key] = ''

                        if ((row['PASSAGEIROS PAGOS'] == '0') and (row['PASSAGEIROS PAGOS'] == '0')) or \
                                (row['PASSAGEIROS PAGOS'] == ''):  # skip rows with no pax
                            continue

                        total_pax = int(row['PASSAGEIROS PAGOS']) + int(row['PASSAGEIROS GRÁTIS'])

                        row_airline = get_airline_by_icao(row['EMPRESA (SIGLA)'], row['EMPRESA (NOME)'], total_pax)

                        if row['AEROPORTO DE ORIGEM (SIGLA)'] in airport_exclusions or \
                            row['AEROPORTO DE DESTINO (SIGLA)'] in airport_exclusions:  # skip exclusions
                            continue

                        airport_origin = get_airport_by_icao(row['AEROPORTO DE ORIGEM (SIGLA)'],
                                                             row['AEROPORTO DE ORIGEM (NOME)'], total_pax)
                        airport_destination = get_airport_by_icao(row['AEROPORTO DE DESTINO (SIGLA)'],
                                                                  row['AEROPORTO DE DESTINO (NOME)'], total_pax)
                        if airport_destination is None:
                            continue
                        if airport_origin is None:
                            continue
                        if row_airline in airline_replacements:
                            row_airline = airline_replacements.get(row_airline)
                        if airport_origin in airport_replacement:
                            airport_origin = airport_replacement.get(airport_origin)
                        if airport_destination in airport_replacement:
                            airport_destination = airport_replacement.get(airport_destination)

                        year_month = '%04d-%02d' % (int(row['ANO']), int(row['MÊS']))
                        # Only treat the requested year_months
                        if year_month not in year_months:
                            continue

                        if year_month not in previous_data['year_month'].values:
                            if External_Segment_Tmp.find_one({'year_month': year_month, 'provider': provider}):
                                log.warning("This year_month (%s) already exists for provider %s", year_month, provider)

                        airline_ref_code = ref_code.get_airline_ref_code(row_airline, airport_origin,
                                                                         airport_destination,
                                                                         YearMonth(year_month))

                        if ((previous_data['origin'] == airport_origin) &
                                (previous_data['destination'] == airport_destination)
                                & (previous_data['year_month'] == year_month)
                                & (previous_data['airline'] == row_airline)
                                & (previous_data['ref_code'] == airline_ref_code)).any():
                            new_row = False
                            # Add to Excel file's total_pax the number of passengers you get from
                            # filtering previous_data on other columns
                            total_pax += int(previous_data['passengers'][
                                (previous_data['origin'] == airport_origin) &
                                (previous_data['destination'] == airport_destination)
                                & (previous_data['year_month'] == year_month)
                                & (previous_data['airline'] == row_airline)
                                & (previous_data['ref_code'] == airline_ref_code)])
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
                                   raw_rec=dict(row),
                                   both_ways=False,
                                   from_line=row_nb,
                                   from_filename=csv_f,
                                   url=full_url)

                        new_data = pd.Series({'origin': airport_origin, 'destination': airport_destination,
                                              'year_month': year_month, 'airline': row_airline,
                                              'ref_code': airline_ref_code, 'passengers': total_pax}).to_frame()
                        if new_row:
                            previous_data = previous_data.append(new_data.T, ignore_index=True)
                        else:
                            previous_data['passengers'][
                                (previous_data['origin'] == airport_origin) &
                                (previous_data['destination'] == airport_destination) &
                                (previous_data['airline'] == row_airline) &
                                (previous_data['ref_code'] == airline_ref_code) &
                                (previous_data['year_month'] == year_month)] = total_pax  # Modify previous_data's pax

                        query = dict((k, dic[k]) for k in ('origin', 'destination', 'year_month', 'provider',
                                                           'data_type', 'airline'))
                        bulk.find(query).upsert().update_one({'$set': dic, '$setOnInsert': dict(inserted=now)})
                        if row_nb % 1000 == 0:
                            print('{0:.3g}'.format(float(row_nb) / float(all_rows) * 100) + '%')
                log.info('stored: %r', bulk.nresult)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Load data from Brazil')
    parser.add_argument('year_months', type=str, nargs='+', help='Year_month(s) to download ([YYYY-MM, YYYY-MM...]')

    p = parser.parse_args()

    logging_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=logging_format)

    handler = BackupFileHandler(filename='load_Brazil.log', mode='w', backupCount=5)
    formatter = logging.Formatter(logging_format)
    handler.setFormatter(formatter)

    main_log = logging.getLogger()  # le root handler
    main_log.addHandler(handler)

    log = logging.getLogger('load_Brazil')

    log.info("Starting to update db with new files content from ANAC-Brazil website, version %s - %r",
             __version__, p)

    start_time = time.time()
    Model.init_db(def_w=True)
    year_months = p.year_months[0].split(', ')
    year = list(set([ym[0:4] for ym in p.year_months]))
    xslx_files = download_files(year)
    # xslx_files = os.listdir(tmp_dir)

    AIRPORTS_CODES = get_airports_codes()
    AIRLINES_BY_ICAO = get_airline_codes()
    unknown_icao_codes = pd.DataFrame(columns=['code', 'name', 'passengers'])
    unknown_airline_codes = pd.DataFrame(columns=['code', 'name', 'passengers'])

    get_data(xslx_files)

    log.info("\n\n--- %s seconds to populate db from ANAC-Brazil---", (time.time() - start_time))
    if len(unknown_icao_codes.index) > 0:
        unknown_icao_codes = unknown_icao_codes.sort_values('passengers', ascending=False)
        print("\n\n", len(unknown_icao_codes.index), "unknown airports (check the reasons why): \n", unknown_icao_codes)
    if len(unknown_airline_codes.index) > 0:
        unknown_airline_codes = unknown_airline_codes.sort_values('passengers', ascending=False)
        print("\n\n", len(unknown_airline_codes.index), "unknown airlines (check the reasons why): \n",
              unknown_airline_codes)
    log.info('End')

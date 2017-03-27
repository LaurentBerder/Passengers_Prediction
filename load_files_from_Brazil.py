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
import time
import urllib2
from pprint import pprint
from bs4 import BeautifulSoup
from optidb.model import *
from utils import utcnow
import csv
import pandas as pd
sys.path.append('../')

provider = 'ANAC-Brazil'
__version__ = 'V1.0.0'
icao_code = 'icao_code'
iata_code = 'iata_code'
AIRPORTS_CODES = dict()
AIRLINES_BY_ICAO = dict()
unknown_icao_codes = set()
unknown_airline_codes = set()
tmp_dir = '/tmp/brazil'
full_url = 'http://www.anac.gov.br/assuntos/dados-e-estatisticas/dados-estatisticos/dados-estatisticos'

logging.basicConfig(level=logging.DEBUG, format=0)
log = logging.getLogger('load_brazil')
log.setLevel(logging.DEBUG)

log.info('Updating db with new files content from ANAC-Brazil website, version %s...', __version__)
nb_record_inserted = 0
nb_record_updated = 0


def open_db():
    #config = {'ming.url':'mongodb://localhost/'}     #connect to local database instead of Optimode
    Model.init_db()


class External_Segment(Model):
    __collection__ = 'external_segment'


class External_Segment_Tmp(Model):
    __collection__ = 'external_segment_laurent_tests'


def get_airports_codes():
    """
    Get a dictionary of all airport codes for reference throughout import algorithm
    :return:
    """
    airports_codes = Airport.find({'code': {"$ne": None}},
                                  {'code': 1, 'iata_code': 1, 'icao_code': 1, 'country': 1,
                                   'city': 1, 'name': 1, '_id': 0})
    return dict((i.icao_code, i) for i in airports_codes if i.icao_code)


def get_airport_by_icao(airport_icao_code, airport_name):
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
        unknown_icao_codes.add(airport_icao_code + ':' + airport_name)
        return None
    return p['iata_code']


def get_airline_codes():
    """
    Get a dictionary of all airline codes for reference throughout import algorithm
    :return:
    """
    airlines = Company.find({"$or:" ['iata_code': {"$ne": None}, 'icao_code': {"$ne": None}]},
                            {'iata_code': 1, 'icao_code': 1, 'name': 1, '_id': 0})
    return dict((a.icao_code, a) for a in airlines if a.icao_code)


def get_airline_by_icao(airline_icao, airline_name):
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
        unknown_airline_codes.add(airline_icao + ':' + airline_name)
        return None
    return t['iata_code']


def download_files():
    """
    Get files from the web site

    The files are downloaded to 'tmp_dir' directory
    :return:
    """
    log.info('Getting files on the web')
    response = urllib2.urlopen(full_url)
    page = response.read()

    # get all files' download urls
    soup = BeautifulSoup(page, 'html.parser')
    files_urls = [href for href in [a.get('href') for a in soup.find_all('a')]
        if href and href.find('xlsb') >= 0]

    base, _ = os.path.split(files_urls[0])
    files_urls = [url for url in files_urls
        if url.find(base) == 0 >= 0]

    if not files_urls:
        raise CannotFindFileToDownload('Cannot find any file')

    if not os.path.isdir(tmp_dir):
        os.mkdir(tmp_dir)

    for url in files_urls: # download all xlsb files
        f_in = urllib2.urlopen('%s' % url)
        _, filename = os.path.split(url)
        if os.path.exists(filename.split('.')[0] + ".csv"):
            with open(os.path.join(tmp_dir, filename), 'wb') as f_out:
                f_out.write(f_in.read())
            os.system("export HOME=/tmp && libreoffice --headless --convert-to csv"
                      " %s/%s --outdir %s --infilter=CSV:44,34,UTF8"
                      % (tmp_dir, filename, tmp_dir))  # convert from xlsb to csv format with UTF-8 encoding
            os.system("rm -f %s/%s" % (tmp_dir, filename))  # delete original xlsb files
            log.info('File %s downloaded and converted', filename)

    csv_files = os.listdir(tmp_dir)

    return csv_files


def get_data(csv_files):
    """
    Populate the database with data extract in csv files
    :return:
    """
    global provider
    airport_replacement = {"SBCD": "SSCC", "SWUY": "SBUY", "SBJI": "SWJI", "RJNN": "RJNA", "SBPM": "SBPJ",
                           "SEQU": "SEQM", "SNQY": "SBJU", "SJDB": "SBDB", "SWJH": "SIZX", "SNNG": "SJNP",
                           "SDFR": "SDDN", "1AON": "SDOW", "SMPB": "SMJP", "2NHT": "SBTC", "SWIQ": "SBMC",
                           "SWKK": "SSKW", "SAIG": "SARI", "SBER": "SWEI"}
    airport_exclusions = {"SBNT", "SUPE", "6ASO", "SAMQ"}
    airline_replacements = {"VIP": "FPG", "BLC": "TAM"}

    def log_bulk(self):
        log.info('  store external_segment: %r', self.nresult)

    for csv_f in csv_files:  # loop through each file
            print('******************** processed csv:  ', csv_f)
            with open('%s/%s' % (tmp_dir, csv_f)) as csv_file:
                dict_reader = csv.DictReader(csv_file)
                row_nb = 0
                previous_data = pd.DataFrame(columns=['origin', 'destination', 'year_month', 'airline', 'passengers'])

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

                        row_airline = get_airline_by_icao(row['EMPRESA (SIGLA)'], row['EMPRESA (NOME)'])

                        if row['AEROPORTO DE ORIGEM (SIGLA)'] in airport_exclusions or \
                            row['AEROPORTO DE DESTINO (SIGLA)'] in airport_exclusions:  # skip exclusions
                            continue

                        airport_origin = get_airport_by_icao(row['AEROPORTO DE ORIGEM (SIGLA)'],
                                                             row['AEROPORTO DE ORIGEM (NOME)'])
                        airport_destination = get_airport_by_icao(row['AEROPORTO DE DESTINO (SIGLA)'],
                                                                  row['AEROPORTO DE DESTINO (NOME)'])
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

                        if ((previous_data['origin'] == airport_origin) &
                                (previous_data['destination'] == airport_destination)
                                & (previous_data['year_month'] == year_month)
                                & (previous_data['airline'] == row_airline)).any():
                            new_row = False
                            total_pax += int(previous_data['passengers'][
                                (previous_data['origin'] == airport_origin) &
                                (previous_data['destination'] == airport_destination)
                                & (previous_data['year_month'] == year_month)
                                & (previous_data['airline'] == row_airline)]) # Add to Excel file's total_pax the "passenger" integer you get from filtering previous_data on other columns
                        else:
                            new_row = True

                        dic = dict(provider=provider,
                                   data_type='airport',
                                   airline=row_airline,
                                   origin=airport_origin,
                                   destination=airport_destination,
                                   year_month=year_month,
                                   total_pax=total_pax,
                                   raw_rec=row,
                                   both_ways=False,
                                   from_line=row_nb,
                                   from_filename=csv_f,
                                   url=full_url)

                        new_data = pd.Series({'origin': airport_origin, 'destination': airport_destination,
                                                              'year_month': year_month, 'airline': row_airline,
                                                              'passengers': total_pax}).to_frame()
                        if new_row:
                            previous_data = previous_data.append(new_data.T, ignore_index=True)
                        else:
                            previous_data['passengers'][
                                (previous_data['origin'] == airport_origin) &
                                (previous_data['destination'] == airport_destination) &
                                (previous_data['airline'] == row_airline) &
                                (previous_data['year_month'] == year_month)] = total_pax  # Modify previous_data's pax

                        now = utcnow()
                        query = dict((k, dic[k]) for k in ('origin', 'destination', 'year_month', 'provider',
                                                           'data_type', 'airline'))
                        bulk.find(query).upsert().update_one({'$set': dic, '$setOnInsert': dict(inserted=now)})
                        if row_nb % 1000 == 0:
                            print(row_nb / len(dict_reader) * 100, "%")
                log.info('stored: %r', bulk.nresult)


def main():
    log.info('Starting ')
    start_time = time.time()
    open_db()

    download_files()
    csv_files = os.listdir(tmp_dir)

    global AIRPORTS_CODES
    AIRPORTS_CODES.update(get_airports_codes())
    AIRLINES_BY_ICAO.update(get_airline_codes())
    get_data(csv_files)
    log.info("\n\n--- %s seconds to populate db from ANAC-Brazil---", (time.time() - start_time))
    global unknown_icao_codes
    if len(unknown_icao_codes) > 0:
        pprint("\n\n", len(unknown_icao_codes), "unknown airports (check the reasons why): ", unknown_icao_codes)
    if len(unknown_airline_codes) > 0:
        pprint("\n\n", len(unknown_airline_codes), "unknown airlines (check the reasons why): ", unknown_airline_codes)
    log.info('End')


if __name__ == '__main__':
    main()

# -*- coding: utf-8 -*-

# -------------------------------------------------------------------------------
# Name:        Optimode / load_files_fro
# Purpose:     Store pax per month from/to eurostat website
#              Each country is considered as a separate source (because they can overlap with different information)
#              One file per country. Months as columns, origin/destination as rows.
#
# Author:      berder
#
# Created:     025/07/17
# Copyright:   (c) Arsynet 2016
# Licence:     Tous droits réservés
# -------------------------------------------------------------------------------

from __future__ import print_function, division
import sys
import time
sys.path.append('../')
import argparse
import logging
from optidb.model import *
from utils import utcnow, YearMonth
from utils.logging_utils import BackupFileHandler
import pandas as pd
import numpy as np


provider = 'Eurostat'
provider_tag = 'query_providers.%s' % provider
__version__ = 'V2.0.2'
tmp_dir = '/tmp/Eurostat'
url_template = 'http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?file=data/'
base_filename_list = ['avia_par_be.tsv.gz', 'avia_par_bg.tsv.gz', 'avia_par_cz.tsv.gz', 'avia_par_dk.tsv.gz',
                      'avia_par_de.tsv.gz', 'avia_par_ee.tsv.gz', 'avia_par_ie.tsv.gz', 'avia_par_el.tsv.gz',
                      'avia_par_es.tsv.gz', 'avia_par_fr.tsv.gz', 'avia_par_hr.tsv.gz', 'avia_par_it.tsv.gz',
                      'avia_par_cy.tsv.gz', 'avia_par_lv.tsv.gz', 'avia_par_lt.tsv.gz', 'avia_par_lu.tsv.gz',
                      'avia_par_hu.tsv.gz', 'avia_par_mt.tsv.gz', 'avia_par_nl.tsv.gz', 'avia_par_at.tsv.gz',
                      'avia_par_pl.tsv.gz', 'avia_par_pt.tsv.gz', 'avia_par_ro.tsv.gz', 'avia_par_si.tsv.gz',
                      'avia_par_sk.tsv.gz', 'avia_par_fi.tsv.gz', 'avia_par_se.tsv.gz', 'avia_par_uk.tsv.gz',
                      'avia_par_is.tsv.gz', 'avia_par_no.tsv.gz', 'avia_par_ch.tsv.gz']

urls = [url_template + base_filename for base_filename in base_filename_list]


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
                                  {'code': 1, 'iata_code': 1, 'icao_code': 1, 'country': 1, 'state': 1,
                                   'city': 1, 'name': 1, '_id': 0})
    return dict((i.icao_code, i) for i in airports_codes if i.icao_code)


def get_airport_by_icao(airport_icao_code):
    """
    Input icao_code, output corresponding iata_code
    :param airport_icao_code: code
    :return: iata_code
    """
    for e in airports_codes:
        if airports_codes.get(e).get('icao_code') == airport_icao_code:
            return airports_codes.get(e).get('iata_code')


def check_airport(airport, country, pax):
    """
    Multiple checks for each airports:
    - that the airport code exists in database
    - that it is located in the same country as the Eurostat's file
    Each failed test is recorded in a dataframe, along with the number of passengers concerned (for information on the
    importance of the airport).
    :param airport: icao_code
    :param country: string (ISO2 country code)
    :param pax: integer
    :return:
    """
    global wrong_airports
    global airports_codes
    # Check the airport code exists in Mongo. If not, skip line
    if airport not in airports_codes:
        if airport in wrong_airports['code'].values:
            wrong_airports.loc[wrong_airports['code'] == airport, 'passengers'] += pax
        else:
            info = pd.Series({'code': airport, 'Eurostat_country': country,
                              'Optimode_country': None,
                              'info_type': 'missing', 'passengers': pax})
            wrong_airports = wrong_airports.append(info, ignore_index=True)
        return False

    # Check that airports are in the same country as in the database (only if we have the country in our database)
    # Wikipedia: The European Commission generally uses ISO 3166-1 alpha-2 codes with two exceptions:
    # EL (not GR) is used to represent Greece, and UK (not GB) is used to represent the United Kingdom.
    if airports_codes.get(airport).get('country'):
        if country == 'EL':
            country = 'GR'
        if country == 'UK':
            country = 'GB'
        if airports_codes.get(airport).get('country') in ['RE', 'MQ', 'GP', 'GF', 'BL', 'YT', 'MF']:
            airports_codes[airport]['country'] = 'FR'
        if not country == airports_codes.get(airport).get('country'):
            if airport in wrong_airports['code'].values:
                wrong_airports.loc[wrong_airports['code'] == airport, 'passengers'] += pax
            else:
                info = pd.Series({'code': airport, 'Eurostat_country': country,
                                  'Optimode_country': airports_codes.get(airport).get('country'),
                                  'info_type': 'country', 'passengers': pax})
                wrong_airports = wrong_airports.append(info, ignore_index=True)
        return True
    else:
        return True


def populate_db(year, month):
    """
    Populate the database with data extract in urls list
    :return:
    """
    def log_bulk(self):
        log.info('  store external_segment: %r', self.nresult)

    now = utcnow()

    # urls = ['http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?file=data/avia_par_be.tsv.gz']
    for url in urls:  # loop through each file
            print('******************** processed url:  ', url)
            file_name = url.split('/')[-1][:-3]
            provider_country = file_name.split('_')[2].split('.')[0]
            full_provider = provider + "-" + provider_country
            dict_reader = pd.read_csv(url, compression='gzip', sep='\t')
            # First column's name is not easy to use with its specific format
            dict_reader.rename(index=str, columns={u'unit,tra_meas,airp_pr\\time': 'description'}, inplace=True)
            # Replace ': ' values with missing values
            dict_reader = dict_reader.replace(': ', np.NaN)
            # Remove columns that do not concern months (years or quarters)
            cols_to_keep = ['description']
            cols_to_keep.extend([x for x in dict_reader.columns if 'M' in x])
            dict_reader = dict_reader[cols_to_keep]
            # only keep rows with pax information
            dict_reader = dict_reader[(dict_reader['description'].str.contains("PAS_BRD_DEP")) | \
                                      (dict_reader['description'].str.contains("PAS_BRD_ARR"))]

            with External_Segment_Tmp.unordered_bulk(1000, execute_callback=log_bulk) as bulk:
                # loop through each row (origin, destination) in file
                row_nb = 0
                for row_index, row in dict_reader.iterrows():
                    row_nb +=1
                    if int(row_nb) % 1000 == 0:
                        print('** {0:.3g}'.format(int(row_nb) / len(dict_reader) * 100) + '% **')
                    # loop through each column (except the first one, which is the description) for passengers count
                    for key in row.keys()[1:len(row)]:
                        total_pax = row[key]
                        if pd.isnull(total_pax):
                            continue
                        else:
                            total_pax = int(total_pax)

                        #convert key to year_month
                        newkey = (str(key).strip().split('M')[0] + '-' + str(key).strip().split('M')[1])
                        year_month = newkey
                        ym = YearMonth(year_month)
                        # Restrict to the requested year_months
                        if ym.year not in year:
                            continue
                        if ym.month not in month:
                            continue
                        row[newkey] = row.pop(key)

                        # way: 'ARR' or 'DEP', meaning the airports will have to be swapped depending on the case
                        way = row['description'].split(',')[1][-3:]
                        icao_1 = row['description'].split(',')[2][3:7]
                        country_1 = row['description'].split(',')[2][0:2]
                        icao_2 = row['description'].split(',')[2][-4:]
                        country_2 = row['description'].split(',')[2][8:10]

                        if way == 'DEP':
                            origin_icao = icao_1
                            origin_country = country_1
                            destination_icao = icao_2
                            destination_country = country_2
                        else:
                            origin_icao = icao_2
                            origin_country = country_2
                            destination_icao = icao_1
                            destination_country = country_1
                        if check_airport(origin_icao, origin_country, total_pax):
                            origin = get_airport_by_icao(origin_icao)
                        else:
                            continue
                        if check_airport(destination_icao, destination_country, total_pax):
                            destination = get_airport_by_icao(destination_icao)
                        else:
                            continue

                        raw_rec = row.dropna()

                        dic = dict(provider=full_provider,
                                   data_type='airport',
                                   origin=[origin],
                                   destination=[destination],
                                   airline=['*'],
                                   airline_ref_code=['*'],
                                   year_month=[year_month],
                                   total_pax=total_pax,
                                   raw_rec=dict(raw_rec),
                                   both_ways=False,
                                   from_line=row_index,
                                   from_filename=file_name,
                                   url=url)
                        query = dict((k, dic[k]) for k in ('origin', 'destination', 'year_month', 'provider',
                                                           'data_type', 'airline'))
                        bulk.find(query).upsert().update_one({'$set': dic, '$setOnInsert': dict(inserted=now)})
            log.info('stored: %r', bulk.nresult)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Load data from Eurostat')
    parser.add_argument('year', type=int, nargs='+', help='Year(s) to download')
    parser.add_argument('--month', type=int, nargs='+', default=[1,2,3,4,5,6,7,8,9,10,11,12] ,help='Month(s) to download')
    p = parser.parse_args()

    logging_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=logging_format)

    handler = BackupFileHandler(filename='load_Eurostat.log', mode='w', backupCount=5)
    formatter = logging.Formatter(logging_format)
    handler.setFormatter(formatter)

    main_log = logging.getLogger()  # le root handler
    main_log.addHandler(handler)

    log = logging.getLogger('load_Eurostat')
    log.info('Starting to get data from Eurostat - version %s - %r', __version__, p)

    start_time = time.time()
    wrong_airports = pd.DataFrame(columns=['code', 'Eurostat_country', 'Optimode_country', 'passengers', 'info_type'])

    Model.init_db(def_w=True)
    year = p.year
    month = p.month
    airports_codes = get_airports_codes()
    populate_db(year, month)
    log.info("\n\n--- %s seconds to populate db with %d files---" % ((time.time() - start_time), len(urls)))
    if len(wrong_airports.index) > 0:
        wrong_airports = wrong_airports.sort_values('passengers', ascending=False)
        log.warning("%s wrong or unknown airports (check the reasons why): \n%s", len(wrong_airports.index),
                    wrong_airports)

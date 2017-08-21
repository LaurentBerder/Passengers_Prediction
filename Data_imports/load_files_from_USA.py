 #-*- coding: utf-8 -*-

# -------------------------------------------------------------------------------
# Name:        Optimode / load_files_from_USA
# Purpose:     Load data from files from USA's DoT
#
# Author:      berder
#
# Created:     22/10/2016
# Copyright:   (c) Arsynet 2015
# Licence:     Tous droits réservés
# -------------------------------------------------------------------------------

from __future__ import print_function, division
import argparse
import logging
import logging.handlers
import csv
import pandas as pd
from selenium import webdriver
import os
import time
import zipfile
import sys
sys.path.append('../')
from optidb.model import *
from utils import utcnow, YearMonth
from utils.logging_utils import BackupFileHandler
import determine_airline_ref_code as ref_code

__version__ = 'V1.0.1'
provider = 'USA'
tmp_dir = '/tmp/USA'
wrong_airports = pd.DataFrame(columns=['code', 'city', 'DOT_country/state', 'Optimode_country/state',
                                           'info_type', 'passengers'])
unknown_airlines = pd.DataFrame(columns=['code', 'name', 'passengers'])
airports_codes = dict()
# full_url = 'http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=292'   Another type of file, for O&D (without via)
full_url = 'http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=293'   # Download file by selecting all fields

if not os.path.isdir(tmp_dir):
     os.mkdir(tmp_dir)


class External_Segment_Tmp(Model):
    __collection__ = 'external_segment_laurent_tests'


def get_airports_codes():
    """
    Get a dictionary of all airport codes for reference throughout import algorithm
    :return:
    """
    airports_codes = Airport.find({'code': {"$ne": None}},
                                  {'code': 1, 'iata_code': 1, 'icao_code': 1, 'country': 1, 'state': 1,
                                   'city': 1, 'name': 1, '_id': 0})
    return dict((i.code, i) for i in airports_codes if i.code)


def get_airline_codes():
    """
    Get a dictionary of all airline codes for reference throughout import algorithm
    :return:
    """
    airlines = Company.find({'iata_code': {"$ne": None}},
                            {'iata_code': 1, 'icao_code': 1, 'name': 1, '_id': 0})
    return dict((a.iata_code, a) for a in airlines if a.iata_code)


def download_one(month, year):
    """
    Download a single year_month's flights. It is not easy to identify where the files are located, so this function
    mimics a user filling the form, selecting year and month as well as all variables, and clicking download.
    :param month: integer
    :param year: integer
    :return: a single renamed csv file
    """
    end_name = "US_Segments_%s-%s.csv" % (month, year)
    if end_name not in os.listdir(tmp_dir):

        # Set chrome options and reach the website
        options = webdriver.ChromeOptions()
        options.add_experimental_option("prefs", {
            "download.default_directory": tmp_dir,
            "download.prompt_for_download": False,
        })
        driver = webdriver.Chrome(chrome_options=options)
        driver.implicitly_wait(10)
        driver.get(full_url)
        assert "RITA" in driver.title

        # Select the demanded year and month (don't select months if "All", because default value is "All Months")
        driver.find_element_by_xpath("//select[@id='XYEAR']/option[@value=%s]" % year).click()
        if not month == "All":
            driver.find_element_by_xpath("//select[@id='FREQUENCY']/option[@value=%s]" % month).click()

        # Click the "select all variables checkbox", then click download
        driver.find_element_by_name('AllVars').click()
        driver.find_element_by_name("Download").click()

        # Wait for file to be downloaded
        time.sleep(120)
        driver.close()

        # Identify downloaded zip file, then unzip its content and delete zip file
        zip_name = max([tmp_dir + "/" + f for f in os.listdir(tmp_dir)], key=os.path.getctime)
        zip_ref = zipfile.ZipFile(zip_name, 'r')
        zip_ref.extractall(tmp_dir)
        zip_ref.close()
        os.remove(zip_name)

        # Identify csv file name, and rename to "US_Segment_month-year.csv"
        csv_name = max([tmp_dir + "/" + f for f in os.listdir(tmp_dir)], key=os.path.getctime)
        os.rename(os.path.join(tmp_dir, csv_name), os.path.join(tmp_dir, end_name))
        log.info("%s downloaded", end_name)
    return end_name


def robot_download(months, years):
    """
    Depending on whether month and/or year are single or multiple values, iterate to download the relevant files
    :param months: list of strings
    :param years: list of strings
    :return: list of downloaded csv files
    """
    csv_files = []
    for y in years:
        for m in months:
            if External_Segment_Tmp.find_one(
                    {'year_month': y + "-" + m, 'provider': provider}):
                log.warning("This year_month (%s) already exists for provider %s",
                            y + "-" + m, provider)
        if months == ['01','02','03','04','05','06','07','08','09','10','11','12']:
            csv_files.append(download_one("All", y))
        else:
            csv_files.append(download_one(m, y))

    return csv_files


def check_airport(airport, city, country, state, pax):
    """
    Multiple checks for each airports:
    - that the airport code exists in database
    - that it is located in the same country as the DOT's file
    - that it is located in the same state as the DOT's file (for US airports)
    Each failed test is recorded in a dataframe, along with the number of passengers concerned (for information on the
    importance of the airport).
    :param airport: code
    :param city: name
    :param country: name
    :param state: abbreviation
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
            info = pd.Series({'code': airport, 'city': city, 'DOT_country/state/state': country + ':' + state,
                              'Optimode_country/state': None,
                              'info_type': 'missing', 'passengers': pax})
            wrong_airports = wrong_airports.append(info, ignore_index=True)
        return False

    # Check that airports are in the same country as in the database
    if airports_codes.get(airport).get('country'):
        if not country == airports_codes.get(airport).get('country'):
            if airport in wrong_airports['code'].values:
                wrong_airports.loc[wrong_airports['code'] == airport, 'passengers'] += pax
            else:
                info = pd.Series({'code': airport, 'city': city, 'DOT_country/state': country,
                                  'Optimode_country/state': airports_codes.get(airport).get('country'),
                                  'info_type': 'country', 'passengers': pax})
                wrong_airports = wrong_airports.append(info, ignore_index=True)

    # For US airports, check that airports are in the same state as in the database
    if country == 'US' and airports_codes.get(airport).get('country') == 'US' and \
            not state == airports_codes.get(airport).get('state'):
        if airport in wrong_airports['code'].values:
            wrong_airports.loc[wrong_airports['code'] == airport, 'passengers'] += pax
        else:
            info = pd.Series({'code': airport, 'city': city, 'DOT_country/state': state,
                              'Optimode_country/state': airports_codes.get(airport).get('state'),
                              'info_type': 'state', 'passengers': pax})
            wrong_airports = wrong_airports.append(info, ignore_index=True)
    return True


def get_data(csv_files):
    """
    Populate the database with data from csv files
    :param csv_files: list of files
    :return:
    """
    global unknown_airlines
    global airports_codes
    now = utcnow()
    ref_code.init_cache()
    airports_codes = get_airports_codes()
    airline_codes = get_airline_codes()
    # Some codes are incorrect in the files from the FAA, "replacements" are the corrected codes):
    airport_replacement = {"JQF": "USA", "JRV": "NRR", "1G4": "GCW", "L41": "MYH", "7AK": "KQA", "UXR": "GMW",
                           "NYL": "YUM", "YR1": "YRC", "PBX": "PVL", "ZXA": "ROZ", "ZXU": "OQU", "NZC": "VQQ",
                           "FAQ": "FTI"}
    airline_replacements = {"09Q": "Q7", "SEB": "BB", "TQQ": "TA", "1BQ": "2D", "0MQ": "3E", "1YQ": "F4", "KAH": "M5",
                            "0JQ": "V2", "1DQ": "IS", "23Q": "5K", "1AQ": "VC", "1WQ": "UE", "J5": "J5", "FCQ": "6F",
                            "04Q": "TJ", "1RQ": "6G", "1QQ": "V9", "PBQ": "PV", "3F": "3F", "AMQ": "7Z", "3SD": "3S",
                            "20Q": "O5", "28Q": "ZB", "AAT": "YI", "AJQ": "4A", "2HQ": "7Q", "NLQ": "N5", "02Q": "ZT",
                            "07Q": "F8", "GCH": "ZS", "4EQ": "4E", "15Q": "6I"}
    # Some FAA codes are incorrect and have existing IATA equivalent, so they should be skipped:
    airport_exclusions = {"XWA", "NAD", "QMA", "ZXM", "ZXN", "RMN", "MQJ", "QMN", "QSO"}

    def log_bulk(self):
        log.info('store external_segment: %r', self.nresult)

    for csv_f in csv_files:  # loop through each file
            print('******************** processed csv:  ', csv_f)
            with open('%s/%s' % (tmp_dir, csv_f)) as csv_file:
                dict_reader = csv.DictReader(csv_file)
                row_nb = 0
                previous_data = pd.DataFrame(columns=['origin', 'destination', 'year_month', 'airline', 'passengers'])
                all_rows = len(list(csv.DictReader(open('%s/%s' % (tmp_dir, csv_f)))))
                """
                In previous_data, we store all the lines that are sent to bulk, to refer to for next lines.
                This allows sum of passengers for similar origin/destination/year_month/airline tuples.
                """

                with External_Segment_Tmp.unordered_bulk(1000, execute_callback=log_bulk) as bulk:

                    for row in dict_reader:  # loop through each row (origin, destination) in file
                        row_nb += 1
                        for key, value in row.items():
                            if value == ':':
                                row[key] = ''

                        passengers = int(row['PASSENGERS'].split('.')[0])
                        if passengers <= 0:  # skip rows with no pax
                            continue

                        row_airline = row['UNIQUE_CARRIER']
                        airport_origin = row['ORIGIN']
                        airport_destination = row['DEST']
                        if airport_origin in airport_exclusions or airport_destination in airport_exclusions:  # skip exclusions
                            continue
                        if row_airline in airline_replacements:
                            row_airline = airline_replacements.get(row_airline)
                        if airport_origin in airport_replacement:     # correct the wrong codes
                            airport_origin = airport_replacement.get(airport_origin)
                        if airport_destination in airport_replacement:     # correct the wrong codes
                            airport_destination = airport_replacement.get(airport_destination)

                        if row_airline not in airline_codes:  # Check airline
                            if row_airline in unknown_airlines['code'].values:
                                unknown_airlines.loc[unknown_airlines['code'] == row_airline, 'passengers'] += passengers
                            else:
                                info = pd.Series({'code': row_airline, 'name': row['CARRIER_NAME'], 'passengers': passengers})
                                unknown_airlines = unknown_airlines.append(info, ignore_index=True)
                            continue


                        if not check_airport(airport_origin, row['ORIGIN_CITY_NAME'],
                                             row['ORIGIN_COUNTRY'], row['ORIGIN_STATE_ABR'], passengers):
                            continue
                        if not check_airport(airport_destination, row['DEST_CITY_NAME'],
                                             row['DEST_COUNTRY'], row['DEST_STATE_ABR'], passengers):
                            continue

                        year_month = '%04d-%02d' % (int(row['YEAR']), int(row['MONTH']))
                        airline_ref_code = ref_code.get_airline_ref_code(row_airline, airport_origin,
                                                                         airport_destination,
                                                                         YearMonth(year_month))
                        if ((previous_data['origin'] == airport_origin) &
                                (previous_data['destination'] == airport_destination)
                                & (previous_data['year_month'] == year_month)
                                & (previous_data['airline'] == row_airline)).any():
                            new_row = False
                            # Add to Excel file's total_pax the "passenger" integer you get from filtering
                            # previous_data on other columns
                            passengers += int(previous_data['passengers'][
                                (previous_data['origin'] == airport_origin) &
                                (previous_data['destination'] == airport_destination) &
                                (previous_data['year_month'] == year_month) &
                                (previous_data['airline'] == row_airline)])
                        else:
                            new_row = True

                        dic = dict(provider=provider,
                                   data_type='airport',
                                   airline=[row_airline],
                                   airline_ref_code=[airline_ref_code],
                                   total_pax=passengers,
                                   overlap=[],
                                   origin=[airport_origin],
                                   destination=[airport_destination],
                                   year_month=[year_month],
                                   raw_rec=dict(row), both_ways=False,
                                   from_line=row_nb, from_filename=csv_f, url=full_url)

                        new_data = pd.Series({'origin': airport_origin, 'destination': airport_destination,
                                              'year_month': year_month, 'airline': row_airline,
                                              'passengers': passengers}).to_frame()
                        if new_row:
                            previous_data = previous_data.append(new_data.T, ignore_index=True)
                        else:
                            previous_data['passengers'][
                                (previous_data['origin'] == airport_origin) &
                                (previous_data['destination'] == airport_destination) &
                                (previous_data['airline'] == row_airline) &
                                (previous_data['year_month'] == year_month)] = passengers  # Modify previous_data's pax

                        query = dict((k, dic[k]) for k in ('origin', 'destination', 'year_month', 'provider',
                                                           'data_type', 'airline'))
                        bulk.find(query).upsert().update_one({'$set': dic, '$setOnInsert': dict(inserted=now)})
                        if row_nb % 1000 == 0:
                            print('{0:.3g}'.format(row_nb / all_rows * 100) + '%')
                log.info('stored: %r', bulk.nresult)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Load data from USA')
    parser.add_argument('year_months', type=str, nargs='+', help='Year_month(s) to download ([YYYY-MM, YYYY-MM...]')

    p = parser.parse_args()

    logging_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=logging_format)

    handler = BackupFileHandler(filename='load_USA.log', mode='w', backupCount=5)
    formatter = logging.Formatter(logging_format)
    handler.setFormatter(formatter)

    main_log = logging.getLogger()  # le root handler
    main_log.addHandler(handler)

    log = logging.getLogger('load_USA')

    log.info("load_files_from_USA's_DOT - version %s - %r", __version__, p)

    log.info('Starting to get data from Bureau of Transportation Statistics')
    start_time = time.time()
    Model.init_db(def_w=True)
    years = list(set([ym[0:4] for ym in p.year_months]))
    months = list(set([ym[5:7] for ym in p.year_months]))
    csv_files = robot_download(months, years)
    # csv_files = os.listdir(tmp_dir)
    get_data(csv_files)
    log.info("\n\n--- %s seconds to populate db with %d files---" % ((time.time() - start_time), len(csv_files)))
    global wrong_airports
    global unknown_airlines
    if len(wrong_airports.index) > 0:
        wrong_airports = wrong_airports.sort_values('passengers', ascending=False)
        log.warning("%s wrong or unknown airports (check the reasons why): \n%s", len(wrong_airports.index),
                    wrong_airports)
    if len(unknown_airlines.index) > 0:
        unknown_airlines = unknown_airlines.sort_values('passengers', ascending=False)
        log.warning("%s unknown airlines (check the reasons why): \n%s", len(unknown_airlines.index), unknown_airlines)
    log.info('End')

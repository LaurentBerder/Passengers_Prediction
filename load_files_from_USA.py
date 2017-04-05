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

from __future__ import print_function
import sys
import logging
import logging.handlers
from optidb.model import *
from utils import utcnow
import csv
import pandas as pd
from selenium import webdriver
import os
import time
import zipfile
from utils import config     # connect to local database instead of Optimode
sys.path.append('../')

provider = 'USA'
__version__ = 'V1.0.0'
unknown_airports = set()
unknown_airlines = set()
tmp_dir = '/tmp/USA'
# full_url = 'http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=292'   Another type of file, less well defined
full_url = 'http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=293'   # Download file by selecting all fields

if not os.path.isdir(tmp_dir):
     os.mkdir(tmp_dir)


logging.basicConfig(level=logging.DEBUG, format=0)
log = logging.getLogger('load_USA')
log.setLevel(logging.DEBUG)


def open_db():
    #config['ming.url'] = 'mongodb://localhost/'     # connect to local database instead of Optimode
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

    # Select the demanded year and month
    driver.find_element_by_xpath("//select[@id='XYEAR']/option[@value=%s]" % year).click()
    driver.find_element_by_xpath("//select[@id='FREQUENCY']/option[@value=%s]" % month).click()

    # Click the "select all variables checkbox", then click download
    driver.find_element_by_name('AllVars').click()
    driver.find_element_by_name("Download").click()

    # Wait for file to be downloaded
    time.sleep(20)
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


def robot_download(month, year):
    """
    Depending on whether month and/or year are single or multiple values, iterate to download the relevant files
    :param month: integer or tuple of integers
    :param year: integer or tuple of integers
    :return: list of downloaded csv files
    """
    csv_files = []
    if not isinstance(month, tuple) and not isinstance(year, tuple):
        csv_files.append(download_one(month, year))
    else:
        if isinstance(month, tuple) and isinstance(year, tuple):
            for y in year:
                for m in month:
                    csv_files.append(download_one(m, y))
        else:
            if isinstance(month, tuple):
                for m in month:
                    csv_files.append(download_one(m, year))
            else:
                for y in year:
                    csv_files.append(download_one(month, y))
    return csv_files


def get_data(csv_files):
    """
    Populate the database with data from csv files
    :return:
    """
    airline_codes = get_airline_codes()
    airport_codes = get_airports_codes()
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
                        if airport_origin not in airport_codes:
                            unknown_airports.add(airport_origin + ":" + row['ORIGIN_CITY_NAME'])
                            continue
                        if airport_destination not in airport_codes:
                            unknown_airports.add(airport_destination + ":" + row['DEST_CITY_NAME'])
                            continue
                        if row_airline not in airline_codes:
                            unknown_airlines.add(row_airline + ":" + row['CARRIER_NAME'])
                            continue

                        year_month = '%04d-%02d' % (int(row['YEAR']), int(row['MONTH']))

                        if ((previous_data['origin'] == airport_origin) &
                                (previous_data['destination'] == airport_destination)
                                & (previous_data['year_month'] == year_month)
                                & (previous_data['airline'] == row_airline)).any():
                            new_row = False
                            passengers += int(previous_data['passengers'][
                                (previous_data['origin'] == airport_origin) &
                                (previous_data['destination'] == airport_destination) &
                                (previous_data['year_month'] == year_month) &
                                (previous_data['airline'] == row_airline)]) # Add to Excel file's total_pax the "passenger" integer you get from filtering previous_data on other columns
                        else:
                            new_row = True

                        dic = dict(provider=provider, data_type='airport', airline=row_airline, total_pax=passengers,
                                   origin=airport_origin, destination=airport_destination, year_month=year_month,
                                   raw_rec=row, both_ways=False, from_line=row_nb, from_filename=csv_f, url=full_url)

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

                        now = utcnow()
                        query = dict((k, dic[k]) for k in ('origin', 'destination', 'year_month', 'provider',
                                                           'data_type', 'airline'))
                        bulk.find(query).upsert().update_one({'$set': dic, '$setOnInsert': dict(inserted=now)})
                        if row_nb % 10000 == 0:
                            print(row_nb / len(dict_reader) * 100, "%")
                log.info('stored: %r', bulk.nresult)



def main():
    log.info('Starting to get data from Bureau of Transportation Statistics')
    start_time = time.time()
    open_db()
    year = str(input("Enter year(s) to download (separated by a comma"))
    if ',' in year:
        year = year.split(',')
    month = input("Enter month number(s) (separated by a comma")
    if ',' in month:
        month = month.split(',')
    csv_files = robot_download(month, year)
    # csv_files = os.listdir(tmp_dir)
    csv_files = ["US_Segments_2014.csv", "US_Segments_2015.csv", "US_Segments_2016.csv"]
    get_data(csv_files)
    log.info("\n\n--- %s seconds to populate db with %d files---" % ((time.time() - start_time), len(csv_files)))
    if len(unknown_airports) > 0:
        print("\n", len(unknown_airports), "unknown airports (check the reasons why): ", unknown_airports)
    if len(unknown_airlines) > 0:
        print("\n", len(unknown_airlines), "unknown airlines (check the reasons why): ", unknown_airlines)
    log.info('End')

if __name__ == '__main__':
    main()

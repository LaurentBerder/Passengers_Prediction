 #-*- coding: utf-8 -*-

# -------------------------------------------------------------------------------
# Name:        Optimode / load_files_from_Australia
# Purpose:     Load data from files from Australian civil aviation BITRE
#
# Author:      berder
#
# Created:     10/04/2017
# Copyright:   (c) Arsynet 2015
# Licence:     Tous droits réservés
# -------------------------------------------------------------------------------

from __future__ import print_function
import time
import argparse
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
import locale
import logging
import logging.handlers
import os
import sys
sys.path.append('../')
from optidb.model import *
from utils import utcnow
from utils.logging_utils import BackupFileHandler
import pandas as pd
import numpy as np
from unidecode import unidecode
import re
import zipfile


locale.setlocale(locale.LC_TIME, "en_US.utf8") # Make sure the months are expressed in English
provider = 'Australia'
provider_tag = 'query_providers.%s' % provider
__version__ = 'V1.0.0'
no_capa = list()
airports_codes = dict()
tmp_dir = '/tmp/australia'
international_url = 'https://bitre.gov.au/publications/ongoing/international_airline_activity-time_series.aspx'
domestic_url = 'https://bitre.gov.au/publications/ongoing/domestic_airline_activity-time_series.aspx'


class External_Segment(Model):
    __collection__ = 'external_segment'


class External_Segment_Tmp(Model):
    __collection__ = 'external_segment_laurent_tests'


def get_international_file():
    """
    Download historical international flights file
    :return:
    """
    end_name = "Australia_international.xlsx"
    # Only download the file once
    if end_name not in os.listdir(tmp_dir):

        # Set chrome options and reach the website
        options = webdriver.ChromeOptions()
        options.add_experimental_option("prefs", {
            "download.default_directory": tmp_dir,
            "download.prompt_for_download": False,
        })
        driver = webdriver.Chrome(chrome_options=options)
        driver.implicitly_wait(10)
        driver.get(international_url)

        # Download the international file for city pairs up to current date
        try:
            driver.find_element_by_xpath("//a[contains(@href, 'CityPairs') and contains(@href, 'Current')]")
        except:
            log.info("International file not found")
            driver.close()
            pass
        else:
            # Click on the right excel file's link
            driver.find_element_by_xpath(
                "//a[contains(@href, 'CityPairs') and contains(@href, 'Current') and contains(@href, 'xls')]").click()
            time.sleep(240)  # Wait until file has finished downloading
            # Identify latest downloaded excel file name, and rename to "Australia_international.xlsx"
            xlsx_name = max([tmp_dir + "/" + f for f in os.listdir(tmp_dir)], key=os.path.getctime)
            os.rename(os.path.join(tmp_dir, xlsx_name), os.path.join(tmp_dir, end_name))
            log.info("%s downloaded", end_name)

            driver.close()


def get_domestic_file():
    """
    Download historical domestic flights file
    :return:
    """
    end_name = "Australia_domestic.xlsx"
    # Only download the file once
    if end_name not in os.listdir(tmp_dir):
        # Set chrome options and reach the website
        options = webdriver.ChromeOptions()
        options.add_experimental_option("prefs", {
            "download.default_directory": tmp_dir,
            "download.prompt_for_download": False,
        })
        driver = webdriver.Chrome(chrome_options=options)
        driver.implicitly_wait(10)
        driver.get(domestic_url)

        # Download the domestic file for city pairs up to current date
        try:
            driver.find_elements_by_xpath("//a[contains(@href, 'TopRoutes') and contains(@href, 'zip')]")
        except:
            log.info("Domestic file not found")
            driver.close()
            pass
        else:
            # Click on the right excel file's link
            driver.find_elements_by_xpath("//a[contains(@href, 'TopRoutes') and contains(@href, 'zip')]")[0].click()
            time.sleep(120)  # Wait until file has finished downloading
            # Identify latest downloaded zip file name, extract and rename new file to "Australia_domestic.xlsx"
            zip_name = max([tmp_dir + "/" + f for f in os.listdir(tmp_dir)], key=os.path.getctime)
            zip_ref = zipfile.ZipFile(zip_name, 'r')
            zip_ref.extractall(tmp_dir)
            zip_ref.close()
            os.remove(zip_name)
            xlsx_name = max([tmp_dir + "/" + f for f in os.listdir(tmp_dir)], key=os.path.getctime)
            os.rename(os.path.join(tmp_dir, xlsx_name), os.path.join(tmp_dir, end_name))
            log.info("%s downloaded", end_name)

            driver.close()


def download_files(year_months):
    """
    Check if requested year_months already exist in database. Download domestic and international files for processing
    :param year_months: list of strings (YYYY-MM)
    :return:
    """
    if not os.path.isdir(tmp_dir):
        os.mkdir(tmp_dir)
    for ym in year_months:
        if External_Segment_Tmp.find_one(
                {'year_month': ym, 'provider': provider}):
            log.warning("This year_month (%s) already exists for provider %s",
                        ym, provider)

    get_domestic_file()
    get_international_file()
    xlsx_files = os.listdir(tmp_dir)
    return xlsx_files


def find_airports_by_name(name, perimeter):
    """
    This function looks up the name of an airport or city in the Excel file based on the Australia-specific
    field of "provider_query", or on "query_names".
    Failures of this function are reported at the end of the algorithm to enrich (manually) the "provider_query" with
    the help of submit_query_providers() function.
    :param name: an upper case string
    :param perimeter: string (indication of international or mexican-only airports)
    :return: airport code (or None)
    """
    if perimeter == "australian":
        city_clean = name.lower().replace('.', '').split('-')[0].split('/')[0].split(',')[0].strip()
        airports = pd.DataFrame.from_records(
            list(Airport.find({'query_names': city_clean, 'code_type': {'$in': ['city','airport']}, 'country': 'AU'},
                              {'_id': 0, 'code': 1, 'name': 1, 'city': 1})))
        if airports.empty:
            airports = pd.DataFrame.from_records(
                list(Airport.find({provider_tag: name.strip(), 'code_type': {'$in': ['city','airport']}, 'country': 'AU'},
                                  {'_id': 0, 'code': 1, 'name': 1, 'city': 1})))
    else:
        city_clean = name.lower().replace('.', '').split('-')[0].split('/')[0].split(',')[0].strip()
        airports = pd.DataFrame.from_records(
            list(Airport.find({'query_names': city_clean, 'code_type': {'$in': ['city','airport']}},
                              {'_id': 0, 'code': 1, 'name': 1, 'city': 1})))
        if airports.empty:
            airports = pd.DataFrame.from_records(
                list(Airport.find({provider_tag: name.strip(), 'code_type': {'$in': ['city','airport']}},
                                  {'_id': 0, 'code': 1, 'name': 1, 'city': 1})))
    return set(airports['code']) if not airports.empty else None


def get_airports_codes():
    """
    Get a dictionary of all airport codes for reference throughout import algorithm
    :return:
    """
    airports_codes = Airport.find({'code': {"$ne": None}},
                                  {'code': 1, 'iata_code': 1, 'icao_code': 1, 'country': 1, 'state': 1,
                                   'city': 1, 'name': 1, '_id': 0})
    return dict((i.code, i) for i in airports_codes if i.code)


def submit_query_providers():
    """
    Save new query names identified from previous run's failures to improve future imports
    """
    print('Saving new airports codes...')
    airport_replacement = {'RGN': 'RANGOON', 'MXP': 'MALPENSA', 'MRU': 'MARUITIUS', 'MUC': 'MUENCHEN', 'KUL': 'KUALALUMPUR', 'AUH': 'ABUDHABI'}
    with Airport.unordered_bulk() as bulk:
        for airport in airport_replacement:
            name = airport_replacement.get(airport)
            log.info('airport: %s', airport)
            bulk.find(dict(code=airport, code_type='airport')).upsert().update_one(
                {'$addToSet': {provider_tag: name}})
    log.info('load_airports_names: %r', bulk.nresult)


def check_airport(airport, pax, perimeter, city=None, country=None):
    """
    Multiple checks for each airports:
    - that the airport code exists in database
    - that it is located in the same country as the downloaded file
    Each failed test is recorded in a dataframe, along with the number of passengers concerned (for information on the
    importance of the airport).
    :param airport: code
    :param pax: integer
    :param perimeter: 'domestic' or 'international'
    :param city: string
    :param country: string
    :return: False if airport not in database
    """
    global unknown_airports, airports_codes
    # Check the airport code exists in Mongo. If not, skip line and save in unknown_airports
    if perimeter == 'domestic':
        if airport not in airports_codes:
            if airport in unknown_airports['code'].values:
                unknown_airports.loc[unknown_airports['code'] == airport, 'passengers'] += pax
            else:
                info = pd.Series({'code': airport, 'city': city, 'country': country, 'passengers': pax})
                unknown_airports = unknown_airports.append(info, ignore_index=True)
            return False
        else:
            return True
    else:
        if city in unknown_airports['city'].values:
            unknown_airports.loc[unknown_airports['city'] == city, 'passengers'] += pax
        else:
            info = pd.Series({'code': airport, 'city': city, 'country': country, 'passengers': pax})
            unknown_airports = unknown_airports.append(info, ignore_index=True)



def format_file(xlsx_f, perimeter):
    """
    Domestic and International files do not have the same format (though the required information is the same in both),
    so we harmonize them, and make sure all column names are the same.
    :param xls: a DataFrame
    :param perimeter: a string, either "domestic" or "international"
    :return: the same DataFrame, with a standardized format
    """
    if perimeter == 'domestic':
        xls = pd.read_excel(tmp_dir + "/" + xlsx_f, sheetname="Top Routes")
        xls.drop(xls.columns[11:], axis=1, inplace=True)
        # Rename columns
        new_columns = xls.columns.values
        new_columns = ['From', 'To', 'Year', 'Month', 'Passengers', 'Trips', 'Load_factor', 'Distance_km',
                       'RPK', 'ASK', 'Seats']
        xls.columns = new_columns
        # Only keep real data
        xls = xls[pd.notnull(xls.To)]
        xls = xls[pd.notnull(xls.Month)]
        # Write year_month
        xls['year_month'] = xls.Year.map(str) + "-" + xls.Month.map("{:02}".format)
        # Only keep the requested year_months
        xls = xls[xls['year_month'].isin(year_months)]
        # Remove rows with no passenger counts
        xls['Passengers'] = pd.to_numeric(xls['Passengers'], errors='coerce')
        xls = xls[pd.notnull(xls.Passengers)]

    else:
        xls = pd.read_excel(tmp_dir + "/" + xlsx_f, sheetname="Data", header=0)
        xls.drop(xls.columns[13:], axis=1, inplace=True)
        xls['year_month'] = xls['Month'].map(lambda x: str(x.year) + "-" + '%02d' % (x.month))
        # Only keep the requested year_months
        xls = xls[xls['Month'].isin(year_months)]
        # Remove rows with no passenger counts
        xls['TotalPax'] = pd.to_numeric(xls['TotalPax'], errors='coerce')
        xls['PaxIn'] = pd.to_numeric(xls['PaxIn'], errors='coerce')
        xls['PaxOut'] = pd.to_numeric(xls['PaxOut'], errors='coerce')
        xls = xls[pd.notnull(xls.TotalPax)]
    return xls


def get_data(xlsx_files, year_months):
    """
    Populate the database with data extract in xlsx files. One file per year_month, only one tab per file.
    Back/Forth routes in rows, one column per way.
    :param xlsx_files: dict of file names
    :param year_months: list of strings (YYYY-MM)
    :return:
    """
    global provider, unknown_airports
    now = utcnow()
    airport_replacement = {}
    airport_exclusions = {}

    def log_bulk(self):
        log.info('  store external_segment: %r', self.nresult)

    for xlsx_f in xlsx_files:  # loop through each file
        previous_data = pd.DataFrame(columns=['origin', 'destination', 'year_month', 'passengers'])
        row_nb = 0
        if "domestic" in xlsx_f:
            perimeter = "domestic"
            full_provider = provider + ' - domestic'
        else:
            perimeter = "international"
            full_provider = provider + ' - intl'
        print('******************** processing Excel file:', xlsx_f)
        xls = format_file(xlsx_f, perimeter)
        all_rows = len(xls.index)
        row_nb = 0

        with External_Segment_Tmp.unordered_bulk(1000, execute_callback=log_bulk) as bulk:
            for row_index, row in xls.iterrows():  # loop through each row (origin, destination) in file
                row_nb += 1
                year_month = row['year_month']
                if year_month not in year_months:
                    continue
                # First the process for domestic files
                if perimeter == "domestic":
                    passengers = int(row['Passengers'])
                    airport_origin = row['From']
                    airport_destination = row['To']
                    if airport_origin in airport_exclusions or airport_destination in airport_exclusions:  # skip exclusions
                        continue
                    if airport_origin in airport_replacement:  # correct the wrong codes
                        airport_origin = airport_replacement.get(airport_origin)
                    if airport_destination in airport_replacement:  # correct the wrong codes
                        airport_destination = airport_replacement.get(airport_destination)
                    if not check_airport(airport_destination, passengers, perimeter):
                        continue
                    if not check_airport(airport_destination, passengers, perimeter):
                        continue

                    if ((previous_data['origin'] == airport_origin) &
                            (previous_data['destination'] == airport_destination)
                            & (previous_data['year_month'] == year_month)).any():
                        new_row = False
                        # Add to Excel file's total_pax the "passenger" integer you get from filtering
                        # previous_data on other columns
                        passengers += int(previous_data['passengers'][
                                              (previous_data['origin'] == airport_origin) &
                                              (previous_data['destination'] == airport_destination) &
                                              (previous_data['year_month'] == year_month)])
                    else:
                        new_row = True
                    dic = dict(provider=full_provider,
                               data_type='airport',
                               airline=['*'],
                               airline_ref_code=['*'],
                               total_pax=passengers,
                               overlap=[],
                               origin=[airport_origin],
                               destination=[airport_destination],
                               year_month=[year_month],
                               raw_rec=dict(row), both_ways=False,
                               from_line=row_index, from_filename=xlsx_f, url=domestic_url)

                    new_data = pd.Series({'origin': airport_origin, 'destination': airport_destination,
                                          'year_month': year_month, 'passengers': passengers}).to_frame()
                    if new_row:
                        previous_data = previous_data.append(new_data.T, ignore_index=True)
                    else:
                        previous_data['passengers'][
                            (previous_data['origin'] == airport_origin) &
                            (previous_data['destination'] == airport_destination) &
                            (previous_data['year_month'] == year_month)] = passengers  # Modify previous_data's pax

                    query = dict((k, dic[k]) for k in ('origin', 'destination', 'year_month', 'provider',
                                                       'data_type', 'airline'))
                    bulk.find(query).upsert().update_one({'$set': dic, '$setOnInsert': dict(inserted=now)})
                    if row_nb % 1000 == 0:
                        print('{0:.3g}'.format(float(row_nb) / float(all_rows) * 100) + '%')

                # Now for international files
                else:
                    # Handle missing data, written ".." in the excel files
                    row.replace('..', np.nan, inplace=True)
                    if pd.isnull(row['TotalPax']):
                        continue
                    if pd.isnull(row['PaxIn']):
                        way_in = False
                    else:
                        way_in = True
                        passengers_in = int(row['PaxIn'])
                    if pd.isnull(row['PaxOut']):
                        way_out = False
                    else:
                        way_out = True
                        passengers_out = int(row['PaxOut'])
                    australian_city = row['AustralianPort']
                    other_city = row['ForeignPort']
                    other_country = row['Country']
                    australian_airport = find_airports_by_name(australian_city, 'australian')
                    other_airport = find_airports_by_name(other_city, 'other')
                    # If one of the airports is not recognized by name, store and skip
                    if not australian_airport:
                        check_airport(airport=None, pax=int(row['TotalPax']), perimeter='international',
                                      city=australian_city, country='Australia')
                        continue
                    if not other_airport:
                        check_airport(airport=None, pax=int(row['TotalPax']), perimeter='international',
                                      city=other_city, country=other_country)
                        continue

                    # Only store data if there was an integer in the PaxIn and/or PaxOut
                    if way_in:
                        dic_in = dict(provider=full_provider,
                               data_type='airport',
                               airline=['*'],
                               airline_ref_code=['*'],
                               total_pax=passengers_in,
                               origin=sorted(other_airport),
                               destination=sorted(australian_airport),
                               year_month=[row['year_month']],
                               raw_rec=dict(row), both_ways=False,
                               from_line=row_index, from_filename=xlsx_f, url=domestic_url)
                        query = dict((k, dic_in[k]) for k in ('origin', 'destination', 'year_month', 'provider', 'data_type'))
                        bulk.find(query).upsert().update_one({'$set': dic_in, '$setOnInsert': dict(inserted=now)})

                    if way_out:
                        dic_out = dict(provider=full_provider,
                                  data_type='airport',
                                  airline=['*'],
                                  airline_ref_code=['*'],
                                  total_pax=passengers_out,
                                  origin=sorted(australian_airport),
                                  destination=sorted(other_airport),
                                  year_month=[row['year_month']],
                                  raw_rec=dict(row), both_ways=False,
                                  from_line=row_index, from_filename=xlsx_f, url=domestic_url)
                        query = dict((k, dic_out[k]) for k in ('origin', 'destination', 'year_month', 'provider', 'data_type'))
                        bulk.find(query).upsert().update_one({'$set': dic_out, '$setOnInsert': dict(inserted=now)})
        log.info('stored: %r', bulk.nresult)


def print_full(x):
    pd.set_option('display.max_rows', len(x))
    print(x)
    pd.reset_option('display.max_rows')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Load data from Brazil')
    parser.add_argument('year_months', type=str, nargs='+', help='Year_month(s) to download ([YYYY-MM, YYYY-MM...]')

    p = parser.parse_args()

    logging_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=logging_format)
    handler = BackupFileHandler(filename='load_files_from_Australia.log', mode='w', backupCount=20)
    formatter = logging.Formatter(logging_format)
    handler.setFormatter(formatter)
    main_log = logging.getLogger()  # le root handler
    main_log.addHandler(handler)
    log = logging.getLogger('load_files_Autralia')
    log.info("Updating db with new file contents from Australia's civil aviation (BITRE) website, version %s...", __version__)

    log.info('Starting to get data')
    start_time = time.time()

    Model.init_db(def_w=True)

    year_months = p.year_months[0].split(', ')
    # submit_query_providers()   # update "provider_query" tags with previously unidentified airports
    xlsx_files = download_files(year_months)
    # xlsx_files = os.listdir(tmp_dir)
    airports_codes = get_airports_codes()

    unknown_airports = pd.DataFrame(columns=['code', 'city', 'country', 'passengers'])

    get_data(xlsx_files, year_months)

    log.info("\n\n--- %s seconds to populate db with %d files---" % ((time.time() - start_time), len(xlsx_files)))
    if len(unknown_airports.index) > 0:
        unknown_airports = unknown_airports.sort_values('passengers', ascending=False)
        log.warning("%s unknown airports (check the reasons why): \n%s", len(unknown_airports.index), print_full(unknown_airports))
    if len(no_capa) > 0:
        log.warning("%s international segments with no capacity (check the reasons why): ", len(no_capa), print_full(no_capa))
    log.info('End')
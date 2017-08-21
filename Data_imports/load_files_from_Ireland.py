# -*- coding: utf-8 -*-

# -------------------------------------------------------------------------------
# Name:        Optimode / load_files_from_Ireland
# Purpose:     Store pax per month from/to ireland airports routes in database.
#              File contains all year_months as columns. Irish airports are in the first column, only displayed once
#              instead of being repeated on each line. Same for the 'inward' or 'outward' indicator in column 2.
#              Airports are identified solely by their iata_code
#
#              Parameters selected on the website are:
#              - Direction: Inward, Outward
#              - Foreign airport: Select all
#              - Irish airport: Select all
#              - Month: the last month available (ex 2016M04)
#              To download the file:
#              - Download file as: 'Comma Separated(*.csv)'
#              - Edit table: 'Show codes only'
#
# Author:      berder
#
# Created:     04/08/16
# Copyright:   (c) Arsynet 2016
# Licence:     Tous droits réservés
# -------------------------------------------------------------------------------

from __future__ import division, print_function
from selenium import webdriver
from selenium.webdriver.support.ui import Select
import argparse
import time
import pandas as pd
import numpy as np
import os.path
import os
import sys
sys.path.append("../")
import logging
import optidb.optimode
from optidb.model import *
import csv
from utils import utcnow
from utils.logging_utils import BackupFileHandler

optidb.optimode.USE_NEW_AGGREGATE = True

__version__ = 'V1.0.1'

provider = 'Ireland'
provider_tag = 'query_providers.%s' % provider
tmp_dir = '/tmp/Ireland'
url = 'http://www.cso.ie/px/pxeirestat/statire/SelectVarVal/Define.asp?Maintable=CTM01&PLanguage=0'
unknown_airports = pd.DataFrame(columns=['code', 'pax'])
airports_codes = dict()


class External_Segment(Model):
    __collection__ = 'external_segment'


class External_Segment_Tmp(Model):
    __collection__ = 'external_segment_laurent_tests'


def generate_year_months(month, year):
    """
    Generates year_months in the format required byt the website (YYYY'M'MM)
    :param month: integer or list of integers
    :param year: integer or list of integers
    :return: list of strings
    """
    year_months = []
    if type(year) == list or type(year) == tuple:
        for y in year:
            if type(month) == list or type(month) == tuple:
                for m in month:
                   year_months.append(str(y) + 'M' + format(m, '02d'))
            else:
                year_months.append(str(y) + 'M' + format(month, '02d'))
    else:
        if type(month) == list or type(month) == tuple:
            for m in month:
                year_months.append(str(year) + 'M' + format(m, '02d'))
        else:
            year_months.append(str(year) + 'M' + format(month, '02d'))
    return year_months


def download_file(year_months):
    """
    Download data for the requested year_months.
    All information is within the same file.
    Airports are represented as iata_codes only.
    :param year_months: list of strings ['YYYY-MM', ...]
    :return: a single renamed csv file
    """
    end_name = "Ireland_Segments.csv"
    if end_name not in os.listdir(tmp_dir):

        # Transform 'YYYY-MM' to 'YYYY"M"MM'
        year_months_IRE = [ym.replace('-', 'M') for ym in year_months]

        # Set chrome options and reach the website
        options = webdriver.ChromeOptions()
        options.add_experimental_option("prefs", {
            "download.default_directory": tmp_dir,
            "download.prompt_for_download": False,
        })
        driver = webdriver.Chrome(chrome_options=options)
        driver.implicitly_wait(10)
        driver.get(url)
        assert "Passenger" in driver.title

        # Select the way in and way out
        select_ways = Select(driver.find_element_by_name('var1'))
        for label in ['Inward', 'Outward']:
            select_ways.select_by_visible_text(label)
        # Select all the foreign airports
        Select(driver.find_element_by_name('grouping2')).select_by_visible_text('Select all')
        # Select all the irish airports
        Select(driver.find_element_by_name('grouping3')).select_by_visible_text('Select all')
        # Deselect the default year_month, then select the requested year_months
        Select(driver.find_element_by_name('grouping4')).select_by_visible_text('Deselect all')
        select_year_months = Select(driver.find_element_by_name('var4'))
        for ym in year_months_IRE:
            select_year_months.select_by_visible_text(ym)
        # Go to next page
        driver.find_element_by_name('Forward').click()
        time.sleep(120)

        # Edit the options to get the table with only codes instead of a mix of text with codes inside
        Select(driver.find_element_by_name('pivot')).select_by_visible_text('Show only codes')
        time.sleep(30)
        # Download file
        driver.find_element_by_name('run').click()

        # Wait for file to be downloaded
        time.sleep(30)
        driver.close()

        # Identify the downloaded csv file name, and rename to "Ireland_Segments-year.csv"
        csv_name = max([tmp_dir + "/" + f for f in os.listdir(tmp_dir)], key=os.path.getctime)
        os.rename(os.path.join(tmp_dir, csv_name), os.path.join(tmp_dir, end_name))
        log.info("%s downloaded", end_name)
    return end_name


def get_airports_codes():
    """
    Get a dictionary of all airport codes for reference throughout import algorithm
    :return:
    """
    airports_codes = Airport.find({'code': {"$ne": None}},
                                  {'code': 1, 'iata_code': 1, 'name': 1, '_id': 0})
    return dict((i.code, i) for i in airports_codes if i.code)


def check_airport(airport, pax):
    global unknown_airports
    global airports_codes
    # Check the airport code exists in Mongo. If not, skip line
    if airport not in airports_codes:
        if airport in unknown_airports['code'].values:
            unknown_airports.loc[unknown_airports['code'] == airport, 'pax'] += pax
        else:
            info = pd.Series({'code': airport, 'pax': pax})
            unknown_airports = unknown_airports.append(info, ignore_index=True)
        return False
    else:
        return True


def update_routes(csv_file):
    """
    Save new records in External_Segment collection
    """
    global aiports_codes
    airports_codes = get_airports_codes()
    now = utcnow()

    def log_bulk(self):
        log.info('  store external_segment: %r', self.nresult)

    log.info('Updating db with contents of %s...', csv_file)
    xls = pd.read_csv(tmp_dir + '/' + csv_file, sep=',', skiprows=[0,1,2])
    new_columns = xls.columns.values
    new_columns[0] = 'irish_airport'
    new_columns[1] = 'way'
    new_columns[2] = 'other_airport'
    for i, col in enumerate(new_columns[4:len(new_columns)], 3):
        new_columns[i] = col.replace('M', '-')
    xls.columns = new_columns
    xls = xls.replace(' ', np.nan)
    available_year_months = new_columns[3:len(new_columns)].tolist()

    with External_Segment_Tmp.unordered_bulk(1000, execute_callback=log_bulk) as bulk:
        for row_index, row in xls.iterrows():
            if pd.notnull(row['irish_airport']):
                irish_airport = row['irish_airport']
            if pd.notnull(row['way']):
                way = row['way']
            if pd.isnull(row['other_airport']):
                continue
            else:
                other_airport = row['other_airport']
            if sum(row[available_year_months]) == 0:
                continue
            for ym in available_year_months:
                pax = row[ym]

                if way == 1:
                    airport_origin = irish_airport
                    airport_destination = other_airport
                else:
                    airport_origin = other_airport
                    airport_destination = irish_airport

                if not check_airport(airport_origin, pax):
                    continue
                if not check_airport(airport_destination, pax):
                    continue

                dic = dict(provider=provider,
                           data_type='airport',
                           airline=['*'],
                           airline_ref_code=['*'],
                           total_pax=pax,
                           origin=[airport_origin],
                           destination=[airport_destination],
                           year_month=[ym],
                           overlap=[],
                           raw_rec=dict(row), both_ways=False,
                           from_line=row_index, from_filename=csv_file, url=url)
                query = dict((k, dic[k]) for k in ('origin', 'destination', 'year_month', 'provider',
                                                   'data_type', 'airline'))
                bulk.find(query).upsert().update_one({'$set': dic, '$setOnInsert': dict(inserted=now)})
                if row_index % 1000 == 0:
                    print('{0:.3g}'.format(row_index / len(xls.index) * 100) + '%')
    log.info('stored: %r', bulk.nresult)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Load data from Ireland')
    parser.add_argument('year_months', type=str, nargs='+', help='Year_month(s) to download ([YYYY-MM, YYYY-MM...]')

    p = parser.parse_args()

    logging_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=logging_format)

    handler = BackupFileHandler(filename='load_Ireland.log', mode='w', backupCount=5)
    formatter = logging.Formatter(logging_format)
    handler.setFormatter(formatter)

    main_log = logging.getLogger()  # le root handler
    main_log.addHandler(handler)

    log = logging.getLogger('load_Ireland')

    log.info('load_files_from_Ireland - version %s - %r', __version__, p)
    year_months = p.year_months
    Model.init_db(def_w=True)
    csv_file = download_file(year_months)
    update_routes(csv_file)
    if len(unknown_airports) > 0:
        log.warning("%s unknown airports (check the reasons why): \n%s", len(unknown_airports.index), unknown_airports)
    log.info('End')

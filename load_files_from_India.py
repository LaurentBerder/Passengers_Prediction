 #-*- coding: utf-8 -*-

# -------------------------------------------------------------------------------
# Name:        Optimode / load_files_from_India
# Purpose:     Load data from files from Indian civil aviation
#
# Author:      berder
#
# Created:     10/04/2017
# Copyright:   (c) Arsynet 2015
# Licence:     Tous droits réservés
# -------------------------------------------------------------------------------

from __future__ import print_function
import argparse
import time
import datetime
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

locale.setlocale(locale.LC_TIME, "en_US.utf8") # Make sure the months are expressed in English
provider = {'international': 'India-intl', 'domestic': 'India-domestic'}
provider_tag = 'query_providers.%s' % provider
__version__ = 'V1.0.1'
unknown_airports = pd.DataFrame(columns=['city_name', 'passengers'])
no_capa = list()
tmp_dir = '/tmp/india'
#TODO: set url to be more generic
base_url = 'http://dgca.nic.in/pub/pub-ind.htm'
international_url = 'http://dgca.nic.in/pub/international/QUARTERLY%20PUBLICATION.htm'
full_url = 'http://dgca.nic.in/pub/month-stats/2016/DOM%20MONTHLY%20CITYPAIR%20DATA.htm' # To be refined (only 1 year here)


class External_Segment(Model):
    __collection__ = 'external_segment'


class External_Segment_Tmp(Model):
    __collection__ = 'external_segment_laurent_tests'


def get_month_domestic_file(year, month):
    month_name = datetime.date(1900, month, 1).strftime('%B')
    end_name = "India_domestic_%s-%s.xlsx" % (month, year)
    if end_name not in os.listdir(tmp_dir):
        # Set chrome options and reach the website
        options = webdriver.ChromeOptions()
        options.add_experimental_option("prefs", {
            "download.default_directory": tmp_dir,
            "download.prompt_for_download": False,
        })
        driver = webdriver.Chrome(chrome_options=options)
        driver.implicitly_wait(10)
        driver.get(base_url)

        # Select the demanded year for domestic files
        dom_link = 'href="month-stats/%s/DOM%20MONTHLY%20CITYPAIR%20DATA.htm"', year
        driver.find_element_by_xpath(
            '//a[@href="month-stats/%d/DOM%sMONTHLY%sCITYPAIR%sDATA.htm"]' % (year, "%20", "%20", "%20")).click()
        # Depending on the year, either click on the link that has the month_name in its address, or
        # Select the line in the table where the requested month is, and click on the second link (excel file)
        try:
            month_position = driver.find_element_by_xpath("//*[contains(text(), '%s')]" % month_name.upper())
        except NoSuchElementException:
            driver.find_element_by_xpath("//*[contains(text(), '%s')]/../following-sibling::td[2]" % month_name).click()
            log.info("File for year_month %d-%d not available", (year, month))

        # Click on the Excel file download link
        try:
            month_position.find_element_by_xpath("./a[2]").click()
        except Exception:
            pass

        time.sleep(5) # Wait until file has finished downloading
        # Identify latest downloaded excel file name, and rename to "India_domestic_month-year.xlsx"
        xlsx_name = max([tmp_dir + "/" + f for f in os.listdir(tmp_dir)], key=os.path.getctime)
        os.rename(os.path.join(tmp_dir, xlsx_name), os.path.join(tmp_dir, end_name))
        log.info("%s downloaded", end_name)

        driver.close()


def get_quarter_international_file(year, month):
    quarters = {0: "Q1", 1: "Q2", 2: "Q3", 3: "Q4"}
    # Files are quarterly, so find out which quarter this month is in
    quarter = (month-1)//3
    quarter_name = quarters.get(quarter)
    end_name = "India_international_%s-%s.xlsx" % (quarter_name, year)
    # Only download the Quarter's file once
    if end_name not in os.listdir(tmp_dir):

        # Set chrome options and reach the website
        options = webdriver.ChromeOptions()
        options.add_experimental_option("prefs", {
            "download.default_directory": tmp_dir,
            "download.prompt_for_download": False,
        })
        driver = webdriver.Chrome(chrome_options=options)
        driver.implicitly_wait(10)
        driver.get(base_url)

        # Click on the international section link
        driver.find_elements_by_xpath("//*[contains(text(), 'International Traffic')]")[0].click()

        try:
            driver.find_element_by_xpath("//a[contains(@href, '%s') and contains(@href, '%s')]" % (quarter_name, year))
        except:
            log.info("File for quarter %d of year %d not available", (quarter_name, year))
            driver.close()
            pass
        else:
            # Click on the right quarter's link
            driver.find_element_by_xpath("//a[contains(@href, '%s') and contains(@href, '%s')]" % (quarter_name, year)).click()
            # Download the city pairwise excel file (4.xlsx)
            driver.find_element_by_xpath("//a[contains(@href, '4.xlsx')]").click()
            time.sleep(5)  # Wait until file has finished downloading
            # Identify latest downloaded excel file name, and rename to "India_international_quarter-year.xlsx"
            xlsx_name = max([tmp_dir + "/" + f for f in os.listdir(tmp_dir)], key=os.path.getctime)
            os.rename(os.path.join(tmp_dir, xlsx_name), os.path.join(tmp_dir, end_name))
            log.info("%s downloaded", end_name)

            driver.close()


def download_month(year, month):
    if type(month) is list or type(month) is tuple:
        # If multiple months were given
        for m in month:
            ym = str(year) + '-' + '%02d' % m
            # Check if data already exists in database
            if External_Segment_Tmp.find_one({'year_month': ym, 'provider': provider}):
                log.warning("This year_month (%s) already exists for provider %s", ym, provider)
            get_month_domestic_file(year, m)
            get_quarter_international_file(year, m)
    else:
        ym = str(year) + '-' + '%02d' % month
        # Check if data already exists in database
        if External_Segment_Tmp.find_one({'year_month': ym, 'provider': provider}):
            log.warning("This year_month (%s) already exists for provider %s", ym, provider)
        get_month_domestic_file(year, int(month))
        get_quarter_international_file(year, int(month))


def download_files(year, month):
    log.info('Getting files on the web')
    if not os.path.isdir(tmp_dir):
        os.mkdir(tmp_dir)
    for y in year:
        download_month(y, month)
    xlsx_files = os.listdir(tmp_dir)
    return xlsx_files


def find_airports_by_name(name, perimeter):
    """
    This function looks up the name of an airport or city in the Excel file based on the Mexico-specific
    field of "provider_query", or on "query_names".
    Failures of this function are reported at the end of the algorithm to enrich (manually) the "provider_query" with
    the help of submit_query_providers() function.
    :param name: an upper case string
    :param tab_name: name of the excel file's tab (indication of international or mexican-only airports)
    :return: airport code (or None)
    """
    if perimeter == "domestic":
        city_clean = name.lower().replace('.', '').split('-')[0].split('/')[0].split(',')[0].strip()
        airports = pd.DataFrame.from_records(list(Airport.find({'query_names': city_clean, 'code_type': 'airport', 'country': 'IN'},
                                                  {'_id': 0, 'code': 1, 'name': 1, 'city': 1})))
        if airports.empty:
            airports = pd.DataFrame.from_records(list(Airport.find({provider_tag: name.strip(), 'code_type': 'airport', 'country': 'IN'},
                                                      {'_id': 0, 'code': 1, 'name': 1, 'city': 1})))
    else:
        city_clean = name.lower().replace('.', '').split('-')[0].split('/')[0].split(',')[0].strip()
        airports = pd.DataFrame.from_records(list(Airport.find({'query_names': city_clean, 'code_type': 'airport'},
                                                  {'_id': 0, 'code': 1, 'name': 1, 'city': 1})))
        if airports.empty:
            airports = pd.DataFrame.from_records(
                list(Airport.find({provider_tag: name.strip(), 'code_type': 'airport'},
                                  {'_id': 0, 'code': 1, 'name': 1, 'city': 1})))
    return set(airports['code']) if not airports.empty else None


def update_unknown_airports(city, pax_to, pax_from):
    global unknown_airports
    if city in unknown_airports['city_name'].values:
        unknown_airports.loc[unknown_airports['city_name'] == city, 'passengers'] += (pax_to + pax_from)
    # If airport is identified for the first time, save it in the dataframe
    else:
        info = pd.Series({'city_name': city, 'passengers': pax_to + pax_from})
        unknown_airports = unknown_airports.append(info, ignore_index=True)


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


def format_file(xls, perimeter):
    """
    Domestic and International files do not have the same format (though the required information is the same in both),
    so we harmonize them, and make sure all column names are the same.
    :param xls: a DataFrame
    :param perimeter: a string, either "domestic" or "international"
    :return: the same DataFrame, with a standardized format
    """
    if perimeter == "international":
        xls = xls.select(lambda x: not re.search('FREIGHT', x), axis=1)
        # Delete the last section of the file (flights with origin & destination outside India carried out by indian carriers)
        end = int((xls[xls.ix[:,0].str.contains("OUTSIDE") == True].index).values)
        xls = xls[xls.index < end]
    # Rename columns
    new_columns = xls.columns.values
    new_columns[0] = "ID"
    new_columns[[i for i, item in enumerate(new_columns) if "from" in item.lower()]] = "PAX FROM 2"
    new_columns[[i for i, item in enumerate(new_columns) if "to" in item.lower()]] = "PAX TO 2"
    new_columns[[i for i, item in enumerate(new_columns) if "city1" in item.lower().replace(" ", "")]] = "CITY 1"
    new_columns[[i for i, item in enumerate(new_columns) if "city2" in item.lower().replace(" ", "")]] = "CITY 2"
    xls.columns = new_columns

    return xls


def get_data(xlsx_files):
    """
    Populate the database with data extract in xlsx files. One file per year_month, only one tab per file.
    Back/Forth routes in rows, one column per way.
    :param xlsx_files: dict of file names
    :return:
    """
    global provider
    months = {"January": "01", "February": "02", "March": "03", "April": "04", "May": "05", "June": "06",
              "July": "07", "August": "08", "September": "09", "October": "10", "November": "11", "December": "12",
              "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04", "Jun": "06", "Jul": "07", "Aug": "08", "Sep": "09",
              "Sept": "09", "Oct": "10", "Nov": "11", "Dec": "12"}
    quarters = {"01": "Q1", "02": "Q1", "03": "Q1", "04": "Q2", "05": "Q2", "06": "Q2", "07": "Q3", "08": "Q3",
                "09": "Q3", "10": "Q4", "11": "Q4", "12": "Q4"}

    def log_bulk(self):
        log.info('  store external_segment: %r', self.nresult)

    for xlsx_f in xlsx_files:  # loop through each file
        if "domestic" in xlsx_f:
            perimeter = "domestic"
        else:
            perimeter = "international"
        provider_label = provider.get(perimeter)
        print('******************** processing Excel file:', xlsx_f)
        xl = pd.ExcelFile(tmp_dir + "/" + xlsx_f)
        xls = xl.parse()
        # Year_month based on the renamed file. List months of the quarter for the case of international files
        year = int(filter(str.isdigit, xlsx_f)[-4:])
        if perimeter == "domestic":
            month = '%02d' % int(xlsx_f.split('_')[2].split('-')[0])
            year_month = [str(year) + "-" + month]
        else:
            quarter = xlsx_f.split('_')[2].split('-')[0]
            year_month = [str(year) + '-' + k for k, v in quarters.items() if v == quarter]

        # Look for line with column names
        if perimeter == "domestic":
            header = np.where(xls.apply(lambda x: x.astype(str).str.upper().str.replace(" ", "")
                                        ).loc[:, :] == "CITY1")[0] + 1
        else:
            header = np.where(xls.apply(lambda x: x.astype(str).str.upper().str.replace(" ", "")
                                        ).loc[:, :] == "CITY1")[0][0] + 1
        xls = xl.parse(header=header)   # Re-load file with headers
        xls = format_file(xls, perimeter)

        all_rows = len(xls.index)

        with External_Segment_Tmp.unordered_bulk(1000, execute_callback=log_bulk) as bulk:
            for row in range(0, len(xls)):  # loop through each row (origin, destination) in file
                full_row = xls.iloc[row]
                # Stop at the end of the table (indicated by "TOTAL")
                if pd.isnull(full_row['CITY 1']) or full_row['CITY 1'] == "CITY 1":
                    continue
                if isinstance(full_row['ID'], str) and "".join(full_row['ID'].split(" ")).upper() == "TOTAL":
                    break
                # Skip empty rows (no text in Origin column, or year Total = 0)
                if isinstance(full_row['PAX TO 2'], float) and full_row['PAX FROM 2'] == 0:
                    continue
                airport1 = find_airports_by_name(unidecode(full_row['CITY 1']).upper(), perimeter)
                airport2 = find_airports_by_name(unidecode(full_row['CITY 2']).upper(), 'domestic')
                if airport1 is None:
                    update_unknown_airports(full_row['CITY 1'], full_row['PAX TO 2'], full_row['PAX FROM 2'])
                    continue
                if airport2 is None:
                    update_unknown_airports(full_row['CITY 2'], full_row['PAX TO 2'], full_row['PAX FROM 2'])
                    continue

                # First save data from city 1 to city 2
                dic_to = dict(provider=provider_label,
                              data_type='airport',
                              airline=['*'],
                              airline_ref_code=['*'],
                              origin=sorted(airport1),
                              destination=sorted(airport2),
                              year_month=year_month,
                              total_pax=int(full_row['PAX TO 2']),
                              overlap=[],
                              raw_rec=dict(full_row),
                              both_ways=False,
                              from_line=row,
                              from_filename=xlsx_f,
                              url=full_url)
                now = utcnow()
                query = dict((k, dic_to[k]) for k in ('origin', 'destination', 'year_month', 'provider',
                                                      'data_type', 'airline'))
                bulk.find(query).upsert().update_one({'$set': dic_to, '$setOnInsert': dict(inserted=now)})

                # Then save data from city 2 to city 1
                dic_from = dict(provider=provider_label,
                                data_type='airport',
                                airline=['*'],
                                airline_ref_code=['*'],
                                origin=sorted(airport2),
                                destination=sorted(airport1),
                                year_month=year_month,
                                total_pax=int(full_row['PAX FROM 2']),
                                overlap=[],
                                raw_rec=dict(full_row),
                                both_ways=False,
                                from_line=row,
                                from_filename=xlsx_f,
                                url=full_url)
                now = utcnow()
                query = dict((k, dic_from[k]) for k in ('origin', 'destination', 'year_month', 'provider',
                                                        'data_type', 'airline'))
                bulk.find(query).upsert().update_one({'$set': dic_from, '$setOnInsert': dict(inserted=now)})
                if row % 100 == 0:
                    print('{0:.3g}'.format(float(row) / float(all_rows) * 100) + '%')
        log.info('stored: %r', bulk.nresult)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Load data from India')
    parser.add_argument('year_months', type=str, nargs='+', help='Year_month(s) to download ([YYYY-MM, YYYY-MM...]')

    p = parser.parse_args()

    logging_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=logging_format)

    handler = BackupFileHandler(filename='load_India.log', mode='w', backupCount=5)
    formatter = logging.Formatter(logging_format)
    handler.setFormatter(formatter)

    main_log = logging.getLogger()  # le root handler
    main_log.addHandler(handler)

    log = logging.getLogger('load_India')

    log.info("Starting to update db with new file contents from India's government website - version %s - %r",
             __version__, p)

    start_time = time.time()
    Model.init_db(def_w=True)
    year = list(set([ym[0:4] for ym in p.year_months]))
    month = list(set([ym[5:7] for ym in p.year_months]))
    # submit_query_providers()   # update "provider_query" tags with previously unidentified airports
    xlsx_files = download_files(year, month)
    # xlsx_files = os.listdir(tmp_dir)
    get_data(xlsx_files)
    log.info("\n\n--- %s seconds to populate db with %d files---" % ((time.time() - start_time), len(xlsx_files)))
    global unknown_airports
    if len(unknown_airports.index) > 0:
        unknown_airports = unknown_airports.sort_values('passengers', ascending=False)
        log.warning("%s unknown airports (check the reasons why): \n%s", len(unknown_airports.index), unknown_airports)
    if len(no_capa) > 0:
        log.warning("%s international segments with no capacity (check the reasons why): ", len(no_capa), no_capa)
    log.info('End')

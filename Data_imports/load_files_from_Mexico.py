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
import time
import argparse
import pandas as pd
import numpy as np
from unidecode import unidecode
import logging
import logging.handlers
import os
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import sys
sys.path.append('../')
from optidb.model import *
from utils import utcnow
from utils.logging_utils import BackupFileHandler


provider = 'Mexico'
provider_tag = 'query_providers.%s' % provider
__version__ = 'V1.0.0'
no_capa = list()
tmp_dir = '/tmp/mexico'
if not os.path.isdir(tmp_dir):
     os.mkdir(tmp_dir)
base_url = 'http://www.sct.gob.mx/'
end_url = 'transporte-y-medicina-preventiva/aeronautica-civil/5-estadisticas/53-estadistica-operacional-de-aerolineas-traffic-statistics-by-airline/'



logging_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=logging_format)

handler = BackupFileHandler(filename='load_Mexico.log', mode='w', backupCount=5)
formatter = logging.Formatter(logging_format)
handler.setFormatter(formatter)

main_log = logging.getLogger()  # le root handler
main_log.addHandler(handler)

log = logging.getLogger('load_Mexico')


class External_Segment(Model):
    __collection__ = 'external_segment'


class External_Segment_Tmp(Model):
    __collection__ = 'external_segment_laurent_tests'


def download_single_file(year):
    year = str(year)
    end_name = "Mexico-%s.xlsx" % year
    if end_name not in os.listdir(tmp_dir):

        # Set chrome options and reach the website
        options = webdriver.ChromeOptions()
        options.add_experimental_option("prefs", {
            "download.default_directory": tmp_dir,
            "download.prompt_for_download": False,
        })
        driver = webdriver.Chrome(chrome_options=options)
        driver.implicitly_wait(10)
        driver.get(base_url + end_url)

        # Select the demanded year and month
        file_link = driver.find_element_by_xpath("//*[contains(text(), 'origen-destino')]")
        if year in file_link.get_attribute('href'):
            file_link.click()
        else:
            driver.find_element_by_xpath("//*[contains(text(), '1992')]").send_keys(Keys.CONTROL + Keys.SHIFT + Keys.ENTER)
            driver.switch_to.window(driver.window_handles[1])
            driver.find_element_by_xpath("//*[contains(text(), 'Monthly Traffic Statistics')]").click()
            driver.find_element_by_xpath(
                "//a[contains(text(), '%s') and contains(text(), 'destino')]" % year).click()

        time.sleep(10)  # Wait until file has finished downloading
        # Identify latest downloaded excel file name, and rename to "Mexico-year.xlsx"
        xlsx_name = max([tmp_dir + "/" + f for f in os.listdir(tmp_dir)], key=os.path.getctime)
        os.rename(os.path.join(tmp_dir, xlsx_name), os.path.join(tmp_dir, end_name))
        log.info("%s downloaded", end_name)

        driver.quit()
    return end_name


def download_files(year):
    log.info('Getting files on the web')
    xlsx_files = []
    for y in year:
        xlsx_files.append(download_single_file(y))
    return xlsx_files


def get_capa(year_month, origin, destination):
    """
    For international flights, select airports of origin/destination which have capacity between them for the selected
    year_month. If None, select all.
    :param year_month: 
    :param origin: list of airport codes 
    :param destination: list of airport codes
    :return: both lists of airport codes, filtered on existence of capacity
    """
    filtered_origin = set()
    filtered_destination = set()
    for o in origin:
        for d in destination:
            capa = CapacityInitialData.aggregate([
                {'$match': {'origin': o, 'destination': d, 'year_month': year_month, 'active_rec': True}},
                {'$group': {
                    '_id': {'origin': '$origin', 'destination': '$destination'}, 'capa': {'$sum': '$capacity'}
                }}
            ])
            cap = list(capa)
            if cap == []:
                continue
            else:
                filtered_destination.add(cap[0].get('_id').get('destination'))
                filtered_origin.add(cap[0].get('_id').get('origin'))

    if len(filtered_origin) > 0:
        if len(filtered_destination) > 0:
            return filtered_origin, filtered_destination
        else:
            return filtered_origin, None
    else:
        if len(filtered_destination) > 0:
            return None, filtered_destination
        else:
            return origin, destination


def find_airports_by_name(name, tab_name):
    """
    This function looks up the name of an airport or city in the Excel file based on the Mexico-specific
    field of "provider_query", or on "query_names".
    Failures of this function are reported at the end of the algorithm to enrich (manually) the "provider_query" with
    the help of submit_query_providers() function.
    :param name: an upper case string
    :param tab_name: name of the excel file's tab (indication of international or mexican-only airports)
    :return: set of airport codes (or None)
    """
    city_clean = name.lower().replace('.', '').split('-')[0].split('/')[0].split(',')[0].strip()
    if 'NAC' in tab_name:
        airports = dict((i.code, i) for i in
                        Airport.find({'query_names': city_clean, 'code_type': 'airport', 'country': 'MX'},
                                     {'_id': 0, 'code': 1, 'name': 1, 'city': 1}) if i.code)
        if len(airports) == 0:
            airports = dict((i.code, i) for i in
                            Airport.find({provider_tag: name.strip(), 'code_type': 'airport', 'country': 'MX'},
                                         {'_id': 0, 'code': 1, 'name': 1, 'city': 1}) if i.code)
    else:
        airports = dict((i.code, i) for i in
                        Airport.find({'query_names': city_clean, 'code_type': 'airport'},
                                     {'_id': 0, 'code': 1, 'name': 1, 'city': 1}) if i.code)
        if len(airports) == 0:
            airports = dict((i.code, i) for i in
                            Airport.find({provider_tag: name.strip(), 'code_type': 'airport'},
                                         {'_id': 0, 'code': 1, 'name': 1, 'city': 1}) if i.code)
    return set([a for a in airports.iterkeys()]) if len(airports) > 0 else None


def update_unknown_airports(city, pax):
    global unknown_airports
    if city in unknown_airports['city_name'].values:
        unknown_airports.loc[unknown_airports['city_name'] == city, 'passengers'] += pax
    # If airport is identified for the first time, save it in the dataframe
    else:
        info = pd.Series({'city_name': city, 'passengers': pax})
        unknown_airports = unknown_airports.append(info, ignore_index=True)


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


def submit_query_providers():
    """
    Save new query names identified from previous run's failures to improve future imports
    """
    print('Saving new airports codes...')
    airport_replacement = {'MSY': 'NUEVA ORLEANS', 'JFK': 'NUEVA YORK', 'NYP': 'NUEVA YORK',
                           'LGA': 'NUEVA YORK', 'FLU': 'NUEVA YORK',
                           'EWR': 'NUEVA YORK', 'WAW': 'VARSOVIA', 'WMI': 'VARSOVIA',
                           'SJD': 'SAN JOSE DEL CABO', 'HAV': 'LA HABANA',
                           'PTY': 'PANAMA', 'PCA': 'PORTAGE CREEK', 'SCL': "SANTIAGO DE CHILE", 'STI': 'SANTIAGO DE CHILE',
                           'SCU': 'SANTIAGO DE CHILE', 'LHR': 'LONDRES', 'LCY': 'LONDRES', 'LGW': 'LONDRES',
                           'LGW': 'LONDRES', 'STN': 'LONDRES', 'YXU': 'LONDRES', 'BQH': 'LONDRES', 'BZE': 'BELICE',
                           'SMP': 'ESTOCOLMO', 'VST': 'ESTOCOLMO', 'NYO': 'ESTOCOLMO', 'ARN': 'ESTOCOLMO', 'BMA': 'ESTOCOLMO'}
    with Airport.unordered_bulk() as bulk:
        for airport in airport_replacement:
            name = airport_replacement.get(airport)
            log.info('airport: %s', airport)
            bulk.find(dict(code=airport, code_type='airport')).upsert().update_one(
                {'$addToSet': {provider_tag: name}})
    log.info('load_airports_names: %r', bulk.nresult)


def get_data(xlsx_files, year_months):
    """
    Populate the database with data extract in xlsx files. 4 different tabs, for distinction of national/international
    and scheduled/charter flights. Routes in rows, months in columns.
    :param xlsx_files: dict of file names
    :return:
    """
    global provider
    now = utcnow()
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
            print('Starting', tab, 'tab in the Excel file')
            xls = xl.parse(tab)
            year = int(filter(str.isdigit, xlsx_f)) # Use the renamed file for the year
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
                    airport_origin = find_airports_by_name(origin, tab)
                    airport_destination = find_airports_by_name(destination, tab)
                    if airport_origin is None:
                        update_unknown_airports(origin, full_row['Total'])
                        continue
                    if airport_destination is None:
                        update_unknown_airports(destination, full_row['Total'])
                        continue

                    for col in range(2, len(xls.columns)-1):   # loop through rows (except for Origin, Dest, and Total)
                        # skip cells with no pax
                        if np.isnan(full_row[col]) or full_row[col] == "" or int(full_row[col]) == 0:
                            continue
                        year_month = str(year) + "-" + months.get(xls.columns[col])
                        total_pax = int(full_row[col])

                        # Only treat the requested year_months
                        if year_month not in year_months:
                            continue

                        if year_month not in previous_data['year_month'].values:
                            if External_Segment_Tmp.find_one({'year_month': year_month, 'provider': provider}):
                                log.warning("This year_month (%s) already exists for provider %s", year_month, provider)

                        # For international flights, only keep the airports for which capacity exists on that year_month
                        if 'INT' in tab:
                            airport_origin, airport_destination = get_capa(year_month, airport_origin, airport_destination)
                            if airport_destination is None or airport_origin is None:
                                no_capa.append({'year_month': year_month, 'origin': origin, 'destination': destination})
                                continue

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
                                   airline=['*'],
                                   airline_ref_code=['*'],
                                   origin=[', '.join(airport_origin)],
                                   destination=[', '.join(airport_destination)],
                                   year_month=[year_month],
                                   total_pax=total_pax,
                                   overlap=[],
                                   raw_rec=dict(full_row),
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

                        query = dict((k, dic[k]) for k in ('origin', 'destination', 'year_month', 'provider',
                                                           'data_type', 'airline'))
                        bulk.find(query).upsert().update_one({'$set': dic, '$setOnInsert': dict(inserted=now)})
            log.info('stored: %r', bulk.nresult)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Load data from Mexico')
    parser.add_argument('year_months', type=str, nargs='+', help='Year_month(s) to download ([YYYY-MM, YYYY-MM...]')

    p = parser.parse_args()

    logging_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=logging_format)

    handler = BackupFileHandler(filename='load_Mexico.log', mode='w', backupCount=5)
    formatter = logging.Formatter(logging_format)
    handler.setFormatter(formatter)

    main_log = logging.getLogger()  # le root handler
    main_log.addHandler(handler)

    log = logging.getLogger('load_Mexico')

    log.info("Starting to update db with new file contents from Mexico's government website - version %s - %r",
             __version__, p)

    start_time = time.time()

    Model.init_db(def_w=True)

    year_months = p.year_months[0].split(', ')
    year = list(set([ym[0:4] for ym in p.year_months]))
    unknown_airports = pd.DataFrame(columns=['city_name', 'passengers'])
    # submit_query_providers()   # update "provider_query" tags with previously unidentified airports
    xlsx_files = download_files(year)
    # xlsx_files = os.listdir(tmp_dir)

    get_data(xlsx_files, year_months)

    log.info("\n\n--- %s seconds to populate db with %d files---" % ((time.time() - start_time), len(xlsx_files)))
    log.info('%d unknown airports', len(unknown_airports))
    if len(unknown_airports.index) > 0:
        log.warning("%s unknown airports (check the reasons why): \n%s", len(unknown_airports.index), unknown_airports)
    if len(no_capa) > 0:
        log.warning("%s international segments with no capacity (check the reasons why): ", len(no_capa), no_capa)
    log.info('End')

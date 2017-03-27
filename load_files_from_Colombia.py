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
import time
from bs4 import BeautifulSoup
from urllib import urlopen, urlretrieve
from optidb.model import *
from utils import utcnow
import pandas as pd
import numpy as np
import re
sys.path.append('../')

provider = 'Colombia'
__version__ = 'V1.0.0'
AIRPORTS_CODES = dict()
AIRLINES_BY_ICAO = dict()
unknown_airports = set()
unknown_airlines = set()
tmp_dir = '/tmp/colombia'
full_url = 'http://www.aerocivil.gov.co/atencion/estadisticas-de-las-actividades-aeronauticas/Paginas/bases-de-datos.aspx'

logging.basicConfig(level=logging.DEBUG, format=0)
log = logging.getLogger('load_colombia')
log.setLevel(logging.DEBUG)

log.info('Updating db with new file contents from Aeronáutica Civil de Colombia website, version %s...', __version__)
nb_record_inserted = 0
nb_record_updated = 0


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
    airports_codes = Airport.find({'iata_code': {"$ne": None}},
                                  {'code': 1, 'iata_code': 1, 'icao_code': 1, 'country': 1, 'city': 1,
                                   'name': 1, '_id': 0})
    return dict((i.iata_code, i) for i in airports_codes if i.iata_code)


def get_airline_codes():
    """
    Get a dictionary of all airline codes for reference throughout import algorithm
    :return:
    """
    airlines = Company.find({'icao_code': {"$ne": None}},
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
    t = AIRLINES_BY_ICAO.get(airline_icao)
    if t is None:
        unknown_airlines.add(airline_icao + ':' + airline_name)
        return None
    return t['iata_code']


def download_files():
    """
    Get files from the web site
    The files are downloaded to 'tmp_dir' directory
    :return:
    """
    log.info('Getting files on the web')
    # response = urllib2.urlopen(full_url)
    # page = response.read()
    # soup = BeautifulSoup(page, 'html.parser')

    u = urlopen(full_url)
    try:
        html = u.read().decode('utf-8')
    finally:
        u.close()

    soup = BeautifulSoup(html, "html.parser")

    if not os.path.isdir(tmp_dir):
        os.mkdir(tmp_dir)

    # Select all A elements with href attributes containing URLs starting with http://
    for link in soup.select('a[href^="http://"]'):
        href = link.get('href')
        # Make sure it has one of the correct extensions
        if not any(href.endswith(x) for x in ['.xls', '.xlsx']):
            continue
        filename = tmp_dir + "/" + href.rsplit('/', 1)[-1]
        # Download Origin/Destination files later than 2001 not already downloaded
        if "Destino" in filename and int(re.findall('\d+', filename)[0]) > 2001 and not os.path.exists(filename):
            urlretrieve(href, filename)
            print('File', filename, 'downloaded')

    xlsx_files = os.listdir(tmp_dir)

    return xlsx_files


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
        row_nb = 0
        previous_data = pd.DataFrame(columns=['origin', 'destination', 'year_month', 'airline', 'passengers'])  # Create a dataframe to save data line after line, so we can check later on

        with External_Segment_Tmp.unordered_bulk(1000, execute_callback=log_bulk) as bulk:

            for row in range(0, len(xls)):    # loop through each row (origin, destination) in file
                row_nb += 1
                full_row = xls.iloc[row]
                if np.isnan(full_row['Passengers']) or full_row['Passengers'] == "" or int(full_row['Passengers']) == 0:  # skip rows with no pax
                    continue

                total_pax = int(full_row['Passengers'])
                row_airline = get_airline_by_icao(full_row['Airline'], full_row['Airline_Name'])
                if row_airline is None:
                    continue
                airport_origin = full_row['Origen']
                airport_destination = full_row['Destino']
                if airport_origin in airport_replacement:  # correct the wrong codes
                    airport_origin = airport_replacement.get(airport_origin)
                if airport_destination in airport_replacement:  # correct the wrong codes
                    airport_destination = airport_replacement.get(airport_destination)
                if airport_destination not in airport_codes:
                    unknown_airports.add(
                        airport_destination + ":" + str(full_row['Airport_Destination']) + ":" +
                        str(full_row['Pais Destino']))
                    continue
                if airport_origin not in airport_codes:
                    unknown_airports.add(
                        airport_origin + ":" + str(full_row['Airport_Origin']) + ":" + str(full_row['Pais Origen']))
                    continue
                year_month = full_row['Year_Month']

                if ((previous_data['origin'] == airport_origin) &
                        (previous_data['destination'] == airport_destination)
                        & (previous_data['year_month'] == year_month)
                        & (previous_data['airline'] == row_airline)).any():
                    new_row = False
                    # Add to Excel file's total_pax the "passenger" integer you get from filtering previous_data on other columns
                    total_pax += int(previous_data['passengers'][
                        (previous_data['origin'] == airport_origin) &
                        (previous_data['destination'] == airport_destination)
                        & (previous_data['year_month'] == year_month)
                        & (previous_data['airline'] == row_airline)])

                else:
                    new_row = True

                dic = dict(provider=provider,
                           data_type='airport',
                           airline=row_airline,
                           origin=airport_origin,
                           destination=airport_destination,
                           year_month=year_month,
                           total_pax=total_pax,
                           raw_rec=full_row.to_json(),
                           both_ways=False,
                           from_line=row_nb,
                           from_filename=xlsx_f,
                           url=full_url)

                new_data = pd.Series({'origin': airport_origin, 'destination': airport_destination,
                                      'year_month': year_month, 'airline': row_airline,
                                      'passengers': total_pax}).to_frame()
                if new_row:
                    previous_data = previous_data.append(new_data.T, ignore_index=True)
                else:
                    # Update the previous_data data frame with the new passengers count
                    previous_data['passengers'][
                        (previous_data['origin'] == airport_origin) &
                        (previous_data['destination'] == airport_destination) &
                        (previous_data['airline'] == row_airline) &
                        (previous_data['year_month'] == year_month)] = total_pax

                now = utcnow()
                query = dict((k, dic[k]) for k in ('origin', 'destination', 'year_month', 'provider',
                                                   'data_type', 'airline'))
                bulk.find(query).upsert().update_one({'$set': dic, '$setOnInsert': dict(inserted=now)})
                if row_nb % 1000 == 0:
                    print(row_nb / len(xls) * 100, "%")
        log.info('stored: %r', bulk.nresult)


def main():
    start_time = time.time()
    Model.init_db()
    #xlsx_files = download_files()
    xlsx_files = os.listdir(tmp_dir)
    AIRLINES_BY_ICAO.update(get_airline_codes())
    get_data(xlsx_files)
    log.info("\n\n--- %s seconds to populate db from Aeronautica Civil de Colombia---", (time.time() - start_time))
    if len(unknown_airports) > 0:
        print("\n\n", len(unknown_airports), "unknown airports (check the reasons why): ", unknown_airports)
    if len(unknown_airlines) > 0:
        print("\n\n", len(unknown_airlines), "unknown airlines (check the reasons why): ", unknown_airlines)


if __name__ == '__main__':
    main()

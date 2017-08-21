# -*- coding: utf-8 -*-

# -------------------------------------------------------------------------------
# Name:        Optimode / load_airport_data_from_wikidata
# Purpose:     Update missing data for airports with data found on wikidata.org
#
# Author:      berder
#
# Created:     06/03/2017
# Copyright:   (c) Arsynet 2015
# Licence:     Tous droits réservés
# -------------------------------------------------------------------------------

from __future__ import print_function
import sys
sys.path.append('../')
from optidb import Model
from optidb.model import *
import pandas as pd
import time
import requests
import unicodedata



logging.basicConfig(level=logging.DEBUG, format=0)
log = logging.getLogger('O&D_RandomForest')
log.setLevel(logging.DEBUG)


def open_db():
    Model.init_db()


class Tmp_airports(Model):
    __collection__ = 'airport_baptiste'


def get_wiki_data():
    print("Sending data query to Wikidata")
    query = """
    #List of all airports in the world
    #defaultView:Table

    SELECT DISTINCT ?airportLabel ?cityLabel ?_countrycode ?stateLabel
            ?iata_code ?icao_code ?faa_code ?coor ?altitude ?timezoneLabel
    WHERE
    {
      ?airport wdt:P31 wd:Q1248784.
      OPTIONAL {?airport wdt:P931 ?city. }
      OPTIONAL {?airport wdt:P238 ?iata_code. }
      OPTIONAL {?airport wdt:P239 ?icao_code. }
      OPTIONAL {?airport wdt:P240 ?faa_code. }
      OPTIONAL {?airport wdt:P17 ?country. }
      OPTIONAL {?country wdt:P297 ?_countrycode. }
      OPTIONAL {?airport wdt:P131 ?state. }
      OPTIONAL {?airport wdt:P625 ?coor. }
      OPTIONAL {?airport wdt:P2044 ?altitude. }
      OPTIONAL {?airport wdt:P421 ?timezone. }

      FILTER (?iata_code != "" || ?icao_code != "" || ?faa_code != "")


               SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
    }
    """

    url = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'

    tmp_airports = requests.get(url, params={'query': query, 'format': 'json'}).json().get('results').get('bindings')
    wiki_airports = pd.DataFrame(tmp_airports).fillna(value="missing value")
    for c in range(len(wiki_airports.columns)):
        for r in range(len(wiki_airports)):
            if wiki_airports.iloc[r][c] == "missing value":
                wiki_airports.iloc[r][c] = None
            else:
                wiki_airports.iloc[r][c] = unicodedata.normalize("NFKD",
                                                                 wiki_airports.iloc[r][c]['value']).encode('ascii',
                                                                                                           'ignore')
    wiki_airports.columns = ["country", "name", "altitude", "coordinates", "faa_code", "iata_code", "icao_code", "city",
                             "state", "timezone"]
    # Extract longitude latitude in the same format as our data
    wiki_airports['lonlat'] = wiki_airports['coordinates'].str.split("(").str[1].str.split(")").str[0].str.split(" ")
    # coords = wiki_airports['lonlat'].apply(pd.Series)
    # wiki_airports['latlon'] = coords[[1, 0]].values.tolist()

    wiki_airports = wiki_airports[['name', 'iata_code', 'icao_code', 'faa_code', 'city', 'country', 'state', 'timezone',
                                   'lonlat', 'date_of_opening', 'altitude', 'airport_url']]

    return wiki_airports


def get_opti_data():
    print('Getting data from Optimode')
    airport_cursor = Airport.find(
        {'code_type': 'airport', "discontinued_date":  9999},
        {'name': 1, 'city': 1, 'iata_code': 1, 'icao_code': 1, 'country': 1, 'state': 1, 'lonlat': 1, 'timezone': 1})
    opti_airports = pd.DataFrame(list(airport_cursor))
    opti_airports = opti_airports[['name', 'iata_code', 'icao_code', 'city', 'country', 'state', 'timezone', 'lonlat',
                                   '_id']]
    return opti_airports


def compare():
    match_by_name = wiki_airports.merge(opti_airports, on=['name'], how="inner", suffixes=('_w', '_o'))
    match_by_name = match_by_name[['name', 'iata_code_w', 'iata_code_o', 'icao_code_w', 'icao_code_o', 'city_w', 'city_o',
                                   'country_w', 'country_o', 'state_w', 'state_o', 'timezone_w', 'timezone_o',
                                   'lonlat_w', 'lonlat_o', 'faa_code', 'airport_url', 'date_of_opening',
                                   'elevation_above_sea_level', '_id']]
    match_by_name[match_by_name.iata_code_w != match_by_name.iata_code_o]
    match_by_name[match_by_name.lonlat_w != match_by_name.lonlat_o]



def main():
    log.info('Starting to get data')
    start_time = time.time()
    open_db()
    wiki_airports = get_wiki_data()
    opti_airports = get_opti_data()
    log.info("\n\n--- %s seconds to populate db with %d files---" % ((time.time() - start_time), len(xlsx_files)))

    log.info('End')

if __name__ == '__main__':
    main()

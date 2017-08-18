from __future__ import print_function
import sys
import pandas as pd
import numpy as np
sys.path.append('../')
from optidb import Model
from optidb.model import *


def open_db():
    Model.init_db()


def get_segments(start_date, end_date):
    """
    :param origin: IATA code typed in by user
    :param destination: IATA code typed in by user
    :param end_date: date, under format YYYY-MM.
    :return: a data frame with pax per monthly (multiple lines) one-way segments between origin and destination
    """
    segment_cursor = SegmentInitialData.find(
                {'year_month': {"$gte": start_date, "$lte": end_date}, 'record_ok': True},
                {'leg_origin': 1, 'leg_destination': 1, 'operating_airline': 1, 'year_month': 1, 'passengers': 1, '_id': 0})
    segment = pd.DataFrame(list(segment_cursor))
    cols = segment.columns.values
    cols[[i for i, item in enumerate(cols) if "origin" in item.lower()]] = "origin"
    cols[[i for i, item in enumerate(cols) if "destination" in item.lower()]] = "destination"
    segment.columns = cols
    segment = segment.groupby([segment.year_month, segment.operating_airline, segment.origin, segment.destination], as_index=False).sum()
    return segment

def get_capa(start_date, end_date):
    """
    :param origin: IATA code typed in by user
    :param destination: IATA code typed in by user
    :param end_date: date, under format YYYY-MM.
    :return: a data frame with pax per monthly (multiple lines) one-way segments between origin and destination
    """
    capa_cursor = CapacityInitialData.find(
                {'year_month': {"$gte": start_date, "$lte": end_date}, 'active_rec': True, 'record_ok': True},
                {'origin': 1, 'destination': 1, 'operating_airline': 1, 'year_month': 1, 'capacity': 1, '_id': 0})
    capa = pd.DataFrame(list(capa_cursor))
    capa = capa.groupby([capa.year_month, capa.operating_airline, capa.origin, capa.destination], as_index=False).sum()
    return capa

def calculate_load_factor(segment, capa):
    load_factor = segment.merge(capa, on=['year_month', 'operating_airline', 'origin', 'destination'])
    load_factor['load_factor'] = load_factor.passengers / load_factor.capacity * 100
    load_factor[load_factor.load_factor == np.inf].load_factor = 'no capacity'
    return load_factor


def main():
    open_db()
    start_date = "2016-06"
    end_date = "2016-09"

    segment = get_segments(start_date, end_date)
    capa = get_capa(start_date, end_date)

    load_factor = calculate_load_factor(segment, capa)
    load_factor.to_csv("/home/laurent/load_factor.csv", index=False)
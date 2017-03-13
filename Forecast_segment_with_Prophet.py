# -*- coding: utf-8 -*-

# -------------------------------------------------------------------------------
# Name:        Forecast_segment_with_Prophet
# Purpose:     Predict segment from past segments, taking capacity into account
#
# Author:      berder
#
# Created:     01/03/2017
# Copyright:   (c) Arsynet 2015
# Licence:     Tous droits réservés
# -------------------------------------------------------------------------------

from __future__ import print_function
import sys
sys.path.append('../')
from xxx import Model
from xxx.model import *
import pandas as pd
import numpy as np
from fbprophet import Prophet
from datetime import datetime


logging.basicConfig(level=logging.DEBUG, format=0)
log = logging.getLogger('ForecastSegmentsProphet')
log.setLevel(logging.DEBUG)


def open_db():
    Model.init_db()


class Results(Model):
    __collection__ = 'o_and_d_laurent_tests'


def get_segment(origin, destination, company, end_date):
    """
    :param origin: IATA code typed in by user
    :param destination: IATA code typed in by user
    :param company: optional, airline IATA code. If not provided, default = all
    :param end_date: date, under format YYYY-MM.
    :return: a data frame with pax per monthly one-way segments between origin and destination
    """

    if company:
        segment_cursor = SegmentInitialData.find(
            {'year_month': {"$gte": "2004-01", "$lte": end_date}, 'leg_origin': origin, 'leg_destination': destination,
             'operating_airline': company, 'record_ok': True},
            {'leg_origin': 1, 'leg_destination': 1, 'year_month': 1, 'passengers': 1})
    else:
        segment_cursor = SegmentInitialData.find(
            {'year_month': {"$gte": "2004-01", "$lte": end_date}, 'leg_origin': origin, 'leg_destination': destination,
             'record_ok': True},
            {'leg_origin': 1, 'leg_destination': 1, 'year_month': 1, 'passengers': 1})
    segment = pd.DataFrame(list(segment_cursor))
    return segment


def get_capa(origin, destination, company, end_date):
    """
    :param origin: IATA code typed in by user
    :param destination: IATA code typed in by user
    :param company: optional, airline IATA code. If not provided, default = all
    :param end_date: date, under format YYYY-MM. Optional: If not provided, default = 3 years from now
    :return: a data frame with capacities between origin and destination
    """

    if company:
        capa_cursor = CapacityInitialData.find(
            {'year_month': {"$lte": end_date, "$gte": "2002-01"}, 'origin': origin, 'destination': destination,
             'operating_airline': company, 'record_ok': True, 'active_rec': True},
            {'origin': 1, 'destination': 1, 'year_month': 1, 'capacity': 1})
    else:
        capa_cursor = CapacityInitialData.find(
            {'year_month': {"$lte": end_date, "$gte": "2002-01"}, 'origin': origin, 'destination': destination,
             'record_ok': True, 'active_rec': True},
            {'origin': 1, 'destination': 1, 'year_month': 1, 'capacity': 1})
    capa = pd.DataFrame(list(capa_cursor))
    return capa


def main():
    print("START")
    open_db()
    origin = raw_input("Type origin: ")
    destination = raw_input("Type destination: ")
    start = str(raw_input("Type start year_month (MM/YY): "))
    end = str(raw_input("Type end year_month (optional): "))
    company = raw_input("Type company (optional): ")

    start_date = "20"+start[3:6]+"-"+start[0:2]
    now = utcnow()
    if end:
        end_date = "20" + end[3:6] + "-" + end[0:2]
    else:
        end_date = str(now.year + 3) + "-" + str(now.month).zfill(2)
    # periods: number of months between now and end_date
    if int(end_date[0:4]) == now.year:
        periods = (datetime.strptime(end_date + '-01', '%Y-%m-%d').month - now.month)
    else:
        periods = (datetime.strptime(end_date+'-01', '%Y-%m-%d').year - now.year) * 12

    capa = get_capa(origin, destination, company, end_date)
    capa['ds'] = pd.to_datetime(capa['year_month']+"-01")
    capa = capa.groupby(capa.ds).sum()
    capa['ds'] = capa.index
    capa = capa[['ds', 'capacity']]
    capa.columns = ['ds', 'cap']
    segment = get_segment(origin, destination, company, end_date)
    segment['ds'] = pd.to_datetime(segment['year_month']+"-01")
    segment = segment.groupby(segment.ds).sum()
    segment['ds'] = segment.index
    segment = segment[['ds', 'passengers']]
    segment.columns = ['ds', 'y']

    # Train the model, then prepare dates to be predicted
    model = Prophet(mcmc_samples=200)
    model.fit(segment)
    future = model.make_future_dataframe(periods=periods, freq='MS')
    # Add capacities to the prediction
    future = future.merge(capa, 'left', on='ds')
    forecast = model.predict(future)
    # restrict data to after the start_date
    forecast = forecast.loc[forecast['ds'] > (start_date + "-01")]
    model.history = model.history.loc[model.history['ds'] > (start_date + "-01")]

    # Since there is no weekly data, remove the weekly columns to avoid getting an empty plot
    forecast = forecast[['ds', 'cap', 't', 'trend', 'seasonal_lower', 'seasonal_upper', 'trend_lower', 'trend_upper',
                         'yhat_lower', 'yhat_upper', 'yearly', 'yearly_lower', 'yearly_upper', 'seasonal', 'yhat']]
    model.plot(forecast).show()
    # The capacity doesn't look nice on the trend plot, so remove it before this plot
    del forecast['cap']
    model.plot_components(forecast).show()
    

if __name__ == '__main__':
    main()

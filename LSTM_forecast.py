# -*- coding: utf-8 -*-

# -------------------------------------------------------------------------------
# Name:        Optimode / LSTM_forecast
# Purpose:     Use past segments in coordination with economical data for long-term forecasts through Deep Learning
#
# Author:      berder
#
# Created:     11/04/2017
# Copyright:   (c) Arsynet 2015
# Licence:     Tous droits réservés
# -------------------------------------------------------------------------------

from __future__ import print_function
import sys
sys.path.append('../')
from optidb import Model
from optidb.model import *
import time
import pandas as pd
from matplotlib import pyplot
import urllib2
import numpy as np
from datetime import datetime
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error
from statsmodels.tsa.stattools import acf, pacf
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM



logging.basicConfig(level=logging.DEBUG, format=0)
log = logging.getLogger('LSTM_Forecast')
log.setLevel(logging.DEBUG)


def open_db():
    Model.init_db()


def get_segments(origin, destination, end_date):
    """
    :param origin: IATA code typed in by user
    :param destination: IATA code typed in by user
    :param end_date: date, under format YYYY-MM.
    :return: a data frame with pax per monthly (multiple lines) one-way segments between origin and destination
    """
    segment_cursor = SegmentInitialData.find(
                {'year_month': {"$gte": "2002-01", "$lte": end_date}, 'leg_origin': origin, 'leg_destination': destination,
                 'record_ok': True},
                {'leg_origin': 1, 'leg_destination': 1, 'year_month': 1, 'passengers': 1, 'segment_revenue_usd': 1})
    segment = pd.DataFrame(list(segment_cursor))
    return segment


def download_IMF():
    # Import IMF's data (downloaded https://www.imf.org/external/pubs/ft/weo/2016/01/weodata/weorept.aspx)
    # Decimal separator is ',', so we need to replace this by '.'
    url = 'https://www.imf.org/external/pubs/ft/weo/2016/01/weodata/weoreptc.aspx?sy=2002&ey=2021&ssd=1&sic=1&ssc=1&sort=subject&ds=%2C&br=1&pr1.x=76&pr1.y=9&c=512%2C672%2C914%2C946%2C612%2C137%2C614%2C546%2C311%2C962%2C213%2C674%2C911%2C676%2C193%2C548%2C122%2C556%2C912%2C678%2C313%2C181%2C419%2C867%2C513%2C682%2C316%2C684%2C913%2C273%2C124%2C868%2C339%2C921%2C638%2C948%2C514%2C943%2C218%2C686%2C963%2C688%2C616%2C518%2C223%2C728%2C516%2C558%2C918%2C138%2C748%2C196%2C618%2C278%2C624%2C692%2C522%2C694%2C622%2C142%2C156%2C449%2C626%2C564%2C628%2C565%2C228%2C283%2C924%2C853%2C233%2C288%2C632%2C293%2C636%2C566%2C634%2C964%2C238%2C182%2C662%2C359%2C960%2C453%2C423%2C968%2C935%2C922%2C128%2C714%2C611%2C862%2C321%2C135%2C243%2C716%2C248%2C456%2C469%2C722%2C253%2C942%2C642%2C718%2C643%2C724%2C939%2C576%2C644%2C936%2C819%2C961%2C172%2C813%2C132%2C199%2C646%2C733%2C648%2C184%2C915%2C524%2C134%2C361%2C652%2C362%2C174%2C364%2C328%2C732%2C258%2C366%2C656%2C734%2C654%2C144%2C336%2C146%2C263%2C463%2C268%2C528%2C532%2C923%2C944%2C738%2C176%2C578%2C534%2C537%2C536%2C742%2C429%2C866%2C433%2C369%2C178%2C744%2C436%2C186%2C136%2C925%2C343%2C869%2C158%2C746%2C439%2C926%2C916%2C466%2C664%2C112%2C826%2C111%2C542%2C298%2C967%2C927%2C443%2C846%2C917%2C299%2C544%2C582%2C941%2C474%2C446%2C754%2C666%2C698%2C668&s=PPPGDP%2CPPPPC%2CPCPI%2CLUR%2CLP&grp=0&a='
    response = urllib2.urlopen(url)
    import csv
    # Either read locally-downloaded file, or directly read file from webpage
    # reader = csv.reader(response, delimiter='\t')
    with open("/home/laurent/Documents/IMF_GDP-Inflation-Population-Unemployment.aspx", 'rb') as csv_file:
        reader = csv.reader(csv_file, delimiter=str(u'\t').encode('utf-8'))
        IMF = pd.DataFrame(list([[x.replace(str(',').encode('utf-8'),
                                            str('.').encode('utf-8')) for x in l] for l in reader]))
    IMF.columns = IMF.iloc[0]
    IMF = IMF.ix[1:len(IMF.index)-3]

    # Country codes in IMF's file are in ISO3 format
    # Need to replace with ISO2 format (found http://www.nationsonline.org/oneworld/country_code_list.htm)
    country_codes = {'AFG': 'AF', 'ALA': 'AX', 'ALB': 'AL',	'DZA': 'DZ', 'ASM': 'AS', 'AND': 'AD', 'AGO': 'AO', 'AIA': 'AI',
                     'ATA': 'AQ', 'ATG': 'AG', 'ARG': 'AR', 'ARM': 'AM', 'ABW': 'AW', 'AUS': 'AU', 'AUT': 'AT', 'AZE': 'AZ',
                     'BHS': 'BS', 'BHR': 'BH', 'BGD': 'BD', 'BRB': 'BB', 'BLR': 'BY', 'BEL': 'BE', 'BLZ': 'BZ', 'BEN': 'BJ',
                     'BMU': 'BM', 'BTN': 'BT', 'BOL': 'BO', 'BIH': 'BA', 'BWA': 'BW', 'BVT': 'BV', 'BRA': 'BR', 'VGB': 'VG',
                     'IOT': 'IO', 'BRN': 'BN', 'BGR': 'BG', 'BFA': 'BF', 'BDI': 'BI', 'KHM': 'KH', 'CMR': 'CM', 'CAN': 'CA',
                     'CPV': 'CV', 'CYM': 'KY', 'CAF': 'CF', 'TCD': 'TD', 'CHL': 'CL', 'CHN': 'CN', 'HKG': 'HK', 'MAC': 'MO',
                     'CXR': 'CX', 'CCK': 'CC', 'COL': 'CO', 'COM': 'KM', 'COG': 'CG', 'COD': 'CD', 'COK': 'CK', 'CRI': 'CR',
                     'CIV': 'CI', 'HRV': 'HR', 'CUB': 'CU', 'CYP': 'CY', 'CZE': 'CZ', 'DNK': 'DK', 'DJI': 'DJ', 'DMA': 'DM',
                     'DOM': 'DO', 'ECU': 'EC', 'EGY': 'EG', 'SLV': 'SV', 'GNQ': 'GQ', 'ERI': 'ER', 'EST': 'EE', 'ETH': 'ET',
                     'FLK': 'FK', 'FRO': 'FO', 'FJI': 'FJ', 'FIN': 'FI', 'FRA': 'FR', 'GUF': 'GF', 'PYF': 'PF', 'ATF': 'TF',
                     'GAB': 'GA', 'GMB': 'GM', 'GEO': 'GE', 'DEU': 'DE', 'GHA': 'GH', 'GIB': 'GI', 'GRC': 'GR', 'GRL': 'GL',
                     'GRD': 'GD', 'GLP': 'GP', 'GUM': 'GU', 'GTM': 'GT', 'GGY': 'GG', 'GIN': 'GN', 'GNB': 'GW', 'GUY': 'GY',
                     'HTI': 'HT', 'HMD': 'HM', 'VAT': 'VA', 'HND': 'HN', 'HUN': 'HU', 'ISL': 'IS', 'IND': 'IN', 'IDN': 'ID',
                     'IRN': 'IR', 'IRQ': 'IQ', 'IRL': 'IE', 'IMN': 'IM', 'ISR': 'IL', 'ITA': 'IT', 'JAM': 'JM', 'JPN': 'JP',
                     'JEY': 'JE', 'JOR': 'JO', 'KAZ': 'KZ', 'KEN': 'KE', 'KIR': 'KI', 'PRK': 'KP', 'KOR': 'KR', 'KWT': 'KW',
                     'KGZ': 'KG', 'LAO': 'LA', 'LVA': 'LV', 'LBN': 'LB', 'LSO': 'LS', 'LBR': 'LR', 'LBY': 'LY', 'LIE': 'LI',
                     'LTU': 'LT', 'LUX': 'LU', 'MKD': 'MK', 'MDG': 'MG', 'MWI': 'MW', 'MYS': 'MY', 'MDV': 'MV', 'MLI': 'ML',
                     'MLT': 'MT', 'MHL': 'MH', 'MTQ': 'MQ', 'MRT': 'MR', 'MUS': 'MU', 'MYT': 'YT', 'MEX': 'MX', 'FSM': 'FM',
                     'MDA': 'MD', 'MCO': 'MC', 'MNG': 'MN', 'MNE': 'ME', 'MSR': 'MS', 'MAR': 'MA', 'MOZ': 'MZ', 'MMR': 'MM',
                     'NAM': 'NA', 'NRU': 'NR', 'NPL': 'NP', 'NLD': 'NL', 'ANT': 'AN', 'NCL': 'NC', 'NZL': 'NZ', 'NIC': 'NI',
                     'NER': 'NE', 'NGA': 'NG', 'NIU': 'NU', 'NFK': 'NF', 'MNP': 'MP', 'NOR': 'NO', 'OMN': 'OM', 'PAK': 'PK',
                     'PLW': 'PW', 'PSE': 'PS', 'PAN': 'PA', 'PNG': 'PG', 'PRY': 'PY', 'PER': 'PE', 'PHL': 'PH', 'PCN': 'PN',
                     'POL': 'PL', 'PRT': 'PT', 'PRI': 'PR', 'QAT': 'QA', 'REU': 'RE', 'ROU': 'RO', 'RUS': 'RU', 'RWA': 'RW',
                     'BLM': 'BL', 'SHN': 'SH', 'KNA': 'KN', 'LCA': 'LC', 'MAF': 'MF', 'SPM': 'PM', 'VCT': 'VC', 'WSM': 'WS',
                     'SMR': 'SM', 'STP': 'ST', 'SAU': 'SA', 'SEN': 'SN', 'SRB': 'RS', 'SYC': 'SC', 'SLE': 'SL', 'SGP': 'SG',
                     'SVK': 'SK', 'SVN': 'SI', 'SLB': 'SB', 'SOM': 'SO', 'ZAF': 'ZA', 'SGS': 'GS', 'SSD': 'SS', 'ESP': 'ES',
                     'LKA': 'LK', 'SDN': 'SD', 'SUR': 'SR', 'SJM': 'SJ', 'SWZ': 'SZ', 'SWE': 'SE', 'CHE': 'CH', 'SYR': 'SY',
                     'TWN': 'TW', 'TJK': 'TJ', 'TZA': 'TZ', 'THA': 'TH', 'TLS': 'TL', 'TGO': 'TG', 'TKL': 'TK', 'TON': 'TO',
                     'TTO': 'TT', 'TUN': 'TN', 'TUR': 'TR', 'TKM': 'TM', 'TCA': 'TC', 'TUV': 'TV', 'UGA': 'UG', 'UKR': 'UA',
                     'ARE': 'AE', 'GBR': 'GB', 'USA': 'US', 'UMI': 'UM', 'URY': 'UY', 'UZB': 'UZ', 'VUT': 'VU', 'VEN': 'VE',
                     'UVK': 'XK', 'VNM': 'VN', 'VIR': 'VI', 'WLF': 'WF', 'ESH': 'EH', 'YEM': 'YE', 'ZMB': 'ZM', 'ZWE': 'ZW'}
    IMF = IMF.replace({'ISO': country_codes})

    # Certain values are missing (either empty column or 'n/a'), we need to replace them with 0
    IMF.replace({'2002': {'n/a': 0, '': 0}})
    IMF.replace({'2003': {'n/a': 0, '': 0}})
    IMF.replace({'2004': {'n/a': 0, '': 0}})
    IMF.replace({'2005': {'n/a': 0, '': 0}})
    IMF.replace({'2006': {'n/a': 0, '': 0}})
    IMF.replace({'2007': {'n/a': 0, '': 0}})
    IMF.replace({'2008': {'n/a': 0, '': 0}})
    IMF.replace({'2009': {'n/a': 0, '': 0}})
    IMF.replace({'2010': {'n/a': 0, '': 0}})
    IMF.replace({'2011': {'n/a': 0, '': 0}})
    IMF.replace({'2012': {'n/a': 0, '': 0}})
    IMF.replace({'2013': {'n/a': 0, '': 0}})
    IMF.replace({'2014': {'n/a': 0, '': 0}})
    IMF.replace({'2015': {'n/a': 0, '': 0}})
    IMF.replace({'2016': {'n/a': 0, '': 0}})
    IMF.replace({'2017': {'n/a': 0, '': 0}})
    IMF.replace({'2018': {'n/a': 0, '': 0}})
    IMF.replace({'2019': {'n/a': 0, '': 0}})
    IMF.replace({'2020': {'n/a': 0, '': 0}})
    IMF.replace({'2021': {'n/a': 0, '': 0}})

    return IMF


def airport_country(airport):
    airport_cursor = Airport.find(
        {'code': airport, 'code_type': 'airport'},
        {'_id': 0, 'code': 1, 'country': 1})
    return airport_cursor.distinct('country')


def isolate_IMF_data(IMF, country_origin, country_destination, name):
    origin_gdp = IMF.loc[IMF['ISO'] == country_origin].loc[IMF['WEO Subject Code'] == name].T
    origin_gdp.columns = origin_gdp.iloc[0]
    origin_gdp['year'] = origin_gdp.index
    origin_gdp = origin_gdp.iloc[6:len(origin_gdp.index) - 1, ]
    destination_gdp = IMF.loc[IMF['ISO'] == country_destination].loc[IMF['WEO Subject Code'] == name].T
    destination_gdp.columns = destination_gdp.iloc[0]
    destination_gdp['year'] = destination_gdp.index
    destination_gdp = destination_gdp.iloc[6:len(destination_gdp.index) - 1, ]
    return origin_gdp, destination_gdp


def withIMF(segments, origin_imf, destination_imf, country_origin, country_destination, name):
    """
    Adds one column to the segments data frame
    :param segments: data frame
    :param origin_imf: specific data from the IMF related to the country of origin, a data frame
    :param destination_imf: specific data from the IMF related to the country of destination, a data frame
    :param country_origin: ISO2 letters code
    :param country_destination: ISO2 letters code
    :param name: Name of the column to be added
    :return: a data frame with one line per year_month, with number of passengers between origin and destination, and
            an extra column with summed data concerning countries of origin and destination
    """
    if country_destination == country_origin:
        aggregated = pd.merge(segments, origin_imf, how='outer', on=['year'])
        aggregated['new'] = pd.to_numeric(aggregated[country_origin]) * 2
    else:
        aggregated = pd.merge(segments, origin_imf, how='outer', on=['year'])
        aggregated = pd.merge(aggregated, destination_imf, how='outer', on=['year'])
        aggregated['new'] = pd.to_numeric(aggregated[country_origin]) + pd.to_numeric(aggregated[country_destination])
        del aggregated[country_destination]
    del aggregated[country_origin]

    # # Rescaleing variables for consistent range of values
    # if name == "unemployment":
    #     aggregated['new'] *= 100
    # if name == "gdp_per_capita":
    #     aggregated['new'] /= 10
    # if name == "population" or name == "inflation":
    #     aggregated['new'] *= 10

    aggregated = aggregated.rename(columns={'new': name})

    return aggregated


def concatenate_segment(origin, destination, end_date):
    """
    Function adds to the segment the columns with data coming from the IMF
    :param origin: iata_code
    :param destination: iata_code
    :param end_date: YY/MM
    :return:
    """
    IMF = download_IMF()
    segments = get_segments(origin, destination, end_date)
    segments = segments.groupby(segments.year_month).sum()
    segments['year_month'] = segments.index
    segments['year'] = segments['year_month'].str.split('-').str[0]
    segments['average_price'] = segments.segment_revenue_usd / segments.passengers
    del segments['segment_revenue_usd']
    country_origin = airport_country(origin)[0]
    country_destination = airport_country(destination)[0]
    gdp_origin, gdp_destination = isolate_IMF_data(IMF, country_origin, country_destination, "PPPGDP")
    gdp_pc_origin, gdp_pc_destination = isolate_IMF_data(IMF, country_origin, country_destination, "PPPPC")
    inflation_origin, inflation_destination = isolate_IMF_data(IMF, country_origin, country_destination, "PCPI")
    unemployment_origin, unemployment_destination = isolate_IMF_data(IMF, country_origin, country_destination, "LUR")
    population_origin, population_destination = isolate_IMF_data(IMF, country_origin, country_destination, "LP")

    segments = withIMF(segments, gdp_origin, gdp_destination, country_origin, country_destination,
                       'gdp')
    segments = withIMF(segments, gdp_pc_origin, gdp_pc_destination, country_origin, country_destination,
                       'gdp_per_capita')
    segments = withIMF(segments, inflation_origin, inflation_destination, country_origin, country_destination,
                       'inflation')
    segments = withIMF(segments, unemployment_origin, unemployment_destination, country_origin, country_destination,
                       'unemployment')
    segments['unemployment'] = 1 / segments['unemployment'] * 100
    segments = withIMF(segments, population_origin, population_destination, country_origin, country_destination,
                       'population')

    segments = generate_future_dates(segments, end_date)

    return segments



def generate_future_dates(segments, end_date):
    # Last rows do not have a "year_month" since they're future years.
    # Generate rows for missing months of current year (if not complete), and future years:
    last_date = pd.to_datetime(segments[pd.notnull(segments['year_month'])].tail(1)['year_month']).iloc[0]
    if last_date.month == 12:
        future_segment = segments[pd.isnull(segments['year_month'])]
        future_segment = future_segment.append([future_segment] * 11, ignore_index=True).\
            sort_values(by='year', ascending=True).reset_index(drop=True)
    else:
        future_segment = segments[pd.isnull(segments['year_month'])]
        future_segment = future_segment.append([future_segment] * 11, ignore_index=True). \
            sort_values(by='year', ascending=True).reset_index(drop=True)
        future_segment = future_segment.append(segments[pd.notnull(segments['year_month'])].tail(1)).sort_values(by='year',
                                                                                                ascending=True)
        future_segment.set_value(future_segment.iloc[0].name, ['passengers', 'average_price'], np.nan)
        future_segment = future_segment.append(
            [future_segment.iloc[0]] * (11 - int(future_segment.iloc[0].year_month[-2:])), ignore_index=True).\
            sort_values(by='year', ascending=True).reset_index(drop=True)

    # Then fill in the "year_month" column:
    month = last_date.month + 1
    for i in range(0, len(future_segment.index)):
        future_segment.set_value(i, 'year_month',
                                 (future_segment.iloc[i,].year + '-' + "%02d" % month))
        if month<12:
            month +=1
        else:
            month = 1
    segments = segments.append(future_segment, ignore_index=True)
    # Finally, only keep the dates up to the end_date
    segments = segments[segments['year_month'] <= end_date]

    segments['Month'] = pd.to_datetime(segments['year_month'], format='%Y-%m')
    segments = segments.set_index('Month')
    del segments['year_month']
    del segments['year']
    return segments



# frame a sequence as a supervised learning problem
def timeseries_to_supervised(data, lag=1):
    df = pd.DataFrame(data)
    columns = [df.shift(i) for i in range(1, lag+1)]
    columns.append(df)
    df = pd.concat(columns, axis=1)
    df.fillna(0, inplace=True)
    return df

# create a differenced series
def difference(dataset, interval=1):
    diff = list()
    for i in range(interval, len(dataset)):
        value = dataset[i] - dataset[i - interval]
        diff.append(value)
    return pd.Series(diff)

# invert differenced value
def inverse_difference(history, yhat, interval=1):
    return yhat + history[-interval]

# scale train and test data to [-1, 1]
def scale(train, test):
    # fit scaler
    scaler = MinMaxScaler(feature_range=(-1, 1))
    scaler = scaler.fit(train)
    # transform train
    train = train.reshape(train.shape[0], train.shape[1])
    train_scaled = scaler.transform(train)
    # transform test
    test = test.reshape(test.shape[0], test.shape[1])
    test_scaled = scaler.transform(test)
    return scaler, train_scaled, test_scaled

# inverse scaling for a forecasted value
def invert_scale(scaler, X, value):
    new_row = [x for x in X] + [yhat]
    array = np.array(new_row)
    array = array.reshape(1, len(array))
    inverted = scaler.inverse_transform(array)
    return inverted[0, -1]

# fit an LSTM network to training data
def fit_lstm(train, batch_size, nb_epoch, neurons, layers):
    X, y = train[:, 0:-1], train[:, -1]
    X = X.reshape(X.shape[0], 1, X.shape[1])
    model = Sequential()
    if layers == 1:
        model.add(LSTM(neurons, activation='softsign', batch_input_shape=(batch_size, X.shape[1], X.shape[2]), stateful=True))
    if layers == 2:
        model.add(LSTM(neurons, activation='softsign', batch_input_shape=(batch_size, X.shape[1], X.shape[2]), stateful=True, return_sequences=True))
        model.add(LSTM(neurons, activation='softsign', batch_input_shape=(batch_size, X.shape[1], X.shape[2]), stateful=True))
    if layers == 3:
        model.add(LSTM(neurons, activation='softsign', batch_input_shape=(batch_size, X.shape[1], X.shape[2]), stateful=True, return_sequences=True))
        model.add(LSTM(neurons, activation='softsign', batch_input_shape=(batch_size, X.shape[1], X.shape[2]), stateful=True, return_sequences=True))
        model.add(LSTM(neurons, activation='softsign', batch_input_shape=(batch_size, X.shape[1], X.shape[2]), stateful=True))
    model.add(Dense(1))
    model.compile(loss='mean_squared_error', optimizer='adam')
    for i in range(nb_epoch):
        model.fit(X, y, epochs=1, batch_size=batch_size, verbose=0, shuffle=False)
        model.reset_states()
    return model

# make a one-step forecast
def forecast_lstm(model, batch_size, X):
    X = X.reshape(1, 1, len(X))
    yhat = model.predict(X, batch_size=batch_size)
    return yhat[0,0]


def run_experiments(model, train, test, scaler, raw_values, pred_length, repeat):
    rmses = list()
    for i in range(repeat):
        model.predict(train, batch_size=1)
        predictions = list()
        for i in range(len(test)):
            # make one-step forecast
            X, y = test[i, 0:-1], test[i, -1]
            yhat = forecast_lstm(model, 1, X)
            # invert scaling
            yhat = invert_scale(scaler, X, yhat)
            # invert differencing
            yhat = inverse_difference(raw_values, yhat, len(test) + 1 - i)
            # store forecast
            predictions.append(yhat)
            expected = raw_values[len(train) + i + 1]
            rmse = np.sqrt(mean_squared_error(raw_values[-pred_length:], predictions))
        rmses.append(rmse)
    return rmses



def main():
    open_db()
    origin = raw_input("Origin: ")
    destination = raw_input("Destination: ")
    end = raw_input("End Date (MM/YY): ")
    end_date = "20" + end[3:6] + "-" + end[0:2]
    segments = concatenate_segment(origin, destination, end_date)

    series = segments[pd.notnull(segments.passengers)].passengers
    # transform data to be stationary
    raw_values = series.values
    diff_values = difference(raw_values, 1)

    # transform data to be supervised learning
    supervised = timeseries_to_supervised(diff_values, 1)
    supervised_values = supervised.values

    # split data into train and test-sets
    pred_length = 12
    train, test = supervised_values[0:-pred_length], supervised_values[-pred_length:]

    # transform the scale of the data
    scaler, train_scaled, test_scaled = scale(train, test)
    train_reshaped = train_scaled[:, 0].reshape(len(train_scaled), 1, 1)

    # fit the model
    batch_size = 1
    nb_epoch = 500
    neurons = 2
    layers = 1
    start_time = time.time()
    lstm_model = fit_lstm(train_scaled, batch_size=batch_size, nb_epoch=nb_epoch, neurons=neurons, layers=layers)
    # if we want to experiment with the parameters, try the experiment a few times and study the results:
    repeat = 30
    rmses = run_experiments(lstm_model, train_reshaped, test_scaled, scaler, raw_values, pred_length, repeat)

    # forecast the entire training dataset to build up state for forecasting
    lstm_model.predict(train_reshaped, batch_size=1)
    end_time = time.time()

    # walk-forward validation on the test data
    predictions = list()
    for i in range(len(test_scaled)):
        # make one-step forecast
        X, y = test_scaled[i, 0:-1], test_scaled[i, -1]
        yhat = forecast_lstm(lstm_model, 1, X)
        # invert scaling
        yhat = invert_scale(scaler, X, yhat)
        # invert differencing
        yhat = inverse_difference(raw_values, yhat, len(test_scaled) + 1 - i)
        # store forecast
        predictions.append(yhat)
        expected = raw_values[len(train) + i + 1]
        print('Month=%d, Predicted=%f, Expected=%f' % (i + 1, yhat, expected))
    print("\n\n%f seconds to generate model and predictions\n\n" % (end_time - start_time))

    # report performance
    rmse = np.sqrt(mean_squared_error(raw_values[-pred_length:], predictions))
    print('Test RMSE: %.3f' % rmse)
    # plot data and prediction
    toplot = pd.DataFrame(index=series.index)
    toplot['historical'] = np.concatenate([raw_values[:len(raw_values)-pred_length], [np.nan] * pred_length])
    toplot['true_value'] = np.concatenate([[np.nan] * (len(raw_values)-pred_length), raw_values[-pred_length:]])
    toplot['prediction'] = np.concatenate([[np.nan] * (len(raw_values)-pred_length), predictions])
    toplot.plot()



if __name__ == '__main__':
    main()

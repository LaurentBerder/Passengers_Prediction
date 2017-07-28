# -*- coding: utf-8 -*-

# -------------------------------------------------------------------------------
# Name:        Optimode / Macroeco_linear_forecast
# Purpose:     Use past segments in coordination with economical data for long-term forecasts
#              Method used is:
#                           - multivariate linear regression
#                           - seasonal trend decomposition
#                           - addition of both elements
#
# Author:      berder
#
# Created:     08/03/2017
# Copyright:   (c) Arsynet 2015
# Licence:     Tous droits réservés
# -------------------------------------------------------------------------------

from __future__ import print_function
import sys
import urllib2
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.seasonal import seasonal_decompose
import numpy as np
from sklearn import linear_model
sys.path.append('../')
from optidb import Model
from optidb.model import *


logging.basicConfig(level=logging.DEBUG, format=0)
log = logging.getLogger('MacroecoLinearForecast')
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
                {'year_month': {"$gte": "2002-01", "$lte": end_date}, 'leg_origin': origin,
                 'leg_destination': destination, 'record_ok': True},
                {'leg_origin': 1, 'leg_destination': 1, 'year_month': 1, 'passengers': 1, 'segment_revenue_usd': 1})
    segment = pd.DataFrame(list(segment_cursor))
    return segment


def fill_in_missing_dates(df, date_col, fill_value=0):
    """
    Check if there are missing monthly data, and fill with zeros 
    :param df: a dataframe with monthly data
    :param date_col: name of the column in which the dates can be found
    :param fill_value: optional (default 0), what to replace missing data with
    :return: the same dataframe, but with added rows for missing year_months
    """
    df.index = pd.to_datetime(df[date_col], format='%Y-%m')
    end = max(df.index).date()
    start = min(df.index).date()
    idx = pd.date_range(start=start, end=end, freq='MS')
    df = df.loc[idx].fillna(fill_value)
    df['year_month'] = df.index.year.map(str) + "-" + df.index.month.map("{:02}".format)
    return df


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
    """
    Get the country code for each airport
    :param airport: airport code
    :return: country code
    """
    airport_cursor = Airport.find(
        {'code': airport, 'code_type': 'airport'},
        {'_id': 0, 'code': 1, 'country': 1})
    return airport_cursor.distinct('country')


def isolate_IMF_data(IMF, country_origin, country_destination, name):
    """
    Given 2 country codes and the reference of a measure, gets the measures of these countries  
    :param IMF: a dataframe with IMF macro-economic measures (past and forecasted)
    :param country_origin: country code
    :param country_destination: country code
    :param name: reference code of the measure to look-up in the IMF data
    :return: 2 dataframes with yearly measures
    """
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


def generate_future_dates(segments, end_date):
    # Last rows do not have a "year_month" since they're future years.
    # Generate rows for missing months of current year (if not complete), and future years:
    last_date = pd.to_datetime(segments[pd.notnull(segments['year_month'])].tail(1)['year_month']).iloc[0]
    if last_date.month == 12:
        future_segment = segments[pd.isnull(segments['year_month'])]
        future_segment = future_segment.append([future_segment] * 11, ignore_index=True).\
            sort_values(by='year', ascending=True).reset_index(drop=True)
    if last_date.month == 11:
        future_segment = segments[pd.isnull(segments['year_month'])]
        future_segment = future_segment.append([future_segment] * 11, ignore_index=True). \
            sort_values(by='year', ascending=True).reset_index(drop=True)
        future_segment = future_segment.append(segments[pd.notnull(segments['year_month'])].tail(1)).sort_values(
            by='year',
            ascending=True)
        future_segment.set_value(future_segment.iloc[0].name, ['passengers', 'average_price'], np.nan)
    else:
        future_segment = segments[pd.isnull(segments['year_month'])]
        future_segment = future_segment.append([future_segment] * 11, ignore_index=True). \
            sort_values(by='year', ascending=True).reset_index(drop=True)
        future_segment = future_segment.append(segments[pd.notnull(segments['year_month'])].tail(1)).\
            sort_values(by='year', ascending=True)
        future_segment.set_value(future_segment.iloc[0].name, ['passengers', 'average_price'], np.nan)
        future_segment = future_segment.append(
            [future_segment.iloc[0]] * (11 - int(future_segment.iloc[0].year_month[-2:])), ignore_index=True).\
            sort_values(by='year', ascending=True).reset_index(drop=True)

    # Then fill in the "year_month" column:
    month = last_date.month + 1
    for i in range(0, len(future_segment.index)):
        future_segment.set_value(i, 'year_month',
                                 (future_segment.iloc[i, ].year + '-' + "%02d" % month))
        if month < 12:
            month += 1
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


def concatenate_segment(origin, destination, end_date):
    """
    Function adds to the segment the columns with data coming from the IMF
    :param origin: iata_code
    :param destination: iata_code
    :param end_date: YY/MM
    :return: A dataframe with historical data of passengers and average ticket price, and historical+forecast data on
            various IMF macro-economic indicators
    """
    IMF = download_IMF()
    segments = get_segments(origin, destination, end_date)
    segments = segments.groupby(segments.year_month).sum()
    segments['year_month'] = segments.index
    segments = fill_in_missing_dates(segments, date_col='year_month')
    segments['year'] = segments.index.year.map(str)
    segments['average_price'] = segments.segment_revenue_usd / segments.passengers
    segments.set_value(pd.isnull(segments['average_price']), 'average_price', 0)
    del segments['segment_revenue_usd']
    first_year = min(segments['year'])
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
    segments = segments[segments['year'] >= first_year]
    segments = generate_future_dates(segments, end_date)
    return segments


def linear_regression(train, segments):
    """
    Use past segment data and IMF indicators (past & forecast) to generate a seasonal forecast over the next few years.
    :param train: dataframe with past data (segment + IMF)
    :param segments: dataframe containing IMF data for past and future years
    :return: 
    """
    # Check missing indicators and remove them
    indicators = ['gdp', 'gdp_per_capita', 'inflation', 'unemployment', 'population']
    for i in indicators:
        if pd.isnull(train[i][len(train) - 1]):
            indicators.remove(i)
    # Linear regression to predict passengers and average price from IMF indicators
    clf = linear_model.LinearRegression()
    clf.fit(train[indicators], train[['passengers', 'average_price']])
    prediction = pd.DataFrame(clf.predict(segments[indicators]), columns=['pax_regression', 'price_regression'])
    prediction.index = segments.index
    # Study seasonal trend of segment's passengers and average price
    pax_decomposition = seasonal_decompose(train['passengers'], freq=12)
    prediction['pax_seasonal'] = np.nan
    price_decomposition = seasonal_decompose(train['average_price'], freq=12)
    prediction['price_seasonal'] = np.nan
    pax_variation = np.random.normal(0, np.std(pax_decomposition.resid), len(segments)-len(train)) / 2
    price_variation = np.random.normal(0, np.std(price_decomposition.resid), len(segments) - len(train)) / 2
    pax_months = dict()
    price_months = dict()
    for i in range(1, 13):
        pax_months[i] = pax_decomposition.seasonal[pax_decomposition.seasonal.index.month == i][0]
        price_months[i] = price_decomposition.seasonal[pax_decomposition.seasonal.index.month == i][0]
    for j in range(len(prediction.index)):
        prediction['pax_seasonal'][j] = pax_months.get(prediction.index[j].month)
        prediction['price_seasonal'][j] = price_months.get(prediction.index[j].month)
    # Predict passengers by adding linear regression with seasonal trend (+/- random variation)
    prediction['pax_prediction'] = prediction.pax_regression + prediction.pax_seasonal
    prediction['pax_real_value'] = segments.passengers
    prediction['revenue_real_value'] = segments.passengers * segments.average_price
    prediction['pax_prediction'][pd.isnull(prediction.pax_real_value)] += pax_variation
    prediction.ix[prediction.pax_prediction < 0, 'pax_prediction'] = 0
    prediction['price_regression'][pd.isnull(prediction.pax_real_value)] += price_variation
    prediction['revenue_prediction'] = (prediction.price_regression + prediction.price_seasonal) * prediction['pax_prediction']
    return prediction


def plot_prediction(prediction, origin, destination):
    """
    Produces 2 subplots:
        - first one (on top) compares passengers forecasts with real values
        - second one (bottom) compares revenue forecasts with real values
    :param prediction: dataframe with linear regression results
    :param origin: airport code (for the title)
    :param destination: airport code (for the title)
    :return: 
    """
    maximum_pax = max(max(prediction.pax_prediction), max(prediction.pax_real_value))
    maximum_revenue = max(max(prediction.revenue_prediction), max(prediction.revenue_real_value))

    f, axes = plt.subplots(2, 1)
    plt.suptitle('%s - %s' % (origin, destination))
    axes[0].set_title("Passengers' monthly traffic")
    axes[0].set_ylim(0, maximum_pax + 2*maximum_pax / 100)
    axes[0].plot(prediction.index, prediction.pax_real_value, label="Passengers real values")
    axes[0].plot(prediction.index, prediction.pax_prediction, label="Passengers forecast", color='r')
    axes[0].set_ylabel('Passengers')
    axes[0].legend()
    axes[1].set_ylim(0, maximum_revenue + 2*maximum_revenue / 100)
    axes[1].plot(prediction.index, prediction.revenue_real_value, label="Revenue real values")
    axes[1].plot(prediction.index, prediction.revenue_prediction, label="Revenue forecasts", color='r')
    axes[1].set_ylabel('Revenue')
    axes[1].set_title("Monthly revenue (USD)")
    axes[1].legend()
    plt.show()


def main():
    open_db()
    origin = raw_input("Origin: ")
    destination = raw_input("Destination: ")
    end = raw_input("End Date (MM/YY): ")
    end_date = "20" + end[3:6] + "-" + end[0:2]
    segments = concatenate_segment(origin, destination, end_date)
    train = segments[pd.notnull(segments.passengers)]
    test = segments[pd.isnull(segments.passengers)]
    del test['passengers'], test['average_price']

    prediction = linear_regression(np.log(train), np.log(segments))
    plot_prediction(np.exp(prediction), origin, destination)


if __name__ == '__main__':
    main()

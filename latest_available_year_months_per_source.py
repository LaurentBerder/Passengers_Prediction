 #-*- coding: utf-8 -*-

# -------------------------------------------------------------------------------
# Name:        Optimode / treat_sources_scope
# Purpose:     Look at all the external sources' websites and check for which year_month their latest available data is.
#
# Author:      berder
#
# Created:     28/07/2017
# Copyright:   (c) Arsynet 2015
# Licence:     Tous droits réservés
# -------------------------------------------------------------------------------

import os
import argparse
import time
import logging
import logging.handlers
import urllib
import urllib2
import traceback
from bs4 import BeautifulSoup
from selenium import webdriver
import locale
import pandas as pd
from datetime import datetime
import re
import sys
sys.path.append('../')
from optidb.model import *
from utils.logging_utils import BackupFileHandler
from utils import YearMonth

__version__ = 'V1.0.1'


class External_Segment_Tmp(Model):
    __collection__ = 'external_segment_laurent_tests'


class Provider(Model):
    __collection__ = 'provider'


def update_latest_available_dates(providers):
    english_months = {'january': '01', 'february': '02', 'march': '03', 'april': '04', 'may': '05', 'june': '06',
                      'july': '07', 'august': '08', 'september': '09', 'october': '10', 'november': '11',
                      'december': '12', 'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04', 'jun': '06', 'jul': '07',
                      'aug': '08', 'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'}
    spanish_months = {'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04', 'mayo': '05', 'junio': '06',
                      'julio': '07', 'agosto': '08', 'septiembre': '09', 'octubre': '10', 'noviembre': '11',
                      'diciembre': '12', 'ene': '01', 'feb': '02', 'mar': '03', 'abr': '04', 'may': '05', 'jun': '06',
                      'jul': '07', 'ago': '08', 'sep': '09', 'oct': '10', 'nov': '11', 'dic': '12'}
    portuguese_months = {'janeiro': '01', 'fevereiro': '02', 'marco': '03', u'março': '03', 'abril': '04', 'maio': '05',
                         'junho': '06', 'julho': '07', 'agosto': '08', 'setembro': '09', 'outubro': '10',
                         'novembro': '11', 'dezembro': '12'}

    if 'USA' in providers:
        # USA:
        provider = 'USA'
        log.info('Finding latest available year_month for %s' % provider)
        try:
            page = urllib.urlopen("http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=293").read()
            string = 'Latest Available Data: '
            position = page.find(string)
            date_text = page[position + len(string): position + len(string) + 20].split('<')[0]
            # Make sure the months are expressed in English
            locale.setlocale(locale.LC_TIME, "en_US.utf8")
            date = datetime.strptime(date_text, '%B %Y')
            max_year_month = str(date.year) + '-' + format(date.month, '02d')
            Provider.update(query={'provider': provider}, update={'$set': {'latest_ym_available': max_year_month}})
        except Exception:
            log.exception('URL request failed for %s:', provider)

    if 'Colombia' in providers:
        # Colombia:
        provider = 'Colombia'
        log.info('Finding latest available year_month for %s' % provider)
        try:
            page = urllib.urlopen("http://www.aerocivil.gov.co/atencion/estadisticas-de-las-actividades-aeronauticas/Paginas/bases-de-datos.aspx").read()
            string = '.xlsx'
            position = page.find(string)
            date_text = page[position - 20: position].split('- ')[1]
            max_year_month = date_text.split(' ')[1] + '-' + spanish_months.get(date_text.split(' ')[0].lower())
            Provider.update(query={'provider': provider}, update={'$set': {'latest_ym_available': max_year_month}})
        except Exception:
            log.exception('URL request failed for %s:', provider)

    if 'Brazil' in providers:
        # Brazil:
        provider = 'Brazil'
        log.info('Finding latest available year_month for %s' % provider)
        try:
            page = urllib.urlopen('http://www.anac.gov.br/assuntos/dados-e-estatisticas/dados-estatisticos/dados-estatisticos').read()
            string = 'Dados disponíveis até '
            position = page.find(string)
            date_text = page[position + len(string): position + len(string)+ 20].split('.')[0]
            max_year_month = date_text.split('/')[1] + '-' + portuguese_months.get(date_text.split('/')[0].lower())
            Provider.update(query={'provider': provider}, update={'$set': {'latest_ym_available': max_year_month}})
        except Exception:
            log.exception('URL request failed for %s:', provider)

    if 'Mexico' in providers:
        # Mexico:
        provider = 'Mexico'
        log.info('Finding latest available year_month for %s' % provider)
        try:
            page = urllib.urlopen('http://www.sct.gob.mx/transporte-y-medicina-preventiva/aeronautica-civil/5-estadisticas/53-estadistica-operacional-de-aerolineas-traffic-statistics-by-airline/').read()
            string = '.xlsx'
            position = page.find(string)
            date_text = ' '.join(page[position - 27: position].split('-')[1:3])
            max_year_month = date_text.split(' ')[1] + '-' + spanish_months.get(date_text.split(' ')[0].lower())
            Provider.update(query={'provider': provider}, update={'$set': {'latest_ym_available': max_year_month}})
        except Exception:
            log.exception('URL request failed for %s:', provider)

    if len([s for s in providers if 'Eurostat' in s]) != 0:
        # Eurostat:
        provider = 'Eurostat'
        url_template = 'http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?xl_file=data/'
        base_filename_list = {'be': 'avia_par_be.tsv.gz', 'bg': 'avia_par_bg.tsv.gz', 'cz': 'avia_par_cz.tsv.gz',
                              'dk': 'avia_par_dk.tsv.gz', 'de': 'avia_par_de.tsv.gz', 'ee': 'avia_par_ee.tsv.gz',
                              'ie': 'avia_par_ie.tsv.gz', 'el': 'avia_par_el.tsv.gz', 'es': 'avia_par_es.tsv.gz',
                              'fr': 'avia_par_fr.tsv.gz', 'hr': 'avia_par_hr.tsv.gz', 'it': 'avia_par_it.tsv.gz',
                              'cy': 'avia_par_cy.tsv.gz', 'lv': 'avia_par_lv.tsv.gz', 'lt': 'avia_par_lt.tsv.gz',
                              'lu': 'avia_par_lu.tsv.gz', 'hu': 'avia_par_hu.tsv.gz', 'mt': 'avia_par_mt.tsv.gz',
                              'nl': 'avia_par_nl.tsv.gz', 'at': 'avia_par_at.tsv.gz', 'pl': 'avia_par_pl.tsv.gz',
                              'pt': 'avia_par_pt.tsv.gz', 'ro': 'avia_par_ro.tsv.gz', 'si': 'avia_par_si.tsv.gz',
                              'sk': 'avia_par_sk.tsv.gz', 'fi': 'avia_par_fi.tsv.gz', 'se': 'avia_par_se.tsv.gz',
                              'uk': 'avia_par_uk.tsv.gz', 'is': 'avia_par_is.tsv.gz', 'no': 'avia_par_no.tsv.gz',
                              'ch': 'avia_par_ch.tsv.gz'}
        for filename in base_filename_list:
            provider_country = provider + '-' + filename
            log.info('Finding latest available year_month for %s' % provider_country)
            df = pd.read_csv(url_template + base_filename_list.get(filename), compression='gzip', sep='\t', nrows=1)
            cols_to_keep = [x for x in df.columns if 'M' in x]
            max_year_month = max(cols_to_keep).strip().replace('M','-')
            Provider.update(query={'provider': provider_country}, update={'$set': {'latest_ym_available': max_year_month}})


    if 'Ireland' in providers:
        # Ireland:
        provider = 'Ireland'
        log.info('Finding latest available year_month for %s' % provider)
        try:
            page = urllib.urlopen('http://www.cso.ie/px/pxeirestat/statire/SelectVarVal/Define.asp?Maintable=CTM01&PLanguage=0').read()
            months = BeautifulSoup(page, "lxml").find_all('select',{'name':'var4'}, 'value')
            for month in months:
                M_list = (month.text.strip().split('\n'))
            max_year_month = max(M_list).replace('M', '-')
            Provider.update(query={'provider': provider}, update={'$set': {'latest_ym_available': max_year_month}})
        except Exception:
            log.exception('URL request failed for %s:', provider)

    if 'UK' in providers:
        # UK:
        provider = 'UK'
        log.info('Finding latest available year_month for %s' % provider)
        try:
            page = urllib.urlopen('https://www.caa.co.uk/Data-and-analysis/UK-aviation-market/Airports/Datasets/UK-Airport-data/').read()
            soup = BeautifulSoup(page, 'lxml')
            yms = []
            for lis in soup.findAll('li'):
                for a in lis.findAll('a'):
                    href = a.get('href')
                    # Get all the numbers in the link (for the month and the year)
                    ym = [int(s) for s in href.strip('/').split('-') if s.isdigit()]
                    if len(ym) == 2:
                        yms.append(str(ym[0]) + '-' + format(ym[1], '02d'))
            max_year_month = max(yms)
            Provider.update(query={'provider': provider}, update={'$set': {'latest_ym_available': max_year_month}})
        except Exception:
            log.exception('URL request failed for %s:', provider)

    if 'India - domestic' in providers:
        # India - domestic:
        provider = 'India - domestic'
        log.info('Finding latest available year_month for %s' % provider)
        url = 'http://dgca.nic.in/pub/pub-ind.htm'
        # Set chrome options and reach the website
        options = webdriver.ChromeOptions()
        driver = webdriver.Chrome(chrome_options=options)
        driver.implicitly_wait(10)
        driver.get(url)
        driver.find_element_by_css_selector("a[href*=%s]" % 'CITYPAIR').click()
        yms = []
        for ym in driver.find_elements_by_link_text('Click'):
            if 'xls' not in ym.get_attribute('href'):
                continue
            yms.append(ym.get_attribute('href').split('.xls')[0].split('%20')[-2:])
        ym_list = []
        for ym in yms:
            ym_list.append(ym[1].encode() + '-' + english_months.get(ym[0].encode().translate(None, ' .,/').lower(), '00'))
        driver.close()
        max_year_month = max(ym_list)
        Provider.update(query={'provider': provider}, update={'$set': {'latest_ym_available': max_year_month}})


    if 'India - intl' in providers:
        # India - intl:
        provider = 'India - intl'
        log.info('Finding latest available year_month for %s' % provider)
        url = 'http://dgca.nic.in/pub/pub-ind.htm'
        # Set chrome options and reach the website
        options = webdriver.ChromeOptions()
        driver = webdriver.Chrome(chrome_options=options)
        driver.implicitly_wait(10)
        driver.get(url)
        # Click on the international section link
        driver.find_elements_by_xpath("//*[contains(text(), 'International Traffic')]")[0].click()
        # Find all quarters
        yms = []
        for link in driver.find_elements_by_partial_link_text(''):
            yms.append(str(link.get_attribute('href').split('%20')[-1].split('.')[0]) + '-' +
                         english_months.get(link.text.split('-')[-1].lower()))
        driver.close()
        max_year_month = max(yms)
        Provider.update(query={'provider': provider}, update={'$set': {'latest_ym_available': max_year_month}})


    if 'Australia - domestic' in providers:
        # Australia - domestic:
        provider = 'Australia - domestic'
        log.info('Finding latest available year_month for %s' % provider)
        base_url = 'https://bitre.gov.au/'
        try:
            page = urllib.urlopen(base_url + 'publications/ongoing/domestic_airline_activity-time_series.aspx').read()
            string = 'Domestic_aviation_activity_TopRoutes'
            position = page.find(string)
            date_text = page[position + len(string): position + len(string) + 20].split('2004')[1].split('.xls')[0]
            # Ensure locale is English
            locale.setlocale(locale.LC_TIME, "en_US.utf8")
            max_year_month = str(datetime.strptime(date_text, '%B%Y').year) + '-' +\
                         format(datetime.strptime(date_text, '%B%Y').month, '02d')
            Provider.update(query={'provider': provider}, update={'$set': {'latest_ym_available': max_year_month}})
        except Exception:
            log.exception('URL request failed for %s:', provider)


    if 'Australia - intl' in providers:
        # Australia - intl:
        provider = 'Australia - intl'
        log.info('Finding latest available year_month for %s' % provider)
        base_url = 'https://bitre.gov.au/'
        try:
            page = urllib.urlopen(base_url + 'publications/ongoing/international_airline_activity-time_series.aspx').read()
            soup = BeautifulSoup(page, 'lxml')
            for a in soup.find_all('a'):
                if 'International_airline_activity_CityPairs' not in a.get('href') or 'xls' not in a.get('href'):
                    continue
                else:
                    xl = pd.read_excel(base_url + a.get('href'), sheetname='Data')
            max_year_month = str(max(xl.Month).year) + '-' + format(max(xl.Month).month, '02d')
            Provider.update(query={'provider': provider}, update={'$set': {'latest_ym_available': max_year_month}})
        except Exception:
            log.exception('URL request failed for %s:', provider)

    if 'Chile' in providers:
        # Chile:
        provider = 'Chile'
        log.info('Finding latest available year_month for %s' % provider)
        base_url = 'http://www.jac.gob.cl'
        string = 'Trafico-de-Par-de-ciudades-por-Operador'
        try:
            page = urllib.urlopen(base_url + '/estadisticas/estadisticas-historicas').read()
            soup = BeautifulSoup(page, 'lxml')
            yms = {'Chile - intl': None, 'Chile - domestic': None}
            for a in soup.find_all('a'):
                if not a.get('href') or string not in a.get('href') or 'xls' not in a.get('href'):
                    continue
                else:
                    year = [int(s) for s in a.get('href').split('-')[1].split('/') if s.isdigit()][2]
                    xl_file = pd.ExcelFile(base_url + a.get('href'))
                    if 'inter' in str(xl_file.sheet_names).lower():
                        provider_full = provider + ' - intl'
                    else:
                        provider_full = provider + ' - domestic'
                    xl = xl_file.parse(xl_file.sheet_names[0], header=5)
                    totals = xl.apply(pd.to_numeric, errors='coerce').sum()
                    month = None
                    for col, total in totals.iteritems():
                        if 'total' in col.lower() or pd.isnull(total) or total == 0:
                            continue
                        else:
                            month = spanish_months.get(col.lower())
                    yms[provider_full] = max(yms.get(provider_full), str(year) + '-' + month)
            if yms['Chile - intl'] == yms['Chile - domestic']:
                Provider.update(query={'provider': provider}, update={'$set': {'latest_ym_available': yms['Chile - domestic']}})
            else:
                print('Domestic and International files do not have the same dates available')
        except Exception:
            log.exception('URL request failed for %s:', provider)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Updating "latest year_month" for each external data provider')
    parser.add_argument('--to_process', type=bool, default=True,
                        help='Restrict to the list of providers that are processed')

    p = parser.parse_args()

    logging_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=logging_format)
    handler = BackupFileHandler(filename='latest_available_ym_per_source.log', mode='w', backupCount=20)
    formatter = logging.Formatter(logging_format)
    handler.setFormatter(formatter)
    main_log = logging.getLogger()  # le root handler
    main_log.addHandler(handler)
    log = logging.getLogger('latest_available_ym_per_source')
    log.info('Updating "latest year_month" for each external data provider, version %s - %r', __version__, p)

    Model.init_db(def_w=True)
    if p.to_process:
        providers = [prov.provider for prov in Provider.find({'import_process': True})]
    else:
        providers = [prov.provider for prov in Provider.find({})]

    update_latest_available_dates(providers)

    latest_available = list(Provider.find({'provider': {'$in': providers}}, {'_id': 0, 'provider': 1, 'latest_ym_available':1}))

    latest_downloaded = list(External_Segment_Tmp.aggregate([{'$match': {'provider': {'$in': providers}}},
                                                        {'$unwind': "$year_month"},
                                                        {'$group': {'_id': "$provider",
                                                                    'latest_downloaded': {'$max': "$year_month"}}}
                                                        ]))
    log.info('\n\n\n')
    for prov in latest_downloaded:
        if [item for item in latest_available if item.get("provider") == prov['_id']][0].get('latest_ym_available') == prov['latest_downloaded']:
            log.info('Provider: , OK (%s)' % (prov['_id', prov['latest_downloaded']]))
        else:
            log.info('Provider: %s, latest_downloaded: %s, latest_available: %s'
                  % (prov['_id'], prov['latest_downloaded'], (item for item in latest_available if
                                                              item["provider"] == prov['_id']).next()[
                'latest_ym_available']))

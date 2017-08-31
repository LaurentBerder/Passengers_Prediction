# -*- coding: utf-8 -*-

# -------------------------------------------------------------------------------
# Name:        Optimode / load_files_from_Chili
# Purpose:     Load data from files from Chili "junta de Aeronautica Civil"
#
# Author:      jolin
#
# Created:     08/09/15
# Copyright:   (c) Arsynet 2015
# Licence:     Tous droits réservés
# -------------------------------------------------------------------------------

from __future__ import print_function
import sys
sys.path.append('../')

import argparse
import logging
import logging.handlers
import os
import urllib2
import glob
from pprint import pprint

from bs4 import BeautifulSoup
import xlrd

from optidb.model import *
from utils import utcnow
from utils.asciify import asciify_alphanum
from utils.logging_utils import BackupFileHandler

__version__ = 'V1.1.1'


base_url = 'http://www.jac.gob.cl'  # 'http://www.jac-chile.cl'
tmp_dir = '/tmp/Chili'

months_list = ['ENE', 'FEB', 'MAR', 'ABR', 'MAY', 'JUN', 'JUL', 'AGO', 'SEP', 'OCT', 'NOV', 'DIC']


AIRLINES = dict()


def airline_dict(airline_name):
    al_name = airline_name.upper()
    if al_name not in AIRLINES:
        al = Company.find_one({'query_providers.chili': al_name})
        AIRLINES[al_name] = al
    return AIRLINES[al_name]


def trim(value):
    if value:
        value = unicode(value)
        return value.strip()
    return value


def int_or_str(value):
    if value:
        try:
            value = int(value)
            return value
        except:
            return trim(value)


class External_Segment_Tmp(Model):
    __collection__ = 'external_segment_laurent_tests'


class CannotFindTitle(Exception):
    pass


class UnknownAirlines(Exception):
    pass


class CouldNotFindEndOfBlock(Exception):
    pass


class CannotFindFileToDownload(Exception):
    pass


def get_block(ws, direct, year, historical_data=False):
    """
    Get a specific block of data from the excel worksheet

    :param ws: the worksheet
    :param direct: direct trip or return
    :param year: the year we are looking for
    :return: a list of dicts with the extracted data
    """
    end_of_title = [] if historical_data else months_list
    if direct:
        title = ['DESDE:', 'LLEGAN:', 'OPERADOR'] + end_of_title
        from_to = dict(origin=0, destination=1)
    else:
        title = ['LLEGAN:', 'DESDE:', 'OPERADOR'] + end_of_title
        from_to = dict(origin=1, destination=0)

    title_row = None
    for row in range(ws.nrows):
        title_ok = True
        for col, value in enumerate(title):
            if ws.cell_value(row, col) != value:
                title_ok = False
                break
        if title_ok:
            title_row = row
            break

    if title_row is None:
        raise CannotFindTitle("Cannot find title %r" % title)

    print('title_row:', title_row)
    if historical_data:
        years = [str(int_or_str(ws.cell_value(title_row, col))) for col in range(ws.ncols)]

    block = []
    sub_block = []
    block_ok = False
    for row in range(title_row + 1, ws.nrows):
        c0 = trim(ws.cell_value(row, 0))
        if c0 == 'TOTALES':
            # sub total
            continue
        if c0 == 'TOTAL' and trim(ws.cell_value(row, 1)) == 'GENERAL':
            # last line of the block
            block_ok = True
            break
        if c0 == 'TOTAL':
            # end of sub_block
            airports = ws.cell_value(row, 2).strip().split(' ')
            for line in sub_block:
                line['origin'] = airports[from_to['origin']]
                line['destination'] = airports[from_to['destination']]
                block.append(line)
            sub_block = []
            continue
        # add line to sub_block
        line = dict(al=trim(ws.cell_value(row, 2)),
                    # total=int(ws.cell_value(row, 15)),
                    row=row)
        al = airline_dict(line['al'])
        if al:
            line.update(al_iata_code=al['iata_code'], al_icao_code=al['icao_code'], al_ref_code=al['ref_code'])
        if historical_data:
            line['years'] = dict()
            for col in range(3, ws.ncols):
                line['years'][years[col]] = int(ws.cell_value(row, col))
        else:
            line['ym'] = dict()
            for month in range(1, 13):
                ym = '%04d-%02d' % (year, month)
                line['ym'][ym] = int(ws.cell_value(row, month+2))
        sub_block.append(line)
    if not block_ok:
        raise CouldNotFindEndOfBlock("Could not find end of block marker for title %r" % title)
    return block


def get_file_content(wb, year, historical_data=False):
    """
    Extracts the content of the excel spreadsheet (direct and return trips)
    :param wb: the spreadsheet
    :param year: the year we are looking for
    :return:
    """
    ws = wb.sheet_by_index(0)

    # looking direct trips
    block = get_block(ws, True, year, historical_data)
    # looking for the other way...
    block.extend(get_block(ws, False, year, historical_data))
    print('block:', len(block))
    pprint(block)

    unknown_airlines = set(b['al'] for b in block if 'al_iata_code' not in b)
    if unknown_airlines:
        raise UnknownAirlines("Unknown airlines: %s\n"
                              "  You must find the actual airlines in db.company and, for each company,  add an item "
                              "'query_providers.Chili' with the name from this list "
                              % u", ".join('"%s"' % a for a in unknown_airlines))
    return block


def _get_year(full_filename):
    _, f = os.path.split(full_filename)
    y = f[:4]
    if y.isdigit():
        return int(y)
    return None


def analyse_file(filename, analyse_all_files=False, historical_data=False):
    """
    Analyse an excel file (or multiple files)
    :param filename:
    :param analyse_all_files:
    :return:
    """
    files = os.listdir(tmp_dir)
    print('files:', files)

    results = []
    for full_filename in files:
        log.info('Getting file content for %s', full_filename)
        wb = xlrd.open_workbook(tmp_dir + '/' + full_filename)
        results.append(dict(filename=filename,
                            block=get_file_content(wb, _get_year(full_filename), historical_data)))
    return results


def store_block(block, filename):
    log.info("Storing results for file %s", filename)
    for line in block:
        y, y_label = ('ym', 'year_month') if 'ym' in line else ('years', 'year')
        for ym, pax in line[y].items():
            if pax == 0:
                continue
            seg = dict(data_type='airport',
                       provider='Chili',
                       inserted=now,
                       from_filename=filename,
                       from_line=line['row'],
                       airline=[line['al_iata_code']] or [line['al_icao_code']],
                       airline_ref_code=[line['al_ref_code']],
                       origin=[line['origin']],
                       destination=[line['destination']],
                       total_pax=pax,
                       raw_rec=line,
                       url=base_url,
                       both_ways=False
                       )
            seg[y_label] = [ym]
            key = dict((k, seg[k])
                       for k in ('data_type', 'provider', 'airline', 'origin', 'destination',
                                 y_label))
            key['raw_rec.al'] = line['al']

            External_Segment_Tmp.find_and_modify(key,
                                            {'$set': seg},
                                            upsert=True)


def analyse_and_store(filename, analyse_all_files=False, historical_data=False):
    results = analyse_file(filename, analyse_all_files, historical_data)
    for result in results:
        store_block(**result)


def get_files(requested_year, pattern=False):
    """
    Get files from the web site

    The files are downloaded to 'tmp_dir' directory
    :param pattern:
    :return:
    """
    log.info('Getting files on the web (all? %s)', pattern)
    download_page = urllib2.urlopen('%s/estadisticas/estadisticas-historicas' % base_url)
    page = download_page.read()

    # get all files' download urls
    soup = BeautifulSoup(page, "lxml")
    files_urls = [href for href in [a.get('href') for a in soup.find_all('a')]
                  if href and href.find('wp-content/uploads') >= 0]

    if pattern is not True:
        base, _ = os.path.split(files_urls[0])
        files_urls = [url for url in files_urls
                      if url.find(pattern) >= 0]

    log.info('files_urls: %r', files_urls)
    if not files_urls:
        raise CannotFindFileToDownload('Cannot find any file with pattern "%s"' % pattern)

    if not os.path.isdir(tmp_dir):
        os.mkdir(tmp_dir)
    for url in files_urls:
        file_year = int(os.path.split(url)[1].split('-')[0])
        if file_year not in requested_year:
            continue
        f_in = urllib2.urlopen('%s%s' % (base_url, url))
        _, filename = os.path.split(url)
        with open(os.path.join(tmp_dir, filename), 'wb') as f_out:
            f_out.write(f_in.read())
        log.info('File %s created', filename)


def cmd_line():
    parser = argparse.ArgumentParser(description='Load files from Chili to DB')
    # parser.add_argument('-g', '--get_files', dest='get_files', action='store_true')
    parser.add_argument('-a', '--all_files', dest='all_files', action='store_true')
    parser.add_argument('year_months', type=str, nargs='+', help='Year_month(s) to download ([YYYY-MM, YYYY-MM...]')
    # parser.add_argument('filename')
    return parser.parse_args()


if __name__ == '__main__':
    p = cmd_line()

    logging_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=logging_format)
    handler = BackupFileHandler('load_Chili.log', backupCount=20)
    formatter = logging.Formatter(logging_format)
    handler.setFormatter(formatter)

    main_log = logging.getLogger()  # le root handler
    log = logging.getLogger('load_Chili')
    # log.setLevel(logging.INFO)
    log.setLevel(logging.DEBUG)
    log.addHandler(handler)

    log.info('Load files from Chili - %s - %r', __version__, p)

    now = utcnow()
    year_months = p.year_months[0].split(', ')
    year = list(set([int(ym[0:4]) for ym in year_months]))

    file_pattern = p.all_files or 'Trafico-de-Par-de-ciudades-por-Operador'
    get_files(year, file_pattern)

    Model.init_db(def_w=True)

    if file_pattern is not True:
        for type_flight in ('Internacional', 'Nacional', ):
            analyse_and_store('%s-%s' % (file_pattern, type_flight))

    # analyse_and_store('Trafico-de-Par-de-ciudades-por-Operador-Nacional', True)
    # analyse_and_store('Trafico-de-Par-de-ciudades-por-Operador-Internacional', True)
    # analyse_and_store('Trafico-entre-pares-de-ciudades-por-operador-Internacional', True, True)
    # analyse_and_store('Trafico-entre-pares-de-ciudades-por-operador-Nacional', True, True)

    log.info("C'est fini...")

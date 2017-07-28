 #-*- coding: utf-8 -*-

# -------------------------------------------------------------------------------
# Name:        Optimode / treat_sources_scope
# Purpose:     For a given year_month, check all external sources, calculate ratio for non-overlapping data,
#              spread mass to calculate new passenger counts and revenues, and save in new_segments_initial_data.
#
# Author:      berder
#
# Created:     11/07/2017
# Copyright:   (c) Arsynet 2015
# Licence:     Tous droits réservés
# -------------------------------------------------------------------------------

from __future__ import division, print_function
import time
import argparse
import logging
import logging.handlers
import os
import sys
sys.path.append('../')
from optidb.model import *
from utils import YearMonth, utcnow
from utils.threads import Lock, ThreadPool
from utils.logging_utils import BackupFileHandler

now = utcnow()
lock = Lock()

__version__ = 'V1.0.2'


class External_Segment_Tmp(Model):
    __collection__ = 'external_segment_laurent_tests'


class Provider(Model):
    __collection__ = 'provider'


class Scopes_Tmp(Model):
    __collection__ = 'external_sources_scopes'


def reset_overlaps(year_month, providers):
    """
    If 'reset_overlap' argument is given, delete all the values of overlap on the corresponding year_month for the
    shortlisted providers
    :param year_month: string (YYYY-MM)
    :param providers: list
    :return:
    """
    log.info("Resetting all overlaps")
    reset = External_Segment_Tmp.update({'year_month': year_month,
                                         'provider': {'$in': providers}},
                                        {'$unset': {'overlap': 1}}, multi=True)
    log.info("Number of records reset: %r", reset)


def identify_overlaps(year_month, providers):
    """
    For each line, check if there are overlapping scopes (origin, destination, airline) with the following lines
    and if so, associate the records' id to each other.
    Only do so for the shortlisted providers
    :param year_month: string (YYYY-MM)
    :param providers: list
    :return:
    """
    query = {'year_month': year_month, 'provider': {'$in': providers}}
    sources_cursor = External_Segment_Tmp.find(query)
    nb_sources = sources_cursor.count()
    pct_sources = nb_sources / 100
    log.info("Identifying overlaps over %d new lines", nb_sources)

    with ThreadPool(20) as pool:
        def process_source(source, i):
            # build query
            start = utcnow()
            query = dict(provider={'$in': providers},
                         year_month={'$in': source.year_month},
                         _id={'$ne': source._id},
                         )

            if source.airline[0] != '*':
                query['airline'] = {'$in': source.airline + ['*']}

            query_od = dict((k, {'$in': source[k] + ['*']})
                            for k in ('origin', 'destination')
                            if source[k][0] != '*')

            if query_od:
                query_od_return = dict((k, {'$in': source[r] + ['*']})
                         for k, r in (('origin', 'destination'), ('destination', 'origin'))
                         if source[r][0] != '*')
                if not source.both_ways:
                    query_od_return['both_ways'] = True

                query['$or'] = [query_od, query_od_return]

            update = {'$addToSet': dict(overlap=source._id)}
            result = External_Segment_Tmp.update(query, update, multi=True)
            end = utcnow()
            log.info('Update %d (%ss) - %r', i, end-start, result)

        for i, source in enumerate(sources_cursor, 1):
            if i % 1000 == 0:
                log.info('Processed %dk records (%.1f%%)', i // 1000, i / pct_sources)
            # Parallelize process 20 times
            pool.add_task(process_source, source, i)

    log.info('end identify_overlap')
    return


def get_match(unique, for_segments=True, with_ref_code=False):
    """
    Determine query match for different cases in this program
    :param unique: a line of external_segments
    :param for_segments: boolean
    :param with_ref_code: boolean
    :return: dict
    """
    match = dict(record_ok=True, year_month={'$in': unique['year_month']})
    if unique['airline'][0] != '*':
        match['operating_airline'] = {'$in': unique['airline']}
        if with_ref_code:
            match['operating_airline_ref_code'] = unique['airline_ref_code']

    prefix = 'leg_' if for_segments else ''
    match_od = dict((od, {'$in': unique[k]})
                    for od, k in (('%sorigin' % prefix, 'origin'),
                                  ('%sdestination' % prefix, 'destination'))
                    if unique[k][0] != '*')
    if match_od:
        if unique.both_ways:
            match_od_return = dict((od, {'$in': unique[k]})
                                   for od, k in (('%sorigin' % prefix, 'destination'),
                                                 ('%sdestination' % prefix, 'origin'))
                                   if unique[k][0] != '*')
            match_od = {'$or': [match_od, match_od_return]}
        match.update(match_od)
    return match


def calculate_ratios():
    """
    For all the external segment lines that do not contain overlap, compare the sum of passengers (and revenue if existing)
    to the sum of passengers (and revenue) of the existing segments that the line will have an impact on.
    Calculate the ratio between the sums, and save in the external_segment line
    :return:
    """
    uniques_cursor = External_Segment_Tmp.find({'year_month': year_month,
                                                'overlap': {'$in': [None, []]},
                                                'provider': {'$in': providers}})
    log.info("Calculating ratios on %d non-overlapping data", uniques_cursor.count())

    def log_bulk(self):
        log.info('  saving ratios: %r', self.nresult)

    with External_Segment_Tmp.unordered_bulk(1000, execute_callback=log_bulk) as bulk, ThreadPool(20) as pool:

        def calculate_single_ratio(unique):
            ratio = {}
            match = get_match(unique)

            sum_pax = list(NewSegmentInitialData.aggregate([
                {'$match': match},
                {'$group': {
                    '_id': None,
                    'pax': {'$sum': '$passengers'}, 'revenue': {'$sum': '$segment_revenue_usd'}
                }
                }
            ]))
            if len(sum_pax) == 0 or sum_pax[0].get('pax') == 0:
                ratio['pax_ratio'] = None
            else:
                ratio['pax_ratio'] = unique['total_pax'] / sum_pax[0].get('pax')
                if not unique.get('revenue') or unique.get('revenue') == 0:
                    ratio['pax_rev'] = None
                else:
                    ratio['rev_ratio'] = unique['revenue'] / sum_pax[0].get('revenue')

            with lock:
                bulk.find(unique.__id_dict__).update_one({'$set': {'ratio': ratio}})

        for unique in uniques_cursor:
            pool.add_task(calculate_single_ratio, unique)

    log.info('end calculate ratios: %r', bulk.nresult)


def aggregate_capa(dat):
    match = get_match(dat, for_segments=False, with_ref_code=True)
    match.update(active_rec=True, capacity={'$gt': 0})

    group = {'_id': {'origin': '$origin',
                     'destination': '$destination',
                     'operating_airline_ref_code': '$operating_airline_ref_code',
                     'operating_airline': '$operating_airline',
                     'year_month': '$year_month'},
             'capacity': {'$sum': '$capacity'}}

    capas = CapacityInitialData.aggregate([
        {'$match': match},
        {'$group': group}
    ])

    def process_capa(capa):
        id = capa.pop('_id')
        capa.update(id)
        return capa

    return [process_capa(capa) for capa in capas]


def spread_mass_update(unique, bulk):
    """
    For routes that already exist in new_segment_initial_data and to which lines of external_segment make reference,
    apply the calculated ratio to save the new number of passengers and revenue.
    :param unique: a line of external_segment
    :param bulk: bulk
    :return:
    """
    log.info('update')
    rev_ratio = unique.ratio.get('rev_ratio') or unique.ratio.get('pax_ratio')

    for segment in External_Segment_Tmp.find(get_match(unique)):
        new_pax = max(1, int(segment.passengers * unique.ratio['pax_ratio'] + .5))
        print(segment)
        new_rev = max(1, int(segment.get('segment_revenue_usd', 0) * rev_ratio + .5))
        new_record = dict(passengers=new_pax, segment_revenue_usd=new_rev)
        initial_record = dict((k, segment[k]) for k in new_record.keys())

        updated = dict(on=now,
                       data_type='external_source',
                       initial_record=initial_record,
                       new_record=new_record,
                       external_provider=unique['provider'])
        with lock:
            pass
            bulk.find(segment.__id_dict__).update_one({'$set': new_record, '$push': dict(updated=updated)})


def spread_mass_create(unique, bulk, not_placed):
    """
    For routes that did not already exist in new_segment_initial_data, save the data from the external_segment directly
    if data is sufficiently atomical.
    If not atomical enough, put the route aside for display at the end of the program
    :param unique: a line of external_segment
    :param bulk: bulk
    :param not_placed: list
    :return:
    """
    log.info('create')

    def new_seg(origin, destination, airline, airline_ref_code, ym, pax, rev, unique_id, provider, loaded_from_file):
        ym_ym = YearMonth(ym)
        # Basic segment in the case where external_segment is sufficiently atomical and
        # corresponding new_segment_initial_data does not exist
        seg = NewSegmentInitialData(leg_origin=origin, origin=origin, trip_origin=origin,
                                    leg_destination=destination, destination=destination,
                                    trip_destination=destination, trip=[origin, destination],
                                    operating_airline=airline,
                                    operating_airline_ref_code=airline_ref_code,
                                    year_month=ym,
                                    year=ym_ym.year, month=ym_ym.month,
                                    passengers=pax, segment_revenue_usd=rev, od_revenue_usd=rev,
                                    cabin_class='EC', segment_split='Local',
                                    loaded_from_file=loaded_from_file,
                                    loaded_from_record=unique_id,
                                    record_ok=True, source='external_source', external_provider=provider,
                                    loaded_from_date=now)
        return seg

    if any(len(unique[k]) > 1 or unique[k] == ['*'] for k in ('origin', 'destination', 'airline')) or \
           len(unique.year_month) > 1:
        # Are there any capacity for this route? If so, spread mass according to capacity.
        # Otherwise, put the route aside for display at the end of the program
        capas = aggregate_capa(unique)
        if capas:
            sum_capas = sum(capa['capacity'] for capa in capas)
            ratio_pax = unique.get('total_pax') / sum_capas
            ratio_rev = unique.get('revenue') / sum_capas if unique.get('revenue') else None
            for capa in capas:
                pax = int(ratio_pax * capa['capacity'])
                rev = int(ratio_rev * capa['capacity']) if ratio_rev else None
                seg = new_seg(capa['origin'], capa['destination'], capa['operating_airline'],
                              capa['operating_airline_ref_code'], capa['year_month'],
                              pax, rev, unique.__id_dict__, unique['provider'], 'new_segment_from_external_source_by_capa')
                with lock:
                    pass
                    bulk.insert(seg)
        else:
            with lock:
                not_placed.append(unique)

    else:
        pax = unique.get('total_pax')
        rev = unique.get('revenue')
        origin = unique['origin'][0]
        destination = unique['destination'][0]
        operating_airline = unique['airline'][0]
        operating_airline_ref_code = unique['airline_ref_code'][0]
        ym = unique['year_month'][0]

        seg = new_seg(origin, destination, operating_airline, operating_airline_ref_code,
                      ym, pax, rev, unique.__id_dict__,
                      unique['provider'],'new_segment_from_external_source_by_segments')
        with lock:
            bulk.insert(seg)


def save_new_segments(providers, not_placed):
    """
    Check if route exists (and update it), or needs to be created, and save data.
    Store non-existing data in non-atomical format in not_placed for display at the end of the process.
    :param providers: list
    :param not_placed: empty_list
    :return:
    """
    uniques_cursor = External_Segment_Tmp.find({'year_month': year_month,
                                                'overlap': {'$in'[None, []]},
                                                'provider': {'$in': providers}})
    pct_uniques = uniques_cursor.count() / 100

    def log_bulk(self):
        log.info('  store NewSegments: %r', self.nresult)

    with NewSegmentInitialData.unordered_bulk(1000, execute_callback=log_bulk) as bulk, ThreadPool(20) as pool:

        def process_unique(unique):
            log.info('origin: %r, destination: %r, airline: %r, passengers: %d, pax_ratio:%s',
                     unique.origin, unique.destination, unique.airline, unique.total_pax, unique.get('ratio', {}).get('pax_ratio'))
            # If we've been able to calculate a ratio between new pax count and existing pax count, update data
            # Otherwise, create new segment if data is enough, or store line to see what went wrong.
            if unique.get('ratio', {}).get('pax_ratio'):
                spread_mass_update(unique, bulk)
            else:
                spread_mass_create(unique, bulk, not_placed)

        for i, unique in enumerate(uniques_cursor, 1):
            if i % 1000 == 0:
                log.info('** %s%% **', '{0:.1g}'.format(i / pct_uniques))
            pool.add_task(process_unique, unique)

    log.info('end store NewSegments: %r', bulk.nresult)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Adjust segments based on external sources for a single year month')
    parser.add_argument('ym', type=YearMonth, help='YearMonth (YYYY-MM) to deal with')
    parser.add_argument('--first_step', dest='first_step', type=int, default=1, help='1: Start from overlaps detection,'
                                                                                  '2: Start from ratios calculation,'
                                                                                  '3: Only do the spreading')
    parser.add_argument('--reset_overlap', dest='reset_overlap', action='store_true', help='If present, reset all overlaps')
    p = parser.parse_args()

    logging_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=logging_format)
    handler = BackupFileHandler(filename='treat_sources_scope.log', mode='w', backupCount=20)
    formatter = logging.Formatter(logging_format)
    handler.setFormatter(formatter)
    main_log = logging.getLogger()  # le root handler
    main_log.addHandler(handler)
    log = logging.getLogger('Treat_sources_scope')
    log.info('Treating scope for external sources, version %s - %r',  __version__, p)

    start_time = time.time()
    Model.init_db(def_w=True)

    year_month = str(p.ym)
    providers = [prov.provider for prov in Provider.find({'import_process': True})]
    nb_overall_lines = External_Segment_Tmp.find({'year_month': year_month, 'provider': {'$in': providers}}).count()

    if p.first_step == 1:
        # Phase 1 - Identify overlaps (possibly after deleting all previously identified ones, then save in external_segment
        if p.reset_overlap:
            reset_overlaps(year_month, providers)
        identify_overlaps(year_month, providers)

        pct_overlap = External_Segment_Tmp.find({'year_month': year_month, 'provider': {'$in': providers},
                                                 'overlap': {'$nin': [None, []]}}).count() / nb_overall_lines * 100
        log.info("%3.2f%% of overlapping data over the %d lines treated", pct_overlap, nb_overall_lines)

    if p.first_step <= 2:
        # Phase 2 - Calculate ratios, then save in external_segment
        calculate_ratios()

    # # Phase 3 - Spread mass, then save new passenger counts and revenues in new_segments_initial_data
    # not_placed = []
    # log.info("Identifying routes corresponding to non-overlapping data, saving in database")
    # save_new_segments(providers, not_placed)
    #
    # # Reste à traiter les chevauchements
    #
    # log.info("\n\n--- %s seconds to compile data from %d sources ---", time.time() - start_time,
    #          len(External_Segment_Tmp.find({'year_month': year_month}).distinct('provider')))
    #
    # if len(not_placed) > 0:
    #     log.warning("These %d data were not placed, because of lack of atomicity, and no linked segment nor capacity:",
    #                 len(not_placed))
    #     not_placed = pd.DataFrame.from_records(not_placed)
    #     col_to_keep = ['_id', 'origin', 'destination', 'year_month', 'airline',
    #                    'airline_ref_code', 'provider', 'from_filename']
    #     not_placed = not_placed[col_to_keep]
    #     log.warning(not_placed)
    log.info('End')

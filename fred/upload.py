import logging
import os
import time
import numpy as np
import pandas as pd

from collections import deque
from datetime import datetime, date, timedelta


logger = logging.getLogger(__name__)

def upload_id(db_conn, api, schema_name, table_name, dup_cols = ['id']):
    data = pd.DataFrame(api.get_category(0))
    stack = deque([0])
    
    while stack:
        res = api.get_category_children(category_id = stack.popleft())
        time.sleep(.2)
    
        if res.shape[0] == 0:
            continue
    
        stack += res['id'].tolist()
        data = db_conn.ext_notin_db(res, schema_name = schema_name, table_name = table_name, subset = dup_cols)

        if data.shape[0] == 0:
            continue
            
        db_conn.insert_df(data, schema_name = schema_name, table_name = table_name)

    logger.info('Upload ID finished!')


    
def upload_tags(db_conn, api, schema_name, table_name, dup_cols = ['name', 'series_count']):
    res = api.get_tags(params = {'order_by': 'series_count', 'sort_order': 'asc', 'realtime_start': '1800-01-01'}).drop_duplicates(subset = 'name', keep = 'last')
    data = db_conn.ext_notin_db(res, schema_name = schema_name, table_name = table_name, subset = dup_cols)
    db_conn.upsert(data, schema_name = schema_name, table_name = table_name)
    logger.info('Upload tags finished!')

def upload_releases(db_conn, api, schema_name, table_name, dup_cols = ['id', 'realtime_start']):
    res = api.get_releases(params = {'realtime_start': '1776-07-04'})
    data = db_conn.ext_notin_db(res, schema_name = schema_name, table_name = table_name, subset = dup_cols)
    db_conn.upsert(data, schema_name = schema_name, table_name = table_name)
    logger.info('Upload releases finished!')

def upload_releases_date(db_conn, api, schema_name, table_name):
    db_first_realdate = db_conn.get_maxmin_col(schema_name, 'releases', column = 'realtime_start')[1]

    db_latest_date = db_conn.get_maxmin_col(schema_name, table_name, column = 'date')[0]
    
    realtime_start = max(db_first_realdate, db_latest_date) if db_latest_date else db_first_realdate
    
    data = api.get_data('releases', 'dates', params = {'realtime_start': realtime_start,
                                                    'include_release_dates_with_no_data': 'true'})
    
    db_conn.upsert(data, schema_name, table_name)
    logger.info(f"Upload releases_date data finished!")

def upload_sources(db_conn, api, schema_name, table_name, dup_cols = ['id', 'realtime_start']):
    res = api.get_sources(params = {'realtime_start': '1776-07-04'})
    data = db_conn.ext_notin_db(res, schema_name = schema_name, table_name = table_name, subset = dup_cols)
    db_conn.upsert(data, schema_name = schema_name, table_name = table_name)
    logger.info('Upload sources finished!')


def upload_series(db_conn, api, conf):
    id_list = db_conn.get_data(schema_name = conf['schema_name'], table_name = conf['data']['releases']['table_name'], columns = 'id', is_distinct = True)
    id_list = id_list.values.ravel()

    
    for idx, release_id in enumerate(id_list):
        logger.info(f"upload series related release_id {release_id} processing... ({idx+1}/{len(id_list)})")
        max_ts = db_conn.get_maxmin_col(conf['schema_name'], conf['data']['series']['table_name'], column = '_ts',
                                    where = [f"release_id={release_id}"])[0]

        max_ts = (max_ts - timedelta(days = 3)).strftime('%Y-%m-%d') if max_ts else conf['earliest_date']

        db_last_updated = db_conn.get_maxmin_col(conf['schema_name'], conf['data']['series']['table_name'], column = 'last_updated',
                                                where = [f"release_id={release_id}"])[0]

        db_last_updated = conf['earliest_date'] if db_last_updated is None else db_last_updated

        if db_last_updated == conf['earliest_date']:
            data = api.get_data(detail_url = 'release', sub_url = 'series', 
                            params = {'realtime_start': max_ts, 'release_id': release_id})
        else:
            data = api.get_data(detail_url = 'release', sub_url = 'series', 
                            params = {'realtime_start': max_ts, 'release_id': release_id, 'order_by': 'last_updated'}, 
                            order_by = 'last_updated', db_last_updated = db_last_updated)


        if data.shape[0] == 0:
            continue
        
        data = data.sort_values('last_updated').drop_duplicates(conf['data']['series']['dup_cols'])
        data['release_id'] = release_id
        data = data[pd.to_datetime(data['last_updated'], utc = True) >= pd.to_datetime(db_last_updated, utc = True)]
        db_conn.upsert(data, conf['schema_name'], conf['data']['series']['table_name'])
    
    
    logger.info('Upload series finished!')


def upload_observations(db_conn, api, conf):
    obs_latest_date = db_conn.get_data(conf['schema_name'], conf['data']['observations']['table_name'], columns = ['id', 'max(date) AS obs_max_date'],
                          additional_sql = 'GROUP BY id')
    
    series_latest_date = db_conn.get_data(conf['schema_name'], conf['data']['series']['table_name'], columns = ['id', 'max(observation_end) AS series_max_date'],
                                   additional_sql = 'GROUP BY id')
    
    date_info = series_latest_date.merge(obs_latest_date, how = 'left', on = 'id')
    
    id_list = date_info.loc[date_info.apply(lambda x: True if x['obs_max_date'] is np.nan else x['series_max_date'] > x['obs_max_date'], axis = 1), 'id'].ravel()
    
    

    for idx, series_id in enumerate(id_list):
        db_last_updated = db_conn.get_maxmin_col(conf['schema_name'], conf['data']['observations']['table_name'], column = 'date',
                                                where = [f"id='{series_id}'"])[0]

        db_last_updated = conf['earliest_date'] if db_last_updated is None else db_last_updated
        try:
            data = api.get_data(detail_url= 'series', sub_url = 'observations', params = {'series_id': series_id},
                               order_by = 'date', db_last_updated = db_last_updated).drop_duplicates(keep = 'last')
        except Exception as e:
            logger.error(e)
            continue

        data['id'] = series_id
        data['value'] = data['value'].replace('.', np.nan)
        data = data[pd.to_datetime(data['date']).dt.date >= db_last_updated]
        db_conn.upsert(data, conf['schema_name'], conf['data']['observations']['table_name'])
        logger.info(f"Upload {series_id} data finished! ({idx+1}/{len(id_list)})")
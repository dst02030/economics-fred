import logging
import json
import requests
import io
import os
import time
import zipfile


import numpy as np
import pandas as pd

from io import BytesIO
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)




class Fred:
    def __init__(self, auth_key, max_rows = 1000, max_api_retries = 30):
        logger.info(f"### fred main api is initialized! ###")
        self.title_url = "https://api.stlouisfed.org/fred"
        self.max_rows = max_rows if max_rows else 1000
        self.auth_key = auth_key
        self.params = {'api_key': auth_key,
                       'file_type': "json"}
        self.max_api_retries = max_api_retries



    def get_api_data(self, detail_url = '', sub_url = '', params = {}, max_retries = 30):
        tries = 0
        
        url = f"{self.title_url}/{detail_url}"
        url += f"/{sub_url}" if len(sub_url) > 0 else ""
        
        req_params = params
        req_params.update(self.params)
        params_list = [f"{key}={val}" for key, val in req_params.items()]
        query_url = "&".join(params_list)
    
        url += f"?{query_url}" if len(query_url) > 0 else ""
        

        while tries < max_retries:
            tries += 1
            res = requests.get(url)

            
            if not res.ok:
                if res.status_code == 504:
                    logger.warning(f"504 Error code: Gateway time out occured on server side.")

                
                if res.status_code == 400:
                    logger.warning(f"404 Error code: There is no data in {detail_url}, params: {params}")
                    res.raise_for_status()
                
                logger.info(f"API call tries {tries}/{max_retries}")
                logger.info(res.text)
                time.sleep(5)
                continue

            content = json.loads(
                res.text
                )
            
            return content
        
        res.raise_for_status()
    
    def get_data(self, detail_url, sub_url = '', params = {}, _ts = datetime.astimezone(datetime.now()), order_by = None, db_last_updated = None):
        data = pd.DataFrame()
        
        path_url = detail_url
        path_url += f"/{sub_url}" if sub_url else ""

        if order_by:
            params['sort_order'] = 'desc'


        offset, count = 0, 1000
        
        while offset < count:
            
            res = self.get_api_data(detail_url, sub_url, params)

            count = res['count'] if 'count' in res else len(res)                
            offset += self.max_rows
            params['offset'] = offset

            key = [key for key, val in res.items() if isinstance(val, list)][0]
            res = res[key]
            
            data = pd.concat([data, pd.DataFrame(res)])
            
            logger.info(f"{detail_url}/{sub_url} data, parameters {params} load status: {min(offset, count)} / {count}.")

            if order_by:
                min_last_update = pd.to_datetime(data[order_by], utc = True).min() if data.shape[0] > 0 else None

                if min_last_update is not None and pd.to_datetime(db_last_updated, utc = True) >= min_last_update:
                    logger.info("lastest data load finished. Start to upload task.")
                    break
                    

        if data.shape[0] == 0:
            return pd.DataFrame()
        
        data['_ts'] = _ts
        return data
    
    def get_category(self, category_id = 0, params = {}):
        params['category_id'] = category_id
        return self.get_data(detail_url = 'category', params = params)
    
    def get_category_children(self, category_id = 0, params = {}):
        params['category_id'] = category_id
        return self.get_data(detail_url = 'category', sub_url = 'children', params = params)

    def get_tags(self, params = {}):
        return self.get_data(detail_url = 'tags', params = params)

    def get_sources(self, params = {}):
            return self.get_data(detail_url = 'sources', params = params)

    def get_releases(self, params = {}):
        return self.get_data(detail_url = 'releases', params = params)
   
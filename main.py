import logging
import os
import sys

import pandas as pd

from datetime import datetime
from logging.handlers import TimedRotatingFileHandler


from fred.api import Fred
from fred.utils import get_jinja_yaml_conf, create_db_engine, Postgres_connect
from fred.upload import upload_id, upload_tags, upload_releases, upload_releases_date, upload_sources, upload_series, upload_observations

def main():
    os.chdir(os.path.dirname(__file__))
    conf = get_jinja_yaml_conf('./conf/api.yml', './conf/logging.yml')



    # logger 설정
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=eval(conf['logging']['level']),
        format=conf['logging']['format'],
        handlers = [TimedRotatingFileHandler(filename =  conf['logging']['file_name'],
                                    when=conf['logging']['when'],
                                    interval=conf['logging']['interval'],
                                    backupCount=conf['logging']['backupCount']), logging.StreamHandler()]
                    )



    engine = create_db_engine(os.environ)
    postgres_conn = Postgres_connect(engine)


    fred_api = Fred(auth_key = os.environ['auth_key'])
    
    if sys.argv[1] in conf['data']:
        logger.info(f"Run {sys.argv[1]}!")
    
    else:
        raise Exception(f"You entered not allowed run mode. Run modes are: {list(conf['data'].keys())}")
    
    
    if sys.argv[1] == 'id':
        upload_id(postgres_conn, fred_api, schema_name = conf['schema_name'], table_name = conf['data']['id']['table_name'])


    elif sys.argv[1] == 'tags':
        upload_tags(postgres_conn, fred_api, schema_name = conf['schema_name'], table_name = conf['data']['tags']['table_name'], dup_cols = conf['data']['tags']['dup_cols'])

    elif sys.argv[1] == 'releases':
        upload_releases(postgres_conn, fred_api, schema_name = conf['schema_name'], table_name = conf['data']['releases']['table_name'], dup_cols = conf['data']['releases']['dup_cols'])

    elif sys.argv[1] == 'releases_date':
        upload_releases_date(postgres_conn, fred_api, schema_name = conf['schema_name'], table_name = conf['data']['releases_date']['table_name'])

    elif sys.argv[1] == 'sources':
        upload_sources(postgres_conn, fred_api, schema_name = conf['schema_name'], table_name = conf['data']['sources']['table_name'], dup_cols = conf['data']['sources']['dup_cols'])

    elif sys.argv[1] == 'series':
        upload_series(postgres_conn, fred_api, conf)

    elif sys.argv[1] == 'observations':
        upload_observations(postgres_conn, fred_api, conf)

        
        
if __name__ == "__main__":
    main()
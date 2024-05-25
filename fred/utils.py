import logging
import yaml

import requests
import json

import pandas as pd

from datetime import date, datetime
from sqlalchemy import create_engine
from jinja2 import Template
from urllib.parse import quote


logger = logging.getLogger(__name__)

def get_jinja_yaml_conf(*file_names):
    """
    jinja2 포맷을 이용해서 yaml 파일을 읽는 함수입니다.
    
    Args:
        file_name: 읽을 파일 이름입니다.
    
    Returns: 
        conf_dict: 파일을 읽어낸 결과입니다.
    """

    conf_dict = dict()

    for file_name in file_names:

        with open(file_name, encoding='utf-8') as f:
            t = Template(f.read())
            c = yaml.safe_load(t.render())
            def_conf = yaml.safe_load(t.render(c))
            conf_dict.update(def_conf)
            
    return conf_dict


def create_db_engine(engine_info):
    """
    DB 엔진을 만드는 함수입니다.

    Args:
        engine_info: DB 정보를 포함한 딕셔너리 입니다. DB_TYPE, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME 정보를 포함해야 합니다.

    Returns:
        engine: 생성된 DB 엔진입니다.
    """
    engine_info = {key: quote(val) for key, val in engine_info.items()}
    conn_str = "{DB_TYPE}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}".format(**engine_info)
    engine = create_engine(conn_str, client_encoding='utf8')
    logger.info(f'Connect to {engine_info["DB_HOST"]}. DB_NAME is {engine_info["DB_NAME"]}')
    return engine


class Postgres_connect:

    def __init__(self, engine):
        self.engine = engine
        return

    def get_data(self, schema_name, table_name, columns = [], where = [], where_operator = 'AND', additional_sql = "", is_distinct = False, orderby_cols = []):
        columns = [columns] if isinstance(columns, str) else columns
        
        if len(columns) == 0:
            columns = ['*']
        
        where_clause = self._make_where(where, where_operator)
        sql = f"""SELECT {"DISTINCT" if is_distinct else ""} {', '.join(columns)} 
                                FROM {schema_name}.{table_name} {where_clause}""" + additional_sql
        
        data = pd.read_sql(sql, con = self.engine
                                )

        logger.debug(sql)

        if orderby_cols:
            orderby_cols = [orderby_cols] if isinstance(orderby_cols, str) else orderby_cols
            data.sort_values(by = orderby_cols, ignore_index = True, inplace = True)


        return data


    

    def get_maxmin_col(self, schema_name, table_name, column, where = [], where_operator = 'AND', additional_sql = "", is_max = True, is_min = True):
        if not is_max and not is_min:
            raise Exception("You should set True at least one of is_min or is_max.")

        where_clause = self._make_where(where, where_operator)
        maxmin = []
        if is_max: maxmin.append(f"MAX({column})")
        if is_min: maxmin.append(f"MIN({column})")

        sql = f"""SELECT {", ".join(maxmin)} 
                                FROM {schema_name}.{table_name} {where_clause}""" + additional_sql
        logger.debug(sql)

        return pd.read_sql(sql, con = self.engine
                                ).to_numpy().ravel()

    

    def get_count(self, schema_name, table_name, where = [], where_operator = 'AND', additional_sql = ""):
        where_clause = self._make_where(where, where_operator)
        sql = f"SELECT COUNT(*) FROM {schema_name}.{table_name} {where_clause}" + additional_sql
        data = pd.read_sql(sql, con = self.engine
                                ).iloc[0].item()
        logger.debug(sql)
        return int(data)
    
    
    
    def insert_df(self, data, schema_name, table_name):
        data.to_sql(con = self.engine,
                schema = schema_name,
                name = table_name,
                if_exists = 'append',
                index = False)
        
        logger.info(f"Upload data successfully (rows: {data.shape[0]}).")
        
        
        
    
    def upsert(self, data, schema_name, table_name, del_rows = 1000):

        # 해당 테이블의 pkey constraint 이름 찾기
        pkey_constraint = pd.read_sql(sql = f"""SELECT constraint_name
        FROM information_schema.table_constraints
        WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' AND constraint_type = 'PRIMARY KEY'""",
            con = self.engine)
        
        if pkey_constraint.shape[0] == 0:
            logger.info(f"Upload data starts: {data.shape[0]} rows.")
            return data.to_sql(table_name, schema = schema_name, con = self.engine, index = False, if_exists = "append")

        else:
            pkey_constraint = pkey_constraint.iloc[0, 0]

        # 해당 테이블의 primary key 리스트
        pkey_cols = pd.read_sql(f"""SELECT column_name
        FROM information_schema.key_column_usage
        WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' AND constraint_name = '{pkey_constraint}';""",
                con = self.engine).to_numpy().ravel()
        
        # db 내에 pkey 중복 데이터 제거를 위한 where문 생성
        in_list = list()
        for _, row in data[pkey_cols].iterrows():
            row_processing = [val.replace("'", "''").replace('%', '%%') if isinstance(val, str) else val for val in row.values]
            row_processing = [f"'{val}'" if isinstance(val, (str, date, datetime)) else str(val) for val in row_processing]
            in_list.append(f"({', '.join(row_processing)})")
        
        
        # delete문을 통해 중복 데이터 제거
        for i in range(len(in_list) // del_rows + 1):
            with self.engine.begin() as conn:
                del_list = in_list[i*del_rows:(i+1)*del_rows]
                where_clause = f"WHERE ({', '.join(pkey_cols)}) IN ({', '.join(del_list)})"

                del_sql = f"""DELETE FROM {schema_name}.{table_name}
                                            {where_clause}"""

                if len(del_list) > 0:
                    logger.debug(del_sql)
                    conn.exec_driver_sql(del_sql)

        logger.info(f"Upload data starts: {data.shape[0]} rows.")
        
        # 데이터 업로드
        return data.to_sql(table_name, schema = schema_name, con = self.engine, index = False, if_exists = "append")
    

    
    def ext_notin_db(self, data, schema_name, table_name,  subset = []):

        if isinstance(subset, str):
            subset = [subset]
        
        if len(subset) == 0:
            subset = data.columns.tolist()


        db_data = self.get_data(schema_name = schema_name, 
                        table_name = table_name, 
                        columns = subset,
                        is_distinct = True)
        
        coltype = db_data.dtypes
        coltype[coltype =='object'] = str
        data = data.astype(coltype)

        merge_data = pd.merge(data, db_data, how = 'outer', on = subset, indicator = True)
        

        return merge_data[merge_data['_merge'] == 'left_only'].drop(columns = '_merge')
    

    def get_engine(self):
        return self.engine

    def _make_where(self, where, where_operator = 'AND'):
        return f"WHERE {f' {where_operator} '.join(where)}" if where else ""

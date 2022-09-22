import os
from urllib.parse import quote_plus as urlquote

import pymysql.cursors
import pandas as pd
import sqlalchemy

from src.utils import logging, parse_string_to_list


ENVIRONMENT = os.getenv('ENV')


class MysqlDB:
    def __init__(
        self,
        host: str=None,
        port: int=None,
        user: str=None,
        password: str=None,
        database: str=None,
    ):
        self._logger = logging.getLogger(__class__.__name__)
        self.host = 'host.docker.internal' if ENVIRONMENT == 'DEVELOPMENT' else host
        self.port = 6000 if ENVIRONMENT == 'DEVELOPMENT' else port
        self.host = 'host.docker.internal'
        self.port = 6000
        self.user = user
        self.password = password
        self.database = database

        self.get_connection()

    def get_connection(self):
        self.conn = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            cursorclass=pymysql.cursors.DictCursor
        )
        conn_string = 'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'.format(
            user=self.user,
            password=urlquote(self.password),
            host=self.host,
            port=self.port,
            database=self.database
        )
        self.engine = sqlalchemy.create_engine(conn_string)
        self.cursor = self.conn.cursor()

    def close_connection(self):
        self.conn.close()

    def get_total_records(self, table_name: str):
        query = f'SELECT COUNT(*) AS total_records FROM {table_name}'
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        if result:
            return result['total_records']
        else:
            return 0

    def get_data_from_db(self, query: str):
        df = pd.read_sql(query, self.engine)

        return df

    def get_schema_from_source_table(self, table_name: str, key_columns: str = None):
        query = """
        SELECT COLUMN_NAME AS name,
            CASE
                WHEN DATA_TYPE IN ('tinyint', 'smallint', 'mediumint', 'int', 'bigint') THEN 'INTEGER'
                WHEN DATA_TYPE IN ('decimal', 'float', 'double') THEN 'FLOAT'
                WHEN DATA_TYPE IN ('char', 'nchar', 'varchar', 'tinytext', 'text', 'mediumtext','longtext', 'enum') THEN 'STRING'
                WHEN DATA_TYPE ='bit' THEN 'BOOLEAN'
                WHEN DATA_TYPE ='datetime' THEN 'TIMESTAMP'
                ELSE UPPER(DATA_TYPE)
            END AS type
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = '{db_name}'
        AND TABLE_NAME = '{table_name}'
        """.format(
            db_name=self.database,
            table_name=table_name
        )
        list_key_columns = []
        if key_columns:
            list_key_columns = parse_string_to_list(key_columns)

        df = pd.read_sql(query, self.engine)
        if df.shape[0] > 0:
            df['mode'] = 'NULLABLE'
            df.loc[df['name'].isin(list_key_columns), 'is_primary_key'] = True
            df.loc[~df['name'].isin(list_key_columns), 'is_primary_key'] = False

            return df.to_dict(orient='records')

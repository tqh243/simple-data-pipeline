import os
import ssl

import pyodbc
import pandas as pd
import sqlalchemy

from src.utils import logging

class SqlServerDB:
    def __init__(
        self,
        host: str=None,
        port: str=None,
        user: str=None,
        password: str=None,
        database: str=None,
        catalog: str=None,
    ):
        self._logger = logging.getLogger(__class__.__name__)
        # self.host = host
        # self.port = port
        self.host = 'host.docker.internal'
        self.port = 6000
        self.user = user
        self.password = password
        self.database = database
        self.catalog = catalog

        self.get_connection()

    def get_connection(self):
        self.conn = pyodbc.connect(
            driver='ODBC Driver 17 for SQL Server',
            server=f'{self.host},{self.port}',
            uid=self.user,
            pwd=self.password,
            database=self.database,
        )
        conn_string = 'mssql+pyodbc://{user}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server'.format(
            user=self.user,
            password=self.password,
            server=f'{self.host},{self.port}',
            database=self.database
        )
        self.engine = sqlalchemy.create_engine(conn_string)
        self.cursor = self.conn.cursor()

    def close_connection(self):
        self.conn.close()

    def get_total_records(self, table_name: str):
        query = f'SELECT COUNT(*) FROM {self.database}.{self.catalog}.{table_name}'
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        if result:
            return result[0]
        else:
            return 0

    def get_data_from_db(self, query: str):
        df = pd.read_sql(query, self.engine)

        return df

    def get_schema_from_source_table(self, table_name: str):
        query = """
        SELECT COLUMN_NAME AS name,
            CASE
                WHEN DATA_TYPE LIKE '%int%' THEN 'VARCHAR'
                WHEN DATA_TYPE IN ('decimal', 'money') THEN 'FLOAT'
                WHEN DATA_TYPE IN ('char', 'nchar', 'varchar','nvarchar','uniqueidentifier', 'SMALLDATETIME', 'NUMERIC') THEN 'VARCHAR'
                WHEN DATA_TYPE = 'bit' THEN 'BOOLEAN'
                WHEN DATA_TYPE IN ('datetime') THEN 'TIMESTAMP'
                WHEN COLUMN_NAME = 'VersionNo' THEN 'BYTES'
                ELSE UPPER(DATA_TYPE)
            END AS type
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{catalog}'
        AND TABLE_NAME = '{table_name}'
        """.format(
            catalog=self.catalog,
            table_name=table_name
        )

        df = pd.read_sql(query, self.engine)
        if df.shape[0] > 0:
            df['mode'] = 'NULLABLE'
            return df.to_dict(orient='records')

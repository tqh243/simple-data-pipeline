import os
from typing import Union, Optional

import psycopg2
from sqlalchemy import create_engine
import pandas as pd

from src.utils import logging

DEFAULT_HOST = os.getenv('DWH_POSTGRES_HOST')
DEFAULT_PORT = os.getenv('DWH_POSTGRES_PORT')
DEFAULT_USER = os.getenv('DWH_POSTGRES_USER')
DEFAULT_PASSWORD = os.getenv('DWH_POSTGRES_PASSWORD')
DEFAULT_DATABASE = os.getenv('DWH_POSTGRES_DATABASE')
DEFAULT_SCHEMA = os.getenv('DWH_POSTGRES_SCHEMA')


class PostgresDB:
    def __init__(
        self,
        host: str=None,
        port: str=None,
        user: str=None,
        password: str=None,
        database: str=None,
        schema: str=None,
        logger = None
    ):
        if logger:
            self._logger = logger
        else:
            self._logger = logging.getLogger(__class__.__name__)
        self.host = host if host else DEFAULT_HOST
        self.port = port if port else DEFAULT_PORT
        self.user = user if user else DEFAULT_USER
        self.password = password if password else DEFAULT_PASSWORD
        self.database = database if database else DEFAULT_DATABASE
        self.schema = schema if schema else DEFAULT_SCHEMA

        self.get_connection()

    def get_connection(self):
        self.conn = psycopg2.connect(
            host=self.host,
            port=int(self.port),
            user=self.user,
            password=self.password,
            database=self.database
        )

        self.cursor = self.conn.cursor()

    def get_engine(self):
        return create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'.format(
            user=self.user,
            password=self.password,
            host=self.host,
            port=int(self.port),
            database=self.database
        ))

    def close_connection(self):
        self.conn.close()

    def check_table_exists(self, table_name: str):
        query = f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = '{self.schema}'
                AND table_name = '{table_name}'
            );
        """
        cursor = self.conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()

        return result[0]

    def get_columns_from_source_table(self, source_table_name: str):
        source_table_name = source_table_name.replace('"', '')
        query = f"""
			SELECT COLUMN_NAME
			FROM INFORMATION_SCHEMA.COLUMNS
			WHERE table_name = '{source_table_name}';
		"""

        self.cursor.execute(query)
        columns = []
        for column in self.cursor:
          columns.append(column[0])

        return columns

    def build_merge_query_from_source_table(self, source_table_name: str, destination_table_name: str, primary_key: str):
        columns = self.get_columns_from_source_table(source_table_name)
        columns_str = ', '.join(columns)
        update_statement = 'SET '

        for column in columns:
            update_statement += f'{column} = excluded.{column},\n'
        if update_statement[-2] == ',':
            update_statement = update_statement[:-2]
        update_statement += ';'

        query = f"""
            INSERT INTO {self.schema}.{destination_table_name} ({columns_str})
            SELECT {columns_str} FROM {self.schema}.{source_table_name}
            ON CONFLICT ({primary_key})
            DO UPDATE
                {update_statement}
        """

        return query

    def build_replace_query_from_source_table(self, source_table_name: str, destination_table_name: str):
        query = f"""
			DROP TABLE IF EXISTS {self.schema}.{destination_table_name};

			CREATE TABLE {self.schema}.{destination_table_name} AS
			SELECT * FROM {self.schema}.{source_table_name};
		"""

        return query

    def get_data_from_db(self, query: str, return_as_df: Optional[bool] = False) -> list:
        if return_as_df:
            engine = self.get_engine()
            result = pd.read_sql_query(query, engine)
        else:
            cursor = self.conn.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            result = [x[0] for x in result]

        return result

    def create_new_table(self, table_name: str, table_schema: list):
        fields_statement_components = []
        primary_keys_statement_components = []
        for field in table_schema:
            field_type = '{name} {type}'.format(
                name=field['name'],
                type=field['type']
            )
            fields_statement_components.append(field_type)

            if field['is_primary_key']:
                primary_keys_statement_components.append(field['name'])

        fields_statement = ',\n'.join(fields_statement_components)

        query = f"""
            CREATE TABLE {self.schema}.{table_name} (
                {fields_statement},
                etl_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                etl_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        """

        if primary_keys_statement_components:
            primary_keys = ', '.join(primary_keys_statement_components)
            query += f', PRIMARY KEY ({primary_keys})\n'

        query += ');\n'
        cursor = self.conn.cursor()
        cursor.execute(query)
        self.conn.commit()

        self._logger.info(f'Table {table_name} is created!')

    def _prepare_merge_query(self, cursor, table_name: str, table_schema: list, data: list):
        columns = ','.join([field['name'] for field in table_schema])
        values_statement = ','.join(['%s'] * len(data))

        update_columns = []
        for field in table_schema:
            column_name = field['name']
            statement = f'{column_name} = EXCLUDED.{column_name}'
            update_columns.append(statement)

        update_columns_statement = ',\n'.join(update_columns)
        update_statement = f'SET {update_columns_statement}'

        # get primary keys
        primary_keys = [x['name'] for x in table_schema if x['is_primary_key']]
        primary_keys_statement = ','.join(primary_keys)

        query = f"""
            INSERT INTO {self.schema}.{table_name} ({columns})
            VALUES {values_statement}
            ON CONFLICT ({primary_keys_statement})
            DO UPDATE
            {update_statement},
            etl_updated_at = CURRENT_TIMESTAMP
        """
        query = cursor.mogrify(query, data).decode('utf8')

        return query

    def _prepare_insert_query(self, cursor, table_name: str, table_schema: list, data: list):
        columns = ','.join([field['name'] for field in table_schema])

        # add etl_updated_at column
        values_statement = ','.join(['%s'] * len(data))

        query = f"""
            INSERT INTO {self.schema}.{table_name} ({columns})
            VALUES {values_statement}
        """

        query = cursor.mogrify(query, data).decode('utf8')

        return query

    def merge_data(self, table_name: str, table_schema: list, data: tuple):
        cursor = self.conn.cursor()
        merge_query = self._prepare_merge_query(cursor, table_name, table_schema, data)

        cursor.execute(merge_query)
        self._logger.info(f'Affected rows: {cursor.rowcount}')
        self.conn.commit()

    def overwrite_data(self, table_name: str, table_schema: list, data: Union[list, dict]):
        cursor = self.conn.cursor()
        cursor.execute(f'TRUNCATE TABLE {table_name}')
        self._logger.info(f'TABLE {table_name} IS TRUNCATED!')

        insert_query = self._prepare_insert_query(cursor, table_name, table_schema, data)
        cursor.execute(insert_query)
        self._logger.info(f'Affected rows: {cursor.rowcount}')
        self.conn.commit()

    def sync_data_to_postgres(self, load_type: str, table_name: str, table_schema: list, data: Union[list, dict]):
        is_exist = self.check_table_exists(table_name)
        if is_exist:
            self._logger.info(f'Table {table_name} exists!')
        else:
            self.create_new_table(table_name, table_schema)

        # convert data to tuple
        if isinstance(data, dict):
            final_data = [tuple(data.values())]
        elif isinstance(data, list):
            final_data = [tuple(x.values()) for x in data]

        if load_type == 'merge':
            self.merge_data(table_name, table_schema, final_data)
        elif load_type == 'replace':
            self.overwrite_data(table_name, table_schema, final_data)
        else:
            raise NotImplementedError(f'Load type {load_type} is not implemented!')

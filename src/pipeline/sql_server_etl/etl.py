import math
import os
import time
from datetime import datetime

import pandas as pd

from src import utils
from src.utils.metadata import ETLMetadata
from src.utils.gsheet import GoogleSheet
from src.utils.rbms.postgresdb import PostgresDB
from src.utils.rbms.sql_server import SqlServerDB

class SqlServerETL:

    local_folder = 'staging/sql_server'
    gsheet_id = os.getenv('GOOGLE_SHEET_METADATA_ID')
    batch_size = 10000

    def __init__(self, args):
        self._logger = utils.logging.getLogger(self.__class__.__name__)
        self.job_name = args.job_name
        self.gsheet_client = GoogleSheet(self.gsheet_id)
        self.metadata = ETLMetadata(self.job_name)
        self._get_dwh_info()

    def _get_dwh_info(self):
        df_servers = self.gsheet_client.get_data_from_sheet('server', start='A1')
        self.dwh_info = (df_servers[df_servers['server_name']=='dwh']).to_dict('records')[0]

    def _initialize_dwh_client(self):
        self._logger.info(f'Initialize connection to DWH')
        self.dwh_client = PostgresDB(
            host=self.dwh_info['host'],
            port=int(self.dwh_info['port']),
            user=self.dwh_info['username'],
            password=self.dwh_info['password'],
            database=self.dwh_info['database'],
            schema=self.dwh_info['db_schema']
        )

    def _initialize_source_db_client(self):
        self._logger.info(f'Initialize connection to {self.metadata.server_info["host"]}')
        self.source_db_client = SqlServerDB(
            host=self.metadata.server_info['host'],
            port=int(self.metadata.server_info['port']),
            user=self.metadata.server_info['username'],
            password=self.metadata.server_info['password'],
            database=self.metadata.db_name,
            catalog=self.metadata.server_info['db_schema']
        )

    def _prepare_local_data_folder(self):
        current_date = datetime.utcnow().strftime(utils.constants.DATE_FORMAT)
        self.data_folder = os.path.join(
            self.local_folder,
            self.metadata.server_name,
            self.metadata.db_name,
            self.metadata.job_name,
            current_date
        )
        os.makedirs(self.data_folder, exist_ok=True)

    def _update_schema_type(self, column_name: str, value: str):
        for idx, col in enumerate(self._schema):
            if col.get('name') == column_name:
                self._schema[idx]['type'] == value

    def _save_to_local(self, df: pd.DataFrame):
        filename = str(int(time.time())) + '.json'
        full_file_path = os.path.join(self.data_folder, filename)

        # sort columns follow order in schema if present
        columns = [x.get('name') for x in self._schema]
        for col in columns:
            if col not in df.columns:
                df[col] = None

        df = df[columns]

        # string column
        string_columns = [x.get('name') for x in self._schema if x.get('type')=='VARCHAR']
        if string_columns:
            df[string_columns] = df[string_columns].astype(str)

        df.to_json(full_file_path, orient='records', date_format='iso', lines=True)

    def _prepare_queries(self):
        total_records = self.source_db_client.get_total_records(self.metadata.table_name)
        self._logger.info(f'Total records: {total_records}')
        total_batches = math.ceil(total_records/self.batch_size)
        queries = []
        offset = 0
        for i in range(total_batches):
            query = """
            SELECT *
            FROM {db_name}.{catalog}.{table}
            """.format(
                db_name=self.metadata.db_name,
                catalog=self.metadata.server_info['db_schema'],
                table=self.metadata.table_name
            )
            if self.metadata.max_column:
                query += f"""
                ORDER BY {self.metadata.max_column}
                OFFSET {offset} ROWS
                FETCH NEXT {self.batch_size} ROWS ONLY
                """

            queries.append(query)
            offset += self.batch_size

        return queries

    def _extract_data_from_source_db(self):
        queries = self._prepare_queries()
        self._logger.info(f'Total queries to run: {len(queries)}')
        self._logger.info(f'Batch size per query: {self.batch_size} records')
        for query in queries:
            result = self.source_db_client.get_data_from_db(query)
            if result.shape[0] > 0:
                self._logger.info(f'Received {result.shape[0]} records')
                self._save_to_local(result)

    def _prepare_schema(self):
        if self.metadata.schema:
            self._schema = self.metadata.schema
        else:
            self._schema = self.source_db_client.get_schema_from_source_table(self.metadata.table_name)

    def _load_data_to_dwh(self):
        self._logger.info('Load data to DWH')
        self._initialize_dwh_client()
        is_exist = self.dwh_client.check_table_exists(self.metadata.destination_table)

        if is_exist:
            self._logger.info(f'Table {self.metadata.destination_table} exists!')
        else:
            self.dwh_client.create_new_table(self.metadata.destination_table, self._schema)

        if self.metadata.job_type == 'replace':
            self.dwh_client.truncate_table(self.metadata.destination_table)

        files = utils.get_all_files_in_folder(self.data_folder, '*.json')
        try:
            for file in files:
                df = pd.read_json(file, lines=True)
                data = df.to_dict(orient='records')

                self.dwh_client.sync_data_to_postgres(
                    load_type=self.metadata.job_type,
                    table_name=self.metadata.destination_table,
                    table_schema=self._schema,
                    data=data
                )

                # remove file after syncing
                os.remove(file)

        except Exception as e:
            raise Exception(e)
        finally:
            self.dwh_client.close_connection()

    def execute(self):
        try:
            self._initialize_source_db_client()
            self._prepare_local_data_folder()
            self._prepare_schema()
            self._extract_data_from_source_db()
            self._load_data_to_dwh()
        except Exception as e:
            self._logger.error(e)
            raise
        finally:
            self.source_db_client.close_connection()
            self.dwh_client.close_connection()

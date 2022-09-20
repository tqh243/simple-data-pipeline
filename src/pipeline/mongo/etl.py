import configparser
from datetime import datetime
import os
import time

import pandas as pd

from src import utils
from src.utils import constants
from src.utils.mongodb.utils import MongoJobConfig, MongoHandler
from src.utils.helpers.normalizer import NoSqlNormalizer
from src.utils.rbms.postgresdb import PostgresDB
from src.utils.gsheet import GoogleSheet


class MongoETLJob:

    local_folder = 'staging/mongo'
    gsheet_id = os.getenv('GOOGLE_SHEET_METADATA_ID')

    def __init__(self, args):
        self._logger = utils.logging.getLogger(self.__class__.__name__)
        self._job_config = MongoJobConfig(args)
        self._normalizer = NoSqlNormalizer(self._job_config.schema)
        self._mongo_handler = None

        self.gsheet_client = GoogleSheet(self.gsheet_id)

    def _get_dwh_info(self):
        df_servers = self.gsheet_client.get_data_from_sheet('server', start='A1')
        self.dwh_info = (df_servers[df_servers['server_name']=='dwh']).to_dict('records')[0]

    def _extract_data(self):
        self._logger.info(f'Extracting data from server {self._job_config.server_info.get("host")}')
        self._mongo_handler = MongoHandler(self._logger, self._job_config.server_info)
        self._mongo_handler.get_db(self._job_config.db_name)
        self._mongo_handler.get_collection(self._job_config.collection)

        self._extract_data_full()

    def _extract_data_full(self):
        self._logger.info(f'Load full data of collection {self._job_config.collection} from db {self._job_config.db_name}')
        collection_stats = self._mongo_handler.get_collection_stats(self._job_config.collection)
        total_records = collection_stats.get('total_records')
        self._logger.info(f'Total records: {total_records}')

        queries = []
        if self._job_config.max_column == '_id':
            queries = self._mongo_handler.prepare_queries(collection_stats)

        if queries:
            for idx, query in enumerate(queries, start=1):
                self._logger.info(f'[{idx}/{len(queries)}]Processing for batch {idx}')
                self._mongo_handler.find(
                    filter_=query['query'],
                    projection=self._job_config.projection,
                    sort=query['sort'],
                    limit=query['limit'],
                    offset=query['offset']
                )
                self._process_data()
        else:
            self._mongo_handler.find({}, self._job_config.projection)
            self._process_data()

    def _process_data(self):
        result = self._mongo_handler.result
        if len(result) > 0:
            self._normalizer.normalize_data(result)
            self._data = self._normalizer.result
            self._save_to_local()
            self._has_new_data = True

    def _prepare_local_data_folder(self):
        current_date = datetime.utcnow().strftime(constants.DATE_FORMAT)
        self.data_folder = os.path.join(
            self.local_folder,
            self._job_config.server_name,
            self._job_config.db_name,
            self._job_config.job_name,
            current_date
        )
        os.makedirs(self.data_folder, exist_ok=True)

    def _save_to_local(self):
        df = pd.DataFrame(self._data)
        filename = str(int(time.time())) + '.json'
        full_file_path = os.path.join(self.data_folder, filename)


        # sort columns follow order in schema
        columns = [x.get('name') for x in self._job_config.schema]
        for col in columns:
            if col not in df.columns:
                df[col] = None

        df = df[columns]
        df.to_json(full_file_path, orient='records', lines=True)

    def _get_time_boundary(self):
        timeformat = self._job_config.timeformat

        if self._job_config.start_date is None and self._job_config.end_date is None:
            if timeformat == 'seconds':
                self._min_offset = int(self._job_config.max_value)
                self._now = int(time.time())
            elif timeformat == 'milliseconds':
                self._min_offset = int(self._job_config.max_value)
                self._now = int(time.time() * 1000)
                self._incremental_range *= 100

        else:
            self._min_offset = int(self._job_config.start_date)
            self._now = int(self._job_config.end_date)

    def _initialize_dwh_client(self):
        self.dwh_client = PostgresDB(
            host=self.dwh_info['host'],
            port=int(self.dwh_info['port']),
            user=self.dwh_info['username'],
            password=self.dwh_info['password'],
            database=self.dwh_info['database'],
            schema=self.dwh_info['db_schema']
        )

    def _load_data_to_dwh(self):
        self._logger.info('Load data to DWH')
        self._initialize_dwh_client()
        files = utils.get_all_files_in_folder(self.data_folder, '*.json')
        try:
            for file in files:
                df = pd.read_json(file, lines=True)
                data = df.to_dict(orient='records')

                self.dwh_client.sync_data_to_postgres(
                    load_type=self._job_config.job_type,
                    table_name=self._job_config.destination_table,
                    table_schema=self._job_config.schema,
                    data=data
                )

                # remove file after syncing
                os.remove(file)

        except Exception as e:
            raise Exception(e)
        finally:
            self.dwh_client.close_connection()

    def _etl_data(self):
        try:
            self._prepare_local_data_folder()
            self._extract_data()
            self._load_data_to_dwh()
        except Exception:
            raise

    def main(self):
        self._etl_data()

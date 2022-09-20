import math
from datetime import datetime

from pymongo import MongoClient

from src.utils.metadata import ETLMetadata

max_type_converter = {
    'INTEGER': int,
    'STRING': str,
    'TIMESTAMP': datetime.fromtimestamp
}


class MongoHandler:

    limit_size_per_query_extract = 20*2**20 # 20MB in bytes (1 MB = 2^20 bytes)

    mongodb_sort_type = {
        'asc': 1,
        'desc': -1
    }

    def __init__(self, logger, server_info):
        self._logger = logger
        self._server_info = server_info
        self._db = None
        self._collection = None
        self._result = None

    def get_db(self, db_name):
        driver_type = 'mongodb'
        if self._server_info.get('mongo_srv') == True:
            driver_type = 'mongodb+srv'

        uri = "{driver_type}://{username}:{password}@{host}:{port}/{authentication_db}".format(
            driver_type=driver_type,
            username=self._server_info.get('username'),
            password=self._server_info.get('password'),
            host=self._server_info.get('host'),
            port=int(self._server_info.get('port')),
            authentication_db=self._server_info.get('mongo_authentication_db'),
        )

        client = MongoClient(uri, serverSelectionTimeoutMS=10000)
        self._db = client[db_name]

    def get_collection_stats(self, collection: str):
        collection_stats = self._db.command('collstats', collection)
        limit_records_per_query = math.floor(self.limit_size_per_query_extract/int(collection_stats.get('avgObjSize')))

        return {
            'total_records': collection_stats.get('count'),
            'avg_object_size': collection_stats.get('avgObjSize'),
            'limit_records_per_query': limit_records_per_query
        }

    def prepare_queries(self, collection_stats: dict):
        queries = []
        limit_records = collection_stats.get('limit_records_per_query')
        offset = 0

        query = {}
        total_records = collection_stats.get('total_records')
        sort_by = [('_id', self.mongodb_sort_type['asc'])]

        # calculate total queries
        total_queries = math.ceil(total_records/limit_records)

        for i in range(total_queries):
            query_info = {
                'query': query,
                'limit': limit_records,
                'offset': offset,
                'sort': sort_by,
            }
            queries.append(query_info)
            offset += limit_records

        return queries

    def find(
        self,
        filter_: dict = None,
        projection: dict = None,
        sort: list = [],
        limit: int = 0,
        offset: int = 0,
    ):
        if self._collection is not None:
            self._logger.info(f'Query for data.')
            self._logger.info(f'filter: {filter_}')
            self._logger.info(f'projection: {projection}')
            self._logger.info(f'limit: {limit}')
            filter_ = filter_ if filter_ else {}
            self._result = list(self._collection.find(filter_, projection, sort=sort, limit=limit, skip=offset))
            self._logger.info(f'Received {len(self._result)} records.')
        else:
            err_msg = "Collection is not set yet."
            self._logger.error(err_msg)
            raise Exception(err_msg)

    def count(self, filter_, key_col):
        if self._collection:
            self._logger.info(f"Count over {key_col}")
            filter_ = filter_ if filter_ else {}
            self._result = list(self._collection.group(
                key=key_col,
                condition=filter_
            ))
        else:
            err_msg = "Collection is not set yet."
            self._logger.error(err_msg)
            raise Exception(err_msg)

    def get_collection(self, collection_id):
        self._collection = self._db.get_collection(collection_id)

    @property
    def result(self):
        return self._result


class MongoJobConfig:
    def __init__(self, args):
        self.job_name = args.JOB_NAME
        self._do_reload = args.RELOAD
        self.metadata = ETLMetadata(self.job_name)
        self._get_projection()

    def _get_projection(self):
        self._projection = {}
        ignore_fields = []
        if self.metadata.ignore_fields:
            ignore_fields = list(set(list(map(str.strip, str(self.metadata.ignore_fields).split(',')))))

        if ignore_fields:
            for field in ignore_fields:
                self._projection[field] = 0

        col_list = [col['name'] for col in self.metadata.schema]
        if col_list:
            for col in col_list:
                self._projection[col] = 1
        else:
            self._projection = None

    @property
    def server_name(self):
        return self.metadata.server_name

    @property
    def server_info(self):
        return self.metadata.server_info

    @property
    def db_name(self):
        return self.metadata.db_name

    @property
    def collection(self):
        return self.metadata.table_name

    @property
    def job_type(self):
        return self.metadata.job_type

    @property
    def destination_table(self):
        return self.metadata.destination_table

    @property
    def key_columns(self):
        return self.metadata.key_columns

    @property
    def max_column(self):
        return self.metadata.max_column

    @property
    def projection(self):
        return self._projection

    @property
    def schema(self):
        return self.metadata.schema

    @property
    def limit(self):
        return 1000

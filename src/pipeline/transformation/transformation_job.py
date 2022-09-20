import os

from src import utils
from src.utils.gsheet import GoogleSheet
from src.utils.rbms.postgresdb import PostgresDB

class TransformationJob:

    gsheet_id = os.getenv('GOOGLE_SHEET_METADATA_ID')
    sheet_name = 'transformation'
    sheet_start = 'A2'

    def __init__(self, args):
        self._logger = utils.logging.getLogger(self.__class__.__name__)
        self.job_name = args.job_name
        self.gsheet_client = GoogleSheet(self.gsheet_id)

    def _get_metadata(self):
        metadata = self.gsheet_client.get_data_from_sheet(self.sheet_name, start=self.sheet_start)
        job_metadata = (metadata[metadata['job_name']==self.job_name]).to_dict('records')
        if job_metadata:
            self.job_config = job_metadata[0]

    def _get_dwh_info(self):
        df_servers = self.gsheet_client.get_data_from_sheet('server', start='A1')
        self.dwh_info = (df_servers[df_servers['server_name']=='dwh']).to_dict('records')[0]

    def _initialize_dwh_client(self):
        self.dwh_client = PostgresDB(
            host=self.dwh_info['host'],
            port=int(self.dwh_info['port']),
            user=self.dwh_info['username'],
            password=self.dwh_info['password'],
            database=self.dwh_info['database'],
            schema=self.dwh_info['db_schema']
        )

    def execute(self):
        self._get_metadata()
        self._get_dwh_info()
        self._initialize_dwh_client()

        source_table_name = self.job_config['source_table_name']
        destination_table_name = self.job_config['destination_table_name']
        primary_key = self.job_config.get('primary_key')
        try:
            if self.job_config['mode'] == 'merge':
                query = self.dwh_client.build_merge_query_from_source_table(
                    source_table_name=source_table_name,
                    destination_table_name=destination_table_name,
                    primary_key=primary_key
                )
            elif self.job_config['mode'] == 'replace':
                query = self.dwh_client.build_replace_query_from_source_table(
                    source_table_name=source_table_name,
                    destination_table_name=destination_table_name
                )
            else:
                raise NotImplementedError

            self._logger.info(f'Query: \n{query}')
            self.dwh_client.cursor.execute(query)
            self._logger.info(f'Table {destination_table_name} sync successfully!')
            self._logger.info(f'Affected rows: {self.dwh_client.cursor.rowcount}')

            if self.job_config['mode'] == 'replace' and primary_key:
                self.dwh_client.cursor.execute(f"""ALTER TABLE {self.dwh_info["db_schema"]}.{destination_table_name} ADD PRIMARY KEY ({primary_key})""")

            self.dwh_client.conn.commit()

        except Exception as e:
            self._logger.error(e)
            raise
        finally:
            self.dwh_client.close_connection()

import os
from datetime import datetime
import json
from time import sleep
from ast import literal_eval

import numpy as np
import pandas as pd

from pytz import timezone
from requests.exceptions import HTTPError
from socket import error as SocketError

from src.utils import logging
from src.utils.gsheet import GoogleSheet


HCM_TZ = timezone('Asia/Ho_Chi_Minh')

# ignore pandas warning
pd.options.mode.chained_assignment = None

class GsheetConnectionResetError(Exception):
    def __init__(self, msg):
        super().__init__(msg)


class _Metadata:

    metadata_sheet_id = os.getenv('GOOGLE_SHEET_METADATA_ID')
    pipeline_sheet_name = 'pipeline'
    server_sheet_name = 'server'

    def __init__(self, job_name):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._job_name = None
        # Configure working sheets
        self._working_sheet = None
        self._all_metadata_df = None
        self._job_metadata_df = None
        self._metadata = None
        self._end_cell = None
        self.gsheet_client = GoogleSheet(gsheet_id=self.metadata_sheet_id)

        retry = 0
        while True:
            try:
                self._get_metadata_sheet()
                self._get_job_metadata(job_name)
                self._get_source_server_info()
                break
            except HTTPError as e:
                if e.errno == 429 and retry <= 1:
                    retry += 1
                    self._logger.warning(f"retry #{retry}: limit reached. Sleep 100s and retry")
                    sleep(100)
                else:
                    self._logger.error(f"Other HTTPError: {e}")
                    raise
            except Exception as e:
                self._logger.error(e)
                raise

    def _get_end_cell(self, end_cell_storage_addr):
        end_cell = self._working_sheet.get_value(end_cell_storage_addr)
        if end_cell and end_cell.find('end_cell=') != -1:
            try:
                self._end_cell = tuple([int(x) for x in end_cell.split('=')[1].split(',')])
            except Exception:
                self._end_cell = None
        else:
            self._end_cell = None

    def _get_metadata_sheet(self):
        if self._working_sheet is None:
            self._logger.info(f"Connect to Metadata's gsheet")
            self._working_sheet = self.gsheet_client.get_working_sheet_by_title(self.pipeline_sheet_name)

    def _get_job_metadata(self, job_name, start_cell='A2', end_cell_storage_cell=None):
        self._job_name = job_name
        self._logger.info(f"Get metadata for {self._job_name}")
        if end_cell_storage_cell:
            self._get_end_cell(end_cell_storage_cell)
        self._all_metadata_df = self._working_sheet.get_as_df(start=start_cell, end=self._end_cell)

        try:
            self._job_metadata_df = self._all_metadata_df[self._all_metadata_df['job_name'] == self._job_name]
            df = self._job_metadata_df.apply(lambda x: str(x).strip() if isinstance(x, str) else x).replace('', np.nan)
            df = df.replace({np.nan: None})
            self._metadata = df.to_dict('records')[0]  # Get the first result
        except IndexError:
            err_msg = f"Job {self._job_name} not found in {self.pipeline_sheet_name}."
            self._logger.error(err_msg)
            raise ValueError(err_msg)
        except Exception:
            raise

    def _get_source_server_info(self):
        df_servers = self.gsheet_client.get_data_from_sheet(self.server_sheet_name, start='A1')
        self._server_info = (df_servers[df_servers['server_name']==self.server_name]).to_dict('records')[0]

    @property
    def job_name(self):
        return self._metadata.get('job_name')

    @property
    def server_name(self):
        return self._metadata.get('server_name')

    @property
    def db_name(self):
        return self._metadata.get('db_name')

    @property
    def table_name(self):
        return self._metadata.get('table_name')

    @property
    def job_type(self):
        return self._metadata.get('job_type')

    @property
    def destination_table(self):
        return self._metadata.get('destination_table')

    @property
    def server_info(self):
        return self._server_info


class ETLMetadata(_Metadata):
    def __init__(self, job_name):
        super().__init__(job_name)

    @property
    def key_columns(self):
        return self._metadata.get('key_columns')

    @property
    def max_column(self):
        return self._metadata.get('max_column')

    @property
    def ignore_fields(self):
        return self._metadata.get('ignore_fields')

    @property
    def schema(self):
        if self._metadata.get('schema'):
            return json.loads(self._metadata.get('schema'))
        else:
            return None

    def get_job_metadata(self, job_name):
        start_cell = 'A2'
        super()._get_job_metadata(job_name, start_cell)

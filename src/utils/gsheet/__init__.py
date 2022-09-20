import os
from typing import Optional

import pygsheets
import numpy as np

CREDENTIAL_FILES = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

class GoogleSheet:
    def __init__(self, gsheet_id):
        self._gsheet_id = gsheet_id
        self._get_gsheet()
        self._working_sheet = None

    def _get_gsheet(self):
        gsheet_client = pygsheets.authorize(service_account_file=CREDENTIAL_FILES)
        self._spreadsheet = gsheet_client.open_by_key(self._gsheet_id)

    def get_working_sheet_by_title(self, sheet_name):
        self._working_sheet = self._spreadsheet.worksheet_by_title(sheet_name)
        return self._working_sheet

    def get_data_from_sheet(self, sheet_name: str, start: str, id_columns: Optional[list] = []):
        working_sheet = self.get_working_sheet_by_title(sheet_name)
        df = working_sheet.get_as_df(start=start)
        df = df.apply(lambda x: str(x).strip() if isinstance(x, str) else x).replace('', np.nan)
        df = df.replace({np.nan: None})

        if id_columns:
            df.dropna(subset=id_columns, inplace=True)

        return df

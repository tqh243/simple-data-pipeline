import os
import logging

ENVIRONMENT = os.getenv('ENV')

data_pipeline_chat_id = os.getenv('TELEGRAM_ALERT_GROUP_CHAT_ID')

class OutputInfo:
    JOB_SUCCESS = 'SUCCESS'
    JOB_FAIL = 'ERROR'

    def __init__(self, job_type, job_name: str=None):
        self.status = self.JOB_SUCCESS
        self.job_type = job_type
        self.job_name = job_name
        self._error_msg = ''

    @property
    def error_msg(self):
        return self._error_msg

    @error_msg.setter
    def error_msg(self, message: str):
        if message:
            clean_msg = message[:2000]
            idx = clean_msg.find('-----Query Job SQL Follows-----')
            if idx >= 0:
                clean_msg = clean_msg[:idx]

            self._error_msg = clean_msg.strip()
        else:
            self._error_msg = 'No error message.'

def handle_output(output_info: OutputInfo):
    try:
        if output_info.status == output_info.JOB_FAIL:
            error_msg = f"[ERROR] {output_info.job_type} - {output_info.job_name}: \n{output_info.error_msg}"
            if ENVIRONMENT == 'PROD':
                print(error_msg, data_pipeline_chat_id, parse_mode='html')
    except Exception as e:
        logging.error(e)

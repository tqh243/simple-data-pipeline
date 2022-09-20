import os
import logging

ENVIRONMENT = os.getenv('ENV')


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

def handle_output(output_info: OutputInfo):
    try:
        if output_info.status == output_info.JOB_FAIL:
            error_msg = f"[ERROR] {output_info.job_type} - {output_info.job_name}: \n{output_info.error_msg}"
            print(error_msg)
    except Exception as e:
        logging.error(e)

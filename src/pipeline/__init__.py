import argparse
from src.utils.error_handler import OutputInfo, handle_output


class PipelineBaseClass:
    def __init__(self, job_name):
        self._job_name = job_name
        self._output_info = OutputInfo(self._job_name)
        self._arguments_parser = argparse.ArgumentParser()
        self._args = None
        self._declare_job_arguments()

    def _declare_job_arguments(self):
        raise NotImplementedError

    def _trigger_job(self):
        raise NotImplementedError

    def _update_output_info(self):
        raise NotImplementedError

    def run_job(self):
        self._args = self._arguments_parser.parse_args()
        self._update_output_info()
        try:
            self._trigger_job()
        except Exception as e:
            self._output_info.status = self._output_info.JOB_FAIL
            self._output_info.error_msg = str(e)
            raise
        finally:
            handle_output(self._output_info)

from src.pipeline import PipelineBaseClass
from src.pipeline.mongo.etl import MongoETLJob

class MongodbPipeline(PipelineBaseClass):
    """
    Pipeline use to get data from mongodb and stores at local.
    If start date and end date is given, data is get between [start_date, end_date].
    Else, use max_value from metadata sheet's config field

    Args:
        -j: job name. Name of job according to metadata sheet.
        -s: start date.
        -e: end date.
        -r: If set, will extract 'reload_days' from job's metadata's config field.
            Start time will be (start time - reloads day).

    """
    def __init__(self):
        super().__init__('MongoDb')

    def _declare_job_arguments(self):
        self._arguments_parser.add_argument('-j', '--JOB_NAME', required=True)
        self._arguments_parser.add_argument('-s', '--START_DATE', required=False)
        self._arguments_parser.add_argument('-e', '--END_DATE', required=False)
        self._arguments_parser.add_argument('-r', '--RELOAD', action='store_true', required=False)

    def _trigger_job(self):
        mongodb_etl = MongoETLJob(self._args)
        mongodb_etl.main()

    def _update_output_info(self):
        self._output_info.job_name = self._args.JOB_NAME


if __name__ == '__main__':
    mongodb_job = MongodbPipeline()
    mongodb_job.run_job()

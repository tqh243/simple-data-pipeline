from src.pipeline import PipelineBaseClass
from src.pipeline.sql_server_etl.etl import SqlServerETL

class SqlServerETLPipeline(PipelineBaseClass):
    def __init__(self):
        super().__init__('Pentamic ETL')

    def _declare_job_arguments(self):
        self._arguments_parser.add_argument('-j', '--job_name', required=True)

    def _trigger_job(self):
        sql_server_etl = SqlServerETL(self._args)
        sql_server_etl.execute()

    def _update_output_info(self):
        self._output_info.job_name = self._args.job_name


if __name__ == '__main__':
    sql_server_job = SqlServerETLPipeline()
    sql_server_job.run_job()

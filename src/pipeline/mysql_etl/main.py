from src.pipeline import PipelineBaseClass
from src.pipeline.mysql_etl.etl import MysqlETL

class MysqlETLPipeline(PipelineBaseClass):
    def __init__(self):
        super().__init__('Mysql ETL')

    def _declare_job_arguments(self):
        self._arguments_parser.add_argument('-j', '--job_name', required=True)

    def _trigger_job(self):
        mysql_etl = MysqlETL(self._args)
        mysql_etl.execute()

    def _update_output_info(self):
        self._output_info.job_name = self._args.job_name


if __name__ == '__main__':
    sql_server_job = MysqlETLPipeline()
    sql_server_job.run_job()

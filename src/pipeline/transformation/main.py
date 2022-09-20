from src.pipeline import PipelineBaseClass
from src.pipeline.transformation.transformation_job import TransformationJob

class TransformationJobPipeline(PipelineBaseClass):
    def __init__(self):
        super().__init__('Transformation')

    def _declare_job_arguments(self):
        self._arguments_parser.add_argument('-j', '--job_name', required=True)

    def _trigger_job(self):
        transformation_job = TransformationJob(self._args)
        transformation_job.execute()

    def _update_output_info(self):
        self._output_info.job_name = self._args.job_name


if __name__ == '__main__':
    tranformation_job = TransformationJobPipeline()
    tranformation_job.run_job()

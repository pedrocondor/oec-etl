from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import BasePipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep


class DownloadStep(PipelineStep):
    def run_step(self, prev_result, params):
        return self.connector.download(params=params)


class RussiaSubnationalPipeline(BasePipeline):
    @staticmethod
    def pipeline_id():
        return 'russia-subnational-pipeline'

    @staticmethod
    def name():
        return 'Russia Sub-national Pipeline'

    @staticmethod
    def description():
        return 'Processes Russia sub-national data'

    @staticmethod
    def website():
        return 'http://datawheel.us'

    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Source connector", name="source_connector", dtype=str, source=Connector),
            Parameter(label="DB connector", name="db_connector", dtype=str, source=Connector),
        ]

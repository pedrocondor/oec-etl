import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import AdvancedPipelineExecutor
from bamboo_lib.models import BasePipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import LoadStep
from bamboo_lib.steps import UnzipStep


class DownloadStep(PipelineStep):
    def run_step(self, prev_result, params):
        return self.connector.download(params=params)


class ExtractStep(PipelineStep):
    def run_step(self, prev, params):
        return pd.read_csv(prev)


class YearlyMetaCountriesPipeline(BasePipeline):
    @staticmethod
    def pipeline_id():
        return 'baci-yearly-meta-countries-pipeline'

    @staticmethod
    def name():
        return 'BACI Yearly Meta Countries Pipeline'

    @staticmethod
    def description():
        return 'Processes BACI yearly meta countries data'

    @staticmethod
    def website():
        return 'http://datawheel.us'

    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Source connector", name="source_connector", dtype=str, source=Connector),
            Parameter(label="DB connector", name="db_connector", dtype=str, source=Connector),
        ]

    @staticmethod
    def run(params, **kwargs):
        source_connector = Connector.fetch(params.get("source_connector"), open("etl/conns.yaml"))
        db_connector = Connector.fetch(params.get("db_connector"), open("etl/conns.yaml"))

        dtype = {
            'id':            'String',
            'id_num':        'UInt32',
            'iso3':          'String',
            'iso2':          'String',
            'continent':     'String',
            'color':         'String',
            'name':          'String',
            'export_val':    'Float64'
        }

        download_data = DownloadStep(connector=source_connector)
        unzip_step = UnzipStep(pattern=r"\.csv$")
        extract_step = ExtractStep()

        # TODO: What are all the other options
        # load_step = LoadStep("oec_yearly_meta_countries", db_connector, if_exists="append", pk=["id"])

        pp = AdvancedPipelineExecutor(params)
        pp = pp.next(download_data).next(extract_step)#.next(load_step)

        return pp.run_pipeline()


if __name__ == '__main__':
    pipeline = YearlyMetaCountriesPipeline()
    pipeline.run({
        'source_connector': 'baci-yearly-meta-countries',
        'db_connector': 'clickhouse-remote',
    })

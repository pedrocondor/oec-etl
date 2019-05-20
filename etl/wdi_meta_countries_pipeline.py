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
        names = [
            'country_code', 'short_name', 'table_name', 'long_name',
            'two_alpha_code', 'currency_unit', 'special_notes', 'region',
            'income_group', 'wb_2_code'
        ]

        df = pd.read_csv(prev, header=0, names=names)

        df['special_notes'] = df['special_notes'].fillna('').astype(str)
        df['wb_2_code'] = df['wb_2_code'].fillna('').astype(str)

        return df


class WDIMetaCountriesPipeline(BasePipeline):
    @staticmethod
    def pipeline_id():
        return 'wdi-meta-countries-pipeline'

    @staticmethod
    def name():
        return 'WDI Meta Countries Pipeline'

    @staticmethod
    def description():
        return 'Processes WDI meta countries data'

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
            'country_code':      'String',
            'short_name':        'String',
            'table_name':        'String',
            'long_name':         'String',
            'two_alpha_code':    'String',
            'currency_unit':     'String',
            'special_notes':     'String',
            'region':            'String',
            'income_group':      'String',
            'wb_2_code':         'String'
        }

        download_data = DownloadStep(connector=source_connector)
        unzip_step = UnzipStep(pattern=r"\.csv$")
        extract_step = ExtractStep()

        # TODO: What are all the other options
        # load_step = LoadStep("oec_wdi"), db_connector, if_exists="append", pk=["id"])

        pp = AdvancedPipelineExecutor(params)
        pp = pp.next(download_data).next(extract_step)#.next(load_step)

        return pp.run_pipeline()


if __name__ == '__main__':
    pipeline = WDIMetaCountriesPipeline()
    pipeline.run({
        'source_connector': 'wdi-series',
        'db_connector': 'clickhouse-remote',
    })
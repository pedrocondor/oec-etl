import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import AdvancedPipelineExecutor
from bamboo_lib.models import BasePipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import LoadStep


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

        # TODO: Deal with new columns in the original file now

        return df


class DimWDICountriesPipeline(BasePipeline):
    @staticmethod
    def pipeline_id():
        return 'wdi-countries-dimension-pipeline'

    @staticmethod
    def name():
        return 'WDI Countries Dimension Pipeline'

    @staticmethod
    def description():
        return 'Processes WDI countries data'

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
        extract_step = ExtractStep()
        load_step = LoadStep(
            "dim_shared_wdi_countries", db_connector, if_exists="append", dtype=dtype,
            pk=['country_code']
        )

        pp = AdvancedPipelineExecutor(params)
        pp = pp.next(download_data).next(extract_step).next(load_step)

        return pp.run_pipeline()


if __name__ == '__main__':
    pipeline = DimWDICountriesPipeline()
    pipeline.run({
        'source_connector': 'wdi-country',
        'db_connector': 'clickhouse-remote',
    })

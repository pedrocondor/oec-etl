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
            'year', 'product_category', 'exporter', 'importer',
            'value_thou_us_dollars', 'qty_tons'
        ]

        df = pd.read_csv(prev, header=0, names=names)
        df['value_us_dollars'] = df['value_thou_us_dollars'] * 1000

        return df


class YearlyPipeline(BasePipeline):
    @staticmethod
    def pipeline_id():
        return 'baci-yearly-pipeline'

    @staticmethod
    def name():
        return 'BACI Yearly Pipeline'

    @staticmethod
    def description():
        return 'Processes BACI yearly data'

    @staticmethod
    def website():
        return 'http://datawheel.us'

    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Source connector", name="source_connector", dtype=str, source=Connector),
            Parameter(label="DB connector", name="db_connector", dtype=str, source=Connector),
            Parameter(label="HS Code", name="hs_code", dtype=str),
            Parameter(label="Year", name="year", dtype=str),
        ]

    @staticmethod
    def run(params, **kwargs):
        source_connector = Connector.fetch(params.get("source_connector"), open("etl/conns.yaml"))
        db_connector = Connector.fetch(params.get("db_connector"), open("etl/conns.yaml"))

        dtype = {
            'year': 'UInt32',
            'product_category': 'String',
            'exporter': 'UInt32',
            'importer': 'UInt32',
            'value_thou_us_dollars': 'Float64',
            'qty_tons': 'Float64',
            'value_us_dollars': 'Float64'
        }

        download_data = DownloadStep(connector=source_connector)
        unzip_step = UnzipStep(pattern=r"\.csv$")
        extract_step = ExtractStep()

        # TODO: What are all the other options
        # load_step = LoadStep("oec_yearly_{}".format(params['hs_code']), db_connector, if_exists="append", pk=["id"])

        pp = AdvancedPipelineExecutor(params)
        pp = pp.next(download_data).foreach(unzip_step).next(extract_step)#.next(load_step)

        return pp.run_pipeline()


if __name__ == '__main__':
    pipeline = YearlyPipeline()
    pipeline.run({
        'source_connector': 'baci-yearly',
        'db_connector': 'clickhouse-remote',
        'hs_code': '07',
        'year': '2016'
    })

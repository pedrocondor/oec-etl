import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import AdvancedPipelineExecutor
from bamboo_lib.models import BasePipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import LoadStep
from bamboo_lib.steps import UnzipStep

from etl.util import hs6_converter


class DownloadStep(PipelineStep):
    def run_step(self, prev_result, params):
        return self.connector.download(params=params)


class ExtractStep(PipelineStep):
    def run_step(self, prev, params):
        names = [
            'year', 'product_category', 'exporter', 'importer',
            'trade_value_thou_us_dollars', 'qty_tons'
        ]

        df = pd.read_csv(prev, header=0, names=names)

        df['trade_value_us_dollars'] = df['trade_value_thou_us_dollars'] * 1000
        df['product_category'] = df['product_category'].astype(str).apply(lambda x: int(hs6_converter(x.zfill(6))))

        return df


class BACIAnnualTradePipeline(BasePipeline):
    @staticmethod
    def pipeline_id():
        return 'baci-annual-trade-pipeline'

    @staticmethod
    def name():
        return 'BACI Annual Trade Pipeline'

    @staticmethod
    def description():
        return 'Processes BACI annual trade data'

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
            'product_category': 'UInt32',
            'exporter': 'UInt32',
            'importer': 'UInt32',
            'trade_value_thou_us_dollars': 'Float64',
            'trade_value_us_dollars': 'Float64',
            'qty_tons': 'Float64',
        }

        download_data = DownloadStep(connector=source_connector)
        unzip_step = UnzipStep(pattern=r"\.csv$")
        extract_step = ExtractStep()

        load_step = LoadStep(
            "trade_i_cepii_a_{}".format(params['hs_code']), db_connector, if_exists="append", dtype=dtype,
            pk=['exporter', 'importer', 'year'], nullable_list=['qty_tons']
        )

        pp = AdvancedPipelineExecutor(params)
        pp = pp.next(download_data).foreach(unzip_step).next(extract_step).next(load_step)

        return pp.run_pipeline()


if __name__ == '__main__':
    pipeline = BACIAnnualTradePipeline()

    for hs_code in ['92', '96', '02', '07']:
        if hs_code == '92':
            years = list(range(1995, 2016 + 1))
        else:
            years = [2016]

        for year in years:
            pipeline.run({
                'source_connector': 'baci-yearly',
                'db_connector': 'clickhouse-remote',
                'hs_code': hs_code,
                'year': year
            })

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
            'classification', 'year', 'period', 'period_desc', 'aggregate_level',
            'is_leaf_id', 'trade_flow_id', 'trade_flow', 'reporter_id',
            'reporter', 'reporter_iso', 'partner_id', 'partner', 'partner_iso',
            'service_id', 'service', 'trade_value_us_dollars', 'flag'
        ]

        df = pd.read_csv(prev, header=0, names=names)

        df = df.drop(df[df['partner'] == 'World'].index)
        df = df.dropna(subset=['partner_id', 'trade_value_us_dollars'])

        # TODO: Where is the dimension table?
        #       Or do I just build it from the fact table?
        #       aggregate_level would be a property of the dimension table

        remove_names = [
            'period', 'classification', 'is_leaf_id', 'aggregate_level',
            'service', 'flag', 'period_desc', 'reporter_iso', 'partner_iso',
            'reporter', 'partner', 'trade_flow'
        ]

        for name in remove_names:
            names.remove(name)

        df = df[names]

        return df


class ComtradeAnnualServicesPipeline(BasePipeline):
    @staticmethod
    def pipeline_id():
        return 'comtrade-annual-services-pipeline'

    @staticmethod
    def name():
        return 'Comtrade Annual Services Pipeline'

    @staticmethod
    def description():
        return 'Processes Comtrade annual services data'

    @staticmethod
    def website():
        return 'http://datawheel.us'

    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Source connector", name="source_connector", dtype=str, source=Connector),
            Parameter(label="DB connector", name="db_connector", dtype=str, source=Connector),
            Parameter(label="Year", name="year", dtype=str),
        ]

    @staticmethod
    def run(params, **kwargs):
        source_connector = Connector.fetch(params.get("source_connector"), open("etl/conns.yaml"))
        db_connector = Connector.fetch(params.get("db_connector"), open("etl/conns.yaml"))

        dtype = {
            'year':                    'UInt16',
            'trade_flow_id':           'UInt8',
            'reporter_id':             'UInt32',
            'partner_id':              'UInt32',
            'service_id':              'UInt16',
            'trade_value_us_dollars':  'Int64',
        }

        download_data = DownloadStep(connector=source_connector)
        unzip_step = UnzipStep(pattern=r"\.csv$")
        extract_step = ExtractStep()
        load_step = LoadStep(
            "services_i_comtrade_a_eb02", db_connector, if_exists="append", dtype=dtype,
            pk=['reporter_id', 'trade_flow_id', 'year'],
            nullable_list=['trade_value_us_dollars']
        )

        pp = AdvancedPipelineExecutor(params)
        pp = pp.next(download_data).foreach(unzip_step).next(extract_step).next(load_step)

        return pp.run_pipeline()


if __name__ == '__main__':
    pipeline = ComtradeAnnualServicesPipeline()

    for year in range(2000, 2017 + 1):
        pipeline.run({
            'source_connector': 'comtrade-annual-services',
            'db_connector': 'clickhouse-remote',
            'year': str(year),
        })

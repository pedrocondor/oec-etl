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
            'is_leaf_code', 'trade_flow_code', 'trade_flow', 'reporter_code',
            'reporter', 'reporter_iso', 'partner_code', 'partner', 'partner_iso',
            'commodity_code_pre', 'commodity', 'qty_unit_code', 'qty_unit',
            'qty', 'netweight_kg', 'trade_value_us_dollars', 'flag'
        ]

        df = pd.read_csv(prev, encoding='ISO-8859-1')

        print(df.loc[[0]].to_dict())

        # TODO commodity_code needs to be calculated somehow

        return None


class MonthlyPipeline(BasePipeline):
    @staticmethod
    def pipeline_id():
        return 'comtrade-monthly-pipeline'

    @staticmethod
    def name():
        return 'Comtrade Monthly Pipeline'

    @staticmethod
    def description():
        return 'Processes Comtrade monthly data'

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

        download_data = DownloadStep(connector=source_connector)
        unzip_step = UnzipStep(pattern=r"\.csv$")
        extract_step = ExtractStep()

        # TODO: What are all the other options
        # load_step = LoadStep("rus_meta_region", db_connector, if_exists="append", pk=["id"])

        pp = AdvancedPipelineExecutor(params)
        pp = pp.next(download_data).foreach(unzip_step).next(extract_step)#.next(load_step)

        return pp.run_pipeline()


if __name__ == '__main__':
    pipeline = MonthlyPipeline()
    pipeline.run({
        'source_connector': 'comtrade-monthly',
        'db_connector': 'clickhouse-remote',
    })

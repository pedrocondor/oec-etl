import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import AdvancedPipelineExecutor
from bamboo_lib.models import BasePipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import LoadStep


class ExtractStep(PipelineStep):
    def run_step(self, prev, params):
        data = [
            {
                'trade_flow_id': 1,
                'trade_flow_name': 'Imports',
            },
            {
                'trade_flow_id': 2,
                'trade_flow_name': 'Exports',
            }
        ]

        return pd.DataFrame(data)


class DimTradeFlowPipeline(BasePipeline):
    @staticmethod
    def pipeline_id():
        return 'trade-flow-dimension-pipeline'

    @staticmethod
    def name():
        return 'Trade Flow Dimension Pipeline'

    @staticmethod
    def description():
        return 'Processes trade flow data'

    @staticmethod
    def website():
        return 'http://datawheel.us'

    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Source connector", name="source_connector", dtype=str, source=Connector),
            Parameter(label="DB connector", name="db_connector", dtype=str, source=Connector),
            Parameter(label="HS Code", name="hs_code", dtype=str),
        ]

    @staticmethod
    def run(params, **kwargs):
        db_connector = Connector.fetch(params.get("db_connector"), open("etl/conns.yaml"))

        dtype = {
            'trade_flow_id':       'UInt8',
            'trade_flow_name':     'String',
        }

        extract_step = ExtractStep()
        load_step = LoadStep(
            "dim_shared_trade_flow", db_connector,
            if_exists="append", dtype=dtype, pk=['trade_flow_id']
        )

        pp = AdvancedPipelineExecutor(params)
        pp = pp.next(extract_step).next(load_step)

        return pp.run_pipeline()


if __name__ == '__main__':
    pipeline = DimTradeFlowPipeline()
    pipeline.run({
        'db_connector': 'clickhouse-remote',
    })

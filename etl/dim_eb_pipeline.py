import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import AdvancedPipelineExecutor
from bamboo_lib.models import BasePipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import LoadStep
from bamboo_lib.steps import UnzipStep


SERVICES_DF = None


class DownloadStep(PipelineStep):
    def run_step(self, prev, params):
        return self.connector.download(params=params)


class ExtractStep(PipelineStep):
    def run_step(self, prev, params):
        global SERVICES_DF

        print(type(SERVICES_DF))

        names = [
            'classification', 'year', 'period', 'period_desc', 'aggregate_level',
            'is_leaf_id', 'trade_flow_id', 'trade_flow', 'reporter_id',
            'reporter', 'reporter_iso', 'partner_id', 'partner', 'partner_iso',
            'service_id', 'service', 'trade_value_us_dollars', 'flag'
        ]

        df = pd.read_csv(prev, header=0, names=names)
        df = df[['aggregate_level', 'service_id', 'service']]

        if isinstance(SERVICES_DF, pd.DataFrame):
            SERVICES_DF = SERVICES_DF.append(df, ignore_index=True)
        else:
            SERVICES_DF = df

        return df


class ConsolidationStep(PipelineStep):
    def run_step(self, prev, params):
        data = []
        service_ids = set()

        for i, row in SERVICES_DF.iterrows():
            if row['service_id'] in service_ids:
                continue

            data.append(row.to_dict())
            service_ids.add(row['service_id'])

        return pd.DataFrame(data)


class DimEBPipeline(BasePipeline):
    @staticmethod
    def pipeline_id():
        return 'eb-dimension-pipeline'

    @staticmethod
    def name():
        return 'EB Dimension Pipeline'

    @staticmethod
    def description():
        return 'Processes EB services data'

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
            'aggregate_level':         'UInt8',
            'service_id':              'UInt16',
            'service':                 'String',
        }

        download_data = DownloadStep(connector=source_connector)
        unzip_step = UnzipStep(pattern=r"\.csv$")
        extract_step = ExtractStep()
        consolidation_step = ConsolidationStep()
        load_step = LoadStep(
            "dim_shared_eb02", db_connector, if_exists="append", dtype=dtype,
            pk=['service_id'], nullable_list=['aggregate_level']
        )

        for year in range(2000, 2017 + 1):
            params['year'] = year

            pp = AdvancedPipelineExecutor(params)
            pp = pp.next(download_data).foreach(unzip_step).next(extract_step)
            pp.run_pipeline()

        pp = AdvancedPipelineExecutor(params)
        pp = pp.next(consolidation_step).next(load_step)

        return pp.run_pipeline()


if __name__ == '__main__':
    pipeline = DimEBPipeline()
    pipeline.run({
        'source_connector': 'comtrade-annual-services',
        'db_connector': 'clickhouse-remote',
    })

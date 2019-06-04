import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import AdvancedPipelineExecutor
from bamboo_lib.models import BasePipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import LoadStep


class DownloadStep(PipelineStep):
    def run_step(self, prev, params):
        return self.connector.download(params=params)


class ExtractStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv(prev)

        data = []

        for i, row in df.iterrows():
            for year in range(1960, 2018):
                data.append({
                    'geo_id': row['Country Code'].lower(),
                    'indicator_id': row['Indicator Code'],
                    'year': year,
                    'mea': row[str(year)],
                })

        final_df = pd.DataFrame(data)

        return final_df


class WDIAnnualIndicatorsPipeline(BasePipeline):
    @staticmethod
    def pipeline_id():
        return 'wdi-annual-indicators-pipeline'

    @staticmethod
    def name():
        return 'WDI Annual Indicators Pipeline'

    @staticmethod
    def description():
        return 'Processes WDI annual indicators data'

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
            'geo_id':                  'String',
            'indicator_id':            'String',
            'year':                    'UInt32',
            'mea':                     'Float64'
        }

        download_data = DownloadStep(connector=source_connector)
        extract_step = ExtractStep()
        load_step = LoadStep(
            "indicators_i_wdi_a", db_connector, if_exists="append", dtype=dtype,
            pk=['geo_id'], nullable_list=['mea']
        )

        pp = AdvancedPipelineExecutor(params)
        pp = pp.next(download_data).next(extract_step).next(load_step)

        return pp.run_pipeline()


if __name__ == '__main__':
    pipeline = WDIAnnualIndicatorsPipeline()
    pipeline.run({
        'source_connector': 'wdi-data',
        'db_connector': 'clickhouse-remote',
    })

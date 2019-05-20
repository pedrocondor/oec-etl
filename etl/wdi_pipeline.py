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
        df = pd.read_csv(prev)

        data = []

        for i, row in df.iterrows():
            for year in range(1960, 2018):
                data.append({
                    'country_name': row['Country Name'],
                    'country_code': row['Country Code'],
                    'indicator_name': row['Indicator Name'],
                    'indicator_code': row['Indicator Code'],
                    'year': year,
                    'mea': row[str(year)],
                })

        final_df = pd.DataFrame(data)

        return final_df


class WDIPipeline(BasePipeline):
    @staticmethod
    def pipeline_id():
        return 'wdi-pipeline'

    @staticmethod
    def name():
        return 'WDI Pipeline'

    @staticmethod
    def description():
        return 'Processes WDI data'

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
            'country_name':            'String',
            'country_code':            'String',
            'indicator_name':          'String',
            'indicator_code':          'String',
            'year':                    'UInt32',
            'mea':                     'Float64'
        }

        download_data = DownloadStep(connector=source_connector)
        extract_step = ExtractStep()
        load_step = LoadStep(
            "oec_wdi", db_connector, if_exists="append", dtype=dtype,
            pk=['country_code']
        )

        pp = AdvancedPipelineExecutor(params)
        pp = pp.next(download_data).next(extract_step).next(load_step)

        return pp.run_pipeline()


if __name__ == '__main__':
    pipeline = WDIPipeline()
    pipeline.run({
        'source_connector': 'wdi-data',
        'db_connector': 'clickhouse-remote',
    })

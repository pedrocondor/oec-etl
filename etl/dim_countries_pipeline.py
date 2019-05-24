import numpy as np
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

        # Belgium-Luxembourg pair does not have `iso2`
        df['iso2'] = df['iso2'].fillna('').astype(str)

        # Belgium and Luxembourg don't have individual entries, so we'll add
        # those manually
        manual_entries = [
            {
                'id': 'eubel', 'id_num': 56,
                'iso3': 'bel', 'iso2': 'be',
                'continent': 'eu',
                'color': '#752277',
                'name': 'Belgium',
                'export_val': np.nan,
            },
            {
                'id': 'eulux', 'id_num': 442,
                'iso3': 'lux', 'iso2': 'lu',
                'continent': 'eu',
                'color': '#752277',
                'name': 'Luxembourg',
                'export_val': np.nan,
            }
        ]

        df = df.append(pd.DataFrame(manual_entries), ignore_index=True)
        df.drop('export_val', axis=1, inplace=True)

        return df


class DimCountriesPipeline(BasePipeline):
    @staticmethod
    def pipeline_id():
        return 'countries-dimension-pipeline'

    @staticmethod
    def name():
        return 'Countries Dimension Pipeline'

    @staticmethod
    def description():
        return 'Processes countries data'

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
            'id':            'String',
            'id_num':        'UInt32',
            'iso3':          'String',
            'iso2':          'String',
            'continent':     'String',
            'color':         'String',
            'name':          'String',
        }

        download_data = DownloadStep(connector=source_connector)
        extract_step = ExtractStep()
        load_step = LoadStep(
            "dim_shared_countries", db_connector, if_exists="append",
            dtype=dtype, pk=['id_num']
        )

        pp = AdvancedPipelineExecutor(params)
        pp = pp.next(download_data).next(extract_step).next(load_step)

        return pp.run_pipeline()


if __name__ == '__main__':
    pipeline = DimCountriesPipeline()
    pipeline.run({
        'source_connector': 'dim-countries',
        'db_connector': 'clickhouse-remote',
    })

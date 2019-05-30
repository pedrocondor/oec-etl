import os

import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.helpers import grab_connector
from bamboo_lib.models import AdvancedPipelineExecutor
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import LoadStep

from etl.countries.russia.shared import DownloadStep
from etl.countries.russia.shared import RussiaSubnationalPipeline


DTYPE = {
    'id':   'String',
    'name': 'String'
}


class ExtractStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv(prev, header=0, names=list(DTYPE.keys()))

        for col in DTYPE.keys():
            df[col] = df[col].astype(str)

        df['name'] = df['name'].str[8:]

        return df


class DimRussiaSubnationalRegionsPipeline(RussiaSubnationalPipeline):
    @staticmethod
    def run(params, **kwargs):
        source_connector = Connector.fetch(params.get("source_connector"), open("etl/countries/russia/conns.yaml"))
        db_connector = Connector.fetch(params.get("db_connector"), open("etl/conns.yaml"))

        download_data = DownloadStep(connector=source_connector)
        extract_step = ExtractStep()
        load_step = LoadStep(
            "dim_rus_regions", db_connector, if_exists="append", dtype=DTYPE, pk=['id', 'name']
        )

        pp = AdvancedPipelineExecutor(params)
        pp = pp.next(download_data).next(extract_step).next(load_step)

        return pp.run_pipeline()


if __name__ == '__main__':
    pipeline = DimRussiaSubnationalRegionsPipeline()
    pipeline.run({
        'source_connector': 'russia-meta-region',
        'db_connector': 'clickhouse-remote'
    })

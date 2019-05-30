import copy
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
    'trade_flow_id':           'UInt8',
    'time_id':                 'UInt32',
    'country_id':              'String',
    'hs10_id':                 'String',
    'unit_short_name':         'String',
    'amount':                  'Float64',
    'net_weight':              'Float64',
    'qty':                     'Float64',
    'region_id':               'String',
    'district_id':             'String'
}


class ExtractStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv(prev, header=0, names=list(DTYPE.keys()))

        # Some rows don't have country information
        df.dropna(subset=['country_id'], inplace=True)

        df['trade_flow_id'] = df['trade_flow_id'].map({'IMP': 1, 'EXP': 2}).astype(int)
        df['time_id'] = int('{}{}'.format(params['year'], params['month']))
        df['hs10_id'] = df['hs10_id'].astype(str)
        df['hs6_id'] = df['hs10_id'].astype(str).apply(lambda x: x[:-4])
        df['region_id'] = df['region_id'].astype(str).apply(lambda x: x[:5])
        df['district_id'] = df['district_id'].astype(str).apply(lambda x: x[:2])

        return df


class RussiaSubnationalTradePipeline(RussiaSubnationalPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Source connector", name="source_connector", dtype=str, source=Connector),
            Parameter(label="DB connector", name="db_connector", dtype=str, source=Connector),
            Parameter(label="Year", name="year", dtype=str),
            Parameter(label="Month", name="month", dtype=str),
        ]

    @staticmethod
    def run(params, **kwargs):
        source_connector = Connector.fetch(params.get("source_connector"), open("etl/countries/russia/conns.yaml"))
        db_connector = Connector.fetch(params.get("db_connector"), open("etl/conns.yaml"))

        dtype = copy.deepcopy(DTYPE)
        dtype['hs6_id'] = 'String'

        download_data = DownloadStep(connector=source_connector)
        extract_step = ExtractStep()
        load_step = LoadStep(
            "trade_s_rus_m_hs", db_connector, if_exists="append", dtype=dtype,
            pk=['trade_flow_id', 'time_id', 'country_id', 'region_id', 'district_id', 'hs10_id'],
            nullable_list=['unit_short_name']
        )

        pp = AdvancedPipelineExecutor(params)
        pp = pp.next(download_data).next(extract_step).next(load_step)

        return pp.run_pipeline()


if __name__ == '__main__':
    pipeline = RussiaSubnationalTradePipeline()

    for year in [2015, 2016, 2017, 2018, 2019]:
        if year == 2019:
            months = ['01']
        else:
            months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

        for month in months:
            pipeline.run({
                'source_connector': 'russia-trade',
                'db_connector': '',
                'year': year,
                'month': month
            })

import os

from bamboo_lib.connectors.models import Connector
from bamboo_lib.helpers import grab_connector
from bamboo_lib.models import AdvancedPipelineExecutor
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import LoadStep

from etl.countries.russia.shared import DownloadStep
from etl.countries.russia.shared import RussiaSubnationalPipeline


DTYPE = {
    'direction':               'String',
    'period':                  'String',
    'country':                 'String',
    'hs':                      'String',
    'unit_of_measure':         'String',
    'value':                   'Float64',
    'net_weight':              'Float64',
    'qty':                     'Float64',
    'region':                  'String',
    'district':                'String'
}


class ExtractStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv(prev)
        df.columns = list(DTYPE.keys())
        # TODO: Ignore that first row?
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

        download_data = DownloadStep(connector=source_connector)
        extract_step = ExtractStep()
        load_step = LoadStep(
            "rus_trade", db_connector, if_exists="append", dtype=DTYPE,
            pk=['direction', 'period', 'country', 'region', 'district', 'hs']
        )

        pp = AdvancedPipelineExecutor(params)
        pp = pp.next(download_data).next(extract_step).next(load_step)

        return pp.run_pipeline()


if __name__ == '__main__':
    pipeline = RussiaSubnationalTradePipeline()

    # for year in [2015, 2016, 2017, 2018, 2019]:
    #     if year == 2019:
    #         months = ['01']
    #     else:
    #         months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

    #     for month in months:
    #         pipeline.run({
    #             'source_connector': 'russia-trade',
    #             'db_connector': '',
    #             'year': year,
    #             'month': month
    #         })

    pipeline.run({
        'source_connector': 'russia-trade',
        'db_connector': 'clickhouse-remote',
        'year': '2018',
        'month': '10'
    })

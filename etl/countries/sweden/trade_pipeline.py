import os
import sys

import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.helpers import grab_connector
from bamboo_lib.models import AdvancedPipelineExecutor
from bamboo_lib.models import BasePipeline
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import LoadStep


DTYPE = {
    'commodity_group':         'UInt32',
    'direction':               'String',
    'partner':                 'String',
    'partner_abbr':            'String',
    'year':                    'UInt32',
    'month':                   'UInt32',
    'amount':                  'UInt32'
}


class DownloadStep(PipelineStep):
    def run_step(self, prev_result, params):
        return self.connector.download(params=params)


class ExtractStep(PipelineStep):
    def run_step(self, prev, params):
        try:
            df = pd.read_csv(prev, skiprows=2, sep='\t')
        except UnicodeDecodeError:
            df = pd.read_csv(prev, skiprows=2, sep='\t', encoding="ISO-8859-1")

        direction = "EXP" if params['direction'] == 'Export' else "IMP"
        year = params['year']

        if "07-11" in params['period']:
            months = ["07", "08", "09", "10", "11"]
        elif "01-06" in params['period']:
            months = ["01", "02", "03", "04", "05", "06"]
        else:
            months = [params['period']]

        data = []

        for i, row in df.iterrows():
            for month in months:
                o = {
                    "commodity_group": row["commodity group according to CN"],
                    "direction": direction,
                    "partner": row["trading partner"],
                    # TODO: Use provided abbreviation map
                    "partner_abbr": params['countries'],
                }

                amount = row["{}M{}".format(year, month)]

                try:
                    int(amount)
                except ValueError:
                    amount = "0"

                o["year"] = int(year)
                o["month"] = int(month)
                o["amount"] = int(amount)

                data.append(o)

        final_df = pd.DataFrame(data)
        final_df = final_df[list(DTYPE.keys())]

        return final_df


class SwedenSubnationalTradePipeline(BasePipeline):
    @staticmethod
    def pipeline_id():
        return 'sweden-subnational-pipeline'

    @staticmethod
    def name():
        return 'Sweden Sub-national Pipeline'

    @staticmethod
    def description():
        return 'Processes Sweden sub-national data'

    @staticmethod
    def website():
        return 'http://datawheel.us'

    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Source connector", name="source_connector", dtype=str, source=Connector),
            Parameter(label="DB connector", name="db_connector", dtype=str, source=Connector),
            Parameter(label="Year", name="year", dtype=str),
            Parameter(label="Period", name="period", dtype=str),
            Parameter(label="Direction", name="direction", dtype=str),
            Parameter(label="Countries", name="countries", dtype=str),
        ]

    @staticmethod
    def run(params, **kwargs):
        source_connector = Connector.fetch(params.get("source_connector"), open("etl/countries/sweden/conns.yaml"))
        db_connector = Connector.fetch(params.get("db_connector"), open("etl/conns.yaml"))

        download_data = DownloadStep(connector=source_connector)
        extract_step = ExtractStep()
        load_step = LoadStep(
            "swe_trade", db_connector, if_exists="append", dtype=DTYPE,
            pk=['commodity_group', 'direction', 'partner_abbr'],
            nullable_list=['amount']
        )

        pp = AdvancedPipelineExecutor(params)
        pp = pp.next(download_data).next(extract_step).next(load_step)

        return pp.run_pipeline()


if __name__ == '__main__':
    pipeline = SwedenSubnationalTradePipeline()

    # countries = ['alb', 'dza', 'ago']

    # for year in [2018, 2019]:
    #     for direction in ['Export', 'Import']:
    #         if year == 2018:
    #             for period in ['01-06', '07-11', '12']
    #                 for country in countries:
    #                     pipeline.run({
    #                         'source_connector': 'sweden-trade',
    #                         'db_connector': 'clickhouse-remote',
    #                         'year': year,
    #                         'period': period,
    #                         'direction': direction,
    #                         'countries': country
    #                     })
    #         else:
    #             for period in ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:
    #                 # TODO: Aggregate countries
    #                 pipeline.run({
    #                     'source_connector': 'sweden-trade',
    #                     'db_connector': 'clickhouse-remote',
    #                     'year': year,
    #                     'period': period,
    #                     'direction': direction,
    #                     'countries': []
    #                 })

    pipeline.run({
        'source_connector': 'sweden-trade',
        'db_connector': 'clickhouse-remote',
        'year': '2018',
        'period': '01-06',
        'direction': 'Export',
        'countries': 'bra'
    })

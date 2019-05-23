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
    'hs_code':                 'UInt32',
    'direction':               'String',
    'partner':                 'String',
    'partner_abbr':            'String',
    'year':                    'UInt32',
    'month':                   'UInt32',
    'amount':                  'UInt32'
}

COUNTRIES_DF = pd.read_csv('etl/countries/countries_codes.csv', names=['code', 'country'])

COUNTRIES_ABBR_MAP = dict()

for i, row in COUNTRIES_DF.iterrows():
    COUNTRIES_ABBR_MAP[row['country']] = row['code']

COUNTRIES_ABBR_MAP['Cote d´Ivoire'] = 'civ'
COUNTRIES_ABBR_MAP['Hong Kong Special Administrative Region of China'] = 'hkg'
COUNTRIES_ABBR_MAP['Iran (Islamic Republic of)'] = 'irn'
COUNTRIES_ABBR_MAP['Congo, the Republic of the'] = 'cog'
COUNTRIES_ABBR_MAP['Korea, Republic of Korea'] = 'kor'
COUNTRIES_ABBR_MAP['Libyan Arab Jamahiriya'] = 'lby'
COUNTRIES_ABBR_MAP['Moldova, Republic of'] = 'mda'
COUNTRIES_ABBR_MAP['Russian Federation'] = 'rus'
COUNTRIES_ABBR_MAP['Slovak Republic'] = 'svk'
COUNTRIES_ABBR_MAP['Syrian Arab Republic'] = 'syr'
COUNTRIES_ABBR_MAP['United States of America'] = 'usa'
COUNTRIES_ABBR_MAP['Viet Nam'] = 'vnm'
COUNTRIES_ABBR_MAP['Lao People´s Democratic Republic'] = 'lao'
COUNTRIES_ABBR_MAP['Tanzania, United Republic of'] = 'tza'


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
                    "hs_code": row["commodity group according to CN"],
                    "direction": direction,
                    "partner": row["trading partner"],
                    "partner_abbr": COUNTRIES_ABBR_MAP[row["trading partner"]],
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
            "trade_s_swe_m_hs", db_connector, if_exists="append", dtype=DTYPE,
            pk=['hs_code', 'direction', 'partner_abbr'],
            nullable_list=['amount']
        )

        pp = AdvancedPipelineExecutor(params)
        pp = pp.next(download_data).next(extract_step).next(load_step)

        return pp.run_pipeline()


def process_collection(pipeline, year, direction, period):
    i = 0
    sub_countries = []

    countries = [
        'alb-dza-ago-arg-aus-aut-aze',
        'bgd-blr-bel-bol-bih-bra-bgr',
        'cri-civ-hrv-cub-cze-dnk-dom',
        'ecu-egy-slv-est-eth-fin-fra',
        'gab-geo-deu-gha-grc-gtm-gin',
        'hnd-hkg-hun-ind-idn-irn-irl',
        'isr-ita-jam-jpn-jor-kaz-ken',
        'khm-cmr-can-chl-chn-col-cog',
        'kor-kwt-kgz-lao-lva-lbn-lbr',
        'lby-ltu-mkd-mdg-mwi-mys-mli',
        'mrt-mus-mex-mda-mng-mar-moz',
        'nld-nzl-nic-nga-nor-omn-pak',
        'pan-png-pry-per-phl-pol-prt',
        'qat-rou-rus-sau-sen-srb-sgp',
        'svk-svn-zaf-esp-lka-sdn-che',
        'syr-tjk-tza-tha-tto-tun-tur',
        'tkm-uga-ukr-are-gbr-usa-ury',
        'uzb-ven-vnm-yem-zmb-zwe',
    ]

    for country_collection in countries:
            try:
                pipeline.run({
                    'source_connector': 'sweden-trade',
                    'db_connector': 'clickhouse-remote',
                    'year': year,
                    'period': period,
                    'direction': direction,
                    'countries': country_collection
                })
            except Exception as e:
                print('Downloading {}/{} for {} failed. File may not exist.'.format(period, year, country_collection))
                print(e)


if __name__ == '__main__':
    pipeline = SwedenSubnationalTradePipeline()

    for year in [2018, 2019]:
        for direction in ['Export', 'Import']:
            if year == 2018:
                for period in ['01-06', '07-11']
                    for country in countries:
                        pipeline.run({
                            'source_connector': 'sweden-trade',
                            'db_connector': 'clickhouse-remote',
                            'year': year,
                            'period': period,
                            'direction': direction,
                            'countries': country
                        })

                process_collection(pipeline, year, direction, '12')
            else:
                break
                for period in ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:
                    process_collection(pipeline, year, direction, period)

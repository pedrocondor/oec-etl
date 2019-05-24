import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import AdvancedPipelineExecutor
from bamboo_lib.models import BasePipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import LoadStep
from bamboo_lib.steps import UnzipStep


def hs6_converter(hs6):
    """Adds chaper information to HS6 code"""
    leading2 = int(hs6[:2])
    if leading2 <= 5: return "{}{}".format("01", hs6)
    if leading2 <= 14: return "{}{}".format("02", hs6)
    if leading2 <= 15: return "{}{}".format("03", hs6)
    if leading2 <= 24: return "{}{}".format("04", hs6)
    if leading2 <= 27: return "{}{}".format("05", hs6)
    if leading2 <= 38: return "{}{}".format("06", hs6)
    if leading2 <= 40: return "{}{}".format("07", hs6)
    if leading2 <= 43: return "{}{}".format("08", hs6)
    if leading2 <= 46: return "{}{}".format("09", hs6)
    if leading2 <= 49: return "{}{}".format("10", hs6)
    if leading2 <= 63: return "{}{}".format("11", hs6)
    if leading2 <= 67: return "{}{}".format("12", hs6)
    if leading2 <= 70: return "{}{}".format("13", hs6)
    if leading2 <= 71: return "{}{}".format("14", hs6)
    if leading2 <= 83: return "{}{}".format("15", hs6)
    if leading2 <= 85: return "{}{}".format("16", hs6)
    if leading2 <= 89: return "{}{}".format("17", hs6)
    if leading2 <= 92: return "{}{}".format("18", hs6)
    if leading2 <= 93: return "{}{}".format("19", hs6)
    if leading2 <= 96: return "{}{}".format("20", hs6)
    if leading2 <= 97: return "{}{}".format("21", hs6)
    if leading2 <= 99: return "{}{}".format("22", hs6)
    return "{}{}".format("xx", hs6)


class DownloadStep(PipelineStep):
    def run_step(self, prev_result, params):
        return self.connector.download(params=params)


class ExtractStep(PipelineStep):
    def run_step(self, prev, params):
        names = [
            'classification', 'year', 'time_id', 'period_desc', 'aggregate_level',
            'is_leaf_id', 'trade_flow_id', 'trade_flow', 'reporter_id',
            'reporter', 'reporter_iso', 'partner_id', 'partner', 'partner_iso',
            'commodity_id_pre', 'commodity', 'qty_unit_id', 'qty_unit',
            'qty', 'netweight_kg', 'trade_value_us_dollars', 'flag'
        ]

        df = pd.read_csv(prev, header=0, names=names)

        df['qty_unit_id'] = df['qty_unit_id'].fillna('').astype(str)
        df['qty_unit'] = df['qty_unit'].fillna('').astype(str)

        df = df.drop(df[df['commodity_id_pre'] == 'TOTAL'].index)
        df = df.drop(df[df['aggregate_level'] == 2].index)
        df = df.drop(df[df['aggregate_level'] == 4].index)
        df = df.drop(df[df['partner'] == 'World'].index)

        df = df.dropna(subset=['partner_id', 'trade_value_us_dollars'])

        # Add chapter as prefix to each HS6 id and then turn the final id into an integer
        df['hs6_id'] = df['commodity_id_pre'].astype(str).apply(lambda x: int(hs6_converter(x)))

        names.append('hs6_id')

        remove_names = [
            'aggregate_level', 'classification', 'is_leaf_id',
            'commodity_id_pre', 'commodity', 'flag', 'period_desc',
            'reporter_iso', 'partner_iso', 'reporter', 'partner', 'year',
            'trade_flow'
        ]

        for name in remove_names:
            names.remove(name)

        df = df[names]

        return df


class ComtradeMonthlyPipeline(BasePipeline):
    @staticmethod
    def pipeline_id():
        return 'comtrade-monthly-pipeline'

    @staticmethod
    def name():
        return 'Comtrade Monthly Pipeline'

    @staticmethod
    def description():
        return 'Processes Comtrade monthly data'

    @staticmethod
    def website():
        return 'http://datawheel.us'

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
        source_connector = Connector.fetch(params.get("source_connector"), open("etl/conns.yaml"))
        db_connector = Connector.fetch(params.get("db_connector"), open("etl/conns.yaml"))

        dtype = {
            'time_id':                 'UInt32',
            'trade_flow_id':           'UInt8',
            'reporter_id':             'UInt32',
            'partner_id':              'UInt32',
            'qty_unit_id':             'String',
            'qty_unit':                'String',
            'qty':                     'Float64',
            'netweight_kg':            'Float64',
            'trade_value_us_dollars':  'UInt64',
            'hs6_id':                  'UInt32'
        }

        download_data = DownloadStep(connector=source_connector)
        unzip_step = UnzipStep(pattern=r"\.csv$")
        extract_step = ExtractStep()
        load_step = LoadStep(
            "trade_i_comtrade_m_hs", db_connector, if_exists="append", dtype=dtype,
            pk=['reporter_id', 'trade_flow_id', 'time_id'],
            nullable_list=['qty', 'trade_value_us_dollars']
        )

        pp = AdvancedPipelineExecutor(params)
        pp = pp.next(download_data).foreach(unzip_step).next(extract_step).next(load_step)

        return pp.run_pipeline()


if __name__ == '__main__':
    pipeline = ComtradeMonthlyPipeline()

    for year in range(2008, 2019 + 1):
        if year != 2019:
            months = list(range(1, 12 + 1))
        else:
            months = [1, 2]

        for month in months:
            pipeline.run({
                'source_connector': 'comtrade-monthly',
                'db_connector': 'clickhouse-remote',
                'year': str(year),
                'month': str(month).zfill(2)
            })

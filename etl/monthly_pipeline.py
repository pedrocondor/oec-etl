import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import AdvancedPipelineExecutor
from bamboo_lib.models import BasePipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import LoadStep
from bamboo_lib.steps import UnzipStep


class DownloadStep(PipelineStep):
    def run_step(self, prev_result, params):
        return self.connector.download(params=params)


class ExtractStep(PipelineStep):
    def run_step(self, prev, params):
        names = [
            'classification', 'year', 'period', 'period_desc', 'aggregate_level',
            'is_leaf_code', 'trade_flow_code', 'trade_flow', 'reporter_code',
            'reporter', 'reporter_iso', 'partner_code', 'partner', 'partner_iso',
            'commodity_code_pre', 'commodity', 'qty_unit_code', 'qty_unit',
            'qty', 'netweight_kg', 'trade_value_us_dollars', 'flag'
        ]

        df = pd.read_csv(prev, header=0, names=names)

        df['reporter_iso'] = df['reporter_iso'].fillna('')
        df['partner_iso'] = df['partner_iso'].fillna('')
        df['qty_unit_code'] = df['qty_unit_code'].fillna('').astype(str)
        df['qty_unit'] = df['qty_unit'].fillna('')

        data = []

        for i, row in df.iterrows():
            if row['commodity_code_pre'] == 'TOTAL':
                continue

            row_data = row.to_dict()

            try:
                row_data['commodity_code'] = int(row['commodity_code_pre'])
            except ValueError:
                row_data['commodity_code'] = 0

            data.append(row_data)

        names.append('commodity_code')

        final_df = pd.DataFrame(data)
        final_df = final_df[names]

        return final_df


class MonthlyPipeline(BasePipeline):
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
            'classification':          'String',
            'year':                    'UInt32',
            'period':                  'UInt32',
            'period_desc':             'String',
            'aggregate_level':         'UInt8',
            'is_leaf_code':            'UInt8',
            'trade_flow_code':         'UInt8',
            'trade_flow':              'String',
            'reporter_code':           'UInt32',
            'reporter':                'String',
            'reporter_iso':            'String',
            'partner_code':            'UInt32',
            'partner':                 'String',
            'partner_iso':             'String',
            'commodity_code_pre':      'String',
            'commodity':               'String',
            'qty_unit_code':           'String',
            'qty_unit':                'String',
            'qty':                     'Float64',
            'netweight_kg':            'Float64',
            'trade_value_us_dollars':  'UInt64',
            'flag':                    'UInt8',
            'commodity_code':          'UInt32'
        }

        download_data = DownloadStep(connector=source_connector)
        unzip_step = UnzipStep(pattern=r"\.csv$")
        extract_step = ExtractStep()
        load_step = LoadStep(
            "oec_monthly", db_connector, if_exists="append", dtype=dtype,
            pk=['reporter_code', 'aggregate_level', 'trade_flow_code', 'period'],
            nullable_list=['commodity', 'is_leaf_code', 'partner_code', 'flag']
        )

        pp = AdvancedPipelineExecutor(params)
        pp = pp.next(download_data).foreach(unzip_step).next(extract_step).next(load_step)

        return pp.run_pipeline()


if __name__ == '__main__':
    pipeline = MonthlyPipeline()

    for year in range(2008, 2018 + 1):
        for month in range(1, 12 + 1):
            try:
                pipeline.run({
                    'source_connector': 'comtrade-monthly',
                    'db_connector': 'clickhouse-remote',
                    'year': str(year),
                    'month': str(month)
                })
            except:
                pass

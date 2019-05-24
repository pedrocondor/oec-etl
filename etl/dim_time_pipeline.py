import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import AdvancedPipelineExecutor
from bamboo_lib.models import BasePipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import LoadStep


MONTH_MAP = {
    1: 'January',
    2: 'February',
    3: 'March',
    4: 'April',
    5: 'May',
    6: 'June',
    7: 'July',
    8: 'August',
    9: 'September',
    10: 'October',
    11: 'November',
    12: 'December'
}


def get_quarter(month):
    if month in [1, 2, 3]:
        return 1, 'Q1'
    elif month in [4, 5, 6]:
        return 2, 'Q2'
    elif month in [7, 8, 9]:
        return 3, 'Q3'
    elif month in [10, 11, 12]:
        return 4, 'Q4'
    else:
        raise ValueError('Unrecognized month: {}'.format(month))


class ExtractStep(PipelineStep):
    def run_step(self, prev, params):
        data = []

        for year in range(2000, 2019 + 1):
            for month in range(1, 12 + 1):
                data.append({
                    'year': year,
                    'month': month,
                    'month_name': MONTH_MAP[month],
                    'quarter': get_quarter(month)[0],
                    'quarter_name': get_quarter(month)[1],
                    'time_id': int('{}{}'.format(year, str(month).zfill(2)))
                })

        df = pd.DataFrame(data)

        return df


class DimTimePipeline(BasePipeline):
    @staticmethod
    def pipeline_id():
        return 'time-dimension-pipeline'

    @staticmethod
    def name():
        return 'Time Dimension Pipeline'

    @staticmethod
    def description():
        return 'Processes time data'

    @staticmethod
    def website():
        return 'http://datawheel.us'

    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Source connector", name="source_connector", dtype=str, source=Connector),
            Parameter(label="DB connector", name="db_connector", dtype=str, source=Connector),
            Parameter(label="HS Code", name="hs_code", dtype=str),
        ]

    @staticmethod
    def run(params, **kwargs):
        db_connector = Connector.fetch(params.get("db_connector"), open("etl/conns.yaml"))

        dtype = {
            'year':            'UInt16',
            'month':           'UInt8',
            'month_name':      'String',
            'quarter':         'UInt8',
            'quarter_name':    'String',
            'time_id':         'UInt32',
        }

        extract_step = ExtractStep()
        load_step = LoadStep(
            "dim_shared_time", db_connector,
            if_exists="append", dtype=dtype, pk=['time_id']
        )

        pp = AdvancedPipelineExecutor(params)
        pp = pp.next(extract_step).next(load_step)

        return pp.run_pipeline()


if __name__ == '__main__':
    pipeline = DimTimePipeline()
    pipeline.run({
        'db_connector': 'clickhouse-remote',
    })

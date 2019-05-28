import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import AdvancedPipelineExecutor
from bamboo_lib.models import BasePipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import LoadStep


class DownloadStep(PipelineStep):
    def run_step(self, prev, params):
        return self.connector.download(params=params)


class ExtractStep(PipelineStep):
    def run_step(self, prev, params):
        df = pd.read_csv(prev, sep='\t')

        df['chapter'] = df['chapter'].astype(str).apply(lambda x: x.zfill(2))
        df['hs2'] = df['hs2'].astype(str).apply(lambda x: x.zfill(2))
        df['hs4'] = df['hs4'].astype(str).apply(lambda x: x.zfill(4))
        df['hs6'] = df['hs6'].astype(str).apply(lambda x: x.zfill(6))

        df = df.drop(df[df['hs6'] == '9999AA'].index)

        df['chapter_id'] = df['chapter'].astype(int)
        df['hs2_id'] = df['chapter'] + df['hs2']
        df['hs2_id'] = df['hs2_id'].astype(int)
        df['hs4_id'] = df['chapter'] + df['hs4']
        df['hs4_id'] = df['hs4_id'].astype(int)
        df['hs6_id'] = df['chapter'] + df['hs6']
        df['hs6_id'] = df['hs6_id'].astype(int)

        return df


class DimHSPipeline(BasePipeline):
    @staticmethod
    def pipeline_id():
        return 'hs-dimension-pipeline'

    @staticmethod
    def name():
        return 'HS Dimension Pipeline'

    @staticmethod
    def description():
        return 'Processes HS data'

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
        source_connector = Connector.fetch(params.get("source_connector"), open("etl/conns.yaml"))
        db_connector = Connector.fetch(params.get("db_connector"), open("etl/conns.yaml"))

        dtype = {
            'chapter':         'String',
            'chapter_name':    'String',
            'chapter_id':      'UInt8',
            'hs2':             'String',
            'hs2_name':        'String',
            'hs2_id':          'UInt8',
            'hs4':             'String',
            'hs4_name':        'String',
            'hs4_id':          'UInt16',
            'hs6':             'String',
            'hs6_name':        'String',
            'hs6_id':          'UInt32',
        }

        download_data = DownloadStep(connector=source_connector)
        extract_step = ExtractStep()
        load_step = LoadStep(
            "dim_shared_hs{}".format(params['hs_code']), db_connector,
            if_exists="append", dtype=dtype,
            pk=['hs6_id']
        )

        pp = AdvancedPipelineExecutor(params)
        pp = pp.next(download_data).next(extract_step).next(load_step)

        return pp.run_pipeline()


if __name__ == '__main__':
    pipeline = DimHSPipeline()

    for hs_code in ['92', '96', '02', '07']:
        pipeline.run({
            'source_connector': 'dim-hs',
            'db_connector': 'clickhouse-remote',
            'hs_code': hs_code
        })

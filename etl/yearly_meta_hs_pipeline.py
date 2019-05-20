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
        df = pd.read_csv(prev, sep='\t')

        df['chapter'] = df['chapter'].astype(str).apply(lambda x: x.zfill(2))
        df['hs2'] = df['hs2'].astype(str).apply(lambda x: x.zfill(2))
        df['hs4'] = df['hs4'].astype(str).apply(lambda x: x.zfill(4))
        df['hs6'] = df['hs6'].astype(str).apply(lambda x: x.zfill(6))

        return df


class YearlyMetaHSPipeline(BasePipeline):
    @staticmethod
    def pipeline_id():
        return 'baci-yearly-meta-hs-pipeline'

    @staticmethod
    def name():
        return 'BACI Yearly Meta HS Pipeline'

    @staticmethod
    def description():
        return 'Processes BACI yearly meta HS data'

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
            'hs2':             'String',
            'hs2_name':        'String',
            'hs4':             'String',
            'hs4_name':        'String',
            'hs6':             'String',
            'hs6_name':        'String'
        }

        download_data = DownloadStep(connector=source_connector)
        unzip_step = UnzipStep(pattern=r"\.csv$")
        extract_step = ExtractStep()

        # TODO: What are all the other options
        # load_step = LoadStep(
        #     "oec_yearly_meta_hs{}".format(params['hs_code']), db_connector,
        #     if_exists="append", pk=["id"]
        # )

        pp = AdvancedPipelineExecutor(params)
        pp = pp.next(download_data).next(extract_step)#.next(load_step)

        return pp.run_pipeline()


if __name__ == '__main__':
    pipeline = YearlyMetaHSPipeline()
    pipeline.run({
        'source_connector': 'baci-yearly-meta-hs',
        'db_connector': 'clickhouse-remote',
        'hs_code': '07'
    })

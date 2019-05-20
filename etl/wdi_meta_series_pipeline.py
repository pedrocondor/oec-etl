import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import AdvancedPipelineExecutor
from bamboo_lib.models import BasePipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import LoadStep


class DownloadStep(PipelineStep):
    def run_step(self, prev_result, params):
        return self.connector.download(params=params)


class ExtractStep(PipelineStep):
    def run_step(self, prev, params):
        names = [
            'series_code', 'topic', 'indicator_name', 'short_definition',
            'long_definition', 'unit_of_measure', 'periodicity', 'base_period',
            'other_notes', 'aggregation_method', 'limitations_and_expectations',
            'notes_from_original_source', 'general_comments', 'source',
            'statistical_concept_and_methodology', 'development_relevance',
            'related_source_links', 'other_web_links', 'related_indicators',
            'license_type'
        ]

        df = pd.read_csv(prev, header=0, names=names)

        for name in names:
            df[name]= df[name].fillna('').astype(str)

        return df


class WDIMetaSeriesPipeline(BasePipeline):
    @staticmethod
    def pipeline_id():
        return 'wdi-meta-series-pipeline'

    @staticmethod
    def name():
        return 'WDI Meta Series Pipeline'

    @staticmethod
    def description():
        return 'Processes WDI meta series data'

    @staticmethod
    def website():
        return 'http://datawheel.us'

    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Source connector", name="source_connector", dtype=str, source=Connector),
            Parameter(label="DB connector", name="db_connector", dtype=str, source=Connector),
        ]

    @staticmethod
    def run(params, **kwargs):
        source_connector = Connector.fetch(params.get("source_connector"), open("etl/conns.yaml"))
        db_connector = Connector.fetch(params.get("db_connector"), open("etl/conns.yaml"))

        dtype = {
            'series_code':                         'String',
            'topic':                               'String',
            'indicator_name':                      'String',
            'short_definition':                    'String',
            'long_definition':                     'String',
            'unit_of_measure':                     'String',
            'periodicity':                         'String',
            'base_period':                         'String',
            'other_notes':                         'String',
            'aggregation_method':                  'String',
            'limitations_and_expectations':        'String',
            'notes_from_original_source':          'String',
            'general_comments':                    'String',
            'source':                              'String',
            'statistical_concept_and_methodology': 'String',
            'development_relevance':               'String',
            'related_source_links':                'String',
            'other_web_links':                     'String',
            'related_indicators':                  'String',
            'license_type':                        'String'
        }

        download_data = DownloadStep(connector=source_connector)
        extract_step = ExtractStep()
        load_step = LoadStep(
            "oec_wdi_meta_series", db_connector, if_exists="append", dtype=dtype,
            pk=['series_code']
        )

        pp = AdvancedPipelineExecutor(params)
        pp = pp.next(download_data).next(extract_step).next(load_step)

        return pp.run_pipeline()


if __name__ == '__main__':
    pipeline = WDIMetaSeriesPipeline()
    pipeline.run({
        'source_connector': 'wdi-series',
        'db_connector': 'clickhouse-remote',
    })

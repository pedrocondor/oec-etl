import pandas as pd
import os

from bamboo_lib.models import PipelineStep, AdvancedPipelineExecutor
from bamboo_lib.models import Parameter, BasePipeline
from bamboo_lib.connectors.models import Connector

from bamboo_lib.steps import LoadStep
from bamboo_lib.helpers import grab_connector


class DownloadStep(PipelineStep):
    def run_step(self, prev, params):
        print("DOWNLOAD STEP")
        dim = params.get("dim")
        df = pd.read_csv("./dimension_tables/"+ dim+"_table.csv", encoding = "latin-1")
        return df


class DimPipeline(BasePipeline):
    @staticmethod
    def pipeline_id():
        return 'oec-brazil-ncm-etl-pipeline'

    @staticmethod
    def name():
        return 'OEC Brazil NCM Pipeline'

    @staticmethod
    def description():
        return 'Processes and Loads NCM Brazilian Data'

    @staticmethod
    def website():
        return 'http://datawheel.us'

    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Source connector", name="source-connector", dtype=str, source=Connector),
            Parameter(label="Dimension", name="dim", dtype=str)
        ]

    @staticmethod
    def run(params, **kwargs):
        source_connector = grab_connector(__file__, params.get("source-connector"))
        postgres_connector = grab_connector(__file__, "postgres-local")

        step1 = DownloadStep(connector=source_connector)
        step2 = LoadStep(params.get("dim")+"_table", postgres_connector, if_exists="replace")

        pipeline = AdvancedPipelineExecutor(params)
        pipeline = pipeline.next(step1).next(step2)

        return pipeline.run_pipeline()


def run_dim(params, **kwargs):
    pipeline = DimPipeline()
    pipeline.run(params)


if __name__ == '__main__':
    run_dim({
        "source-connector": "http-local",
        "dim": "date"})
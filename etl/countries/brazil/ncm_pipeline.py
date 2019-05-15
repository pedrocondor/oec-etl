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
        if params.get("flow")+"_"+str(params.get("year"))+".csv" not in os.listdir("./data_temp"):
            r = self.connector.download(params=params)
            new_df = pd.read_csv(r, sep = ";")
            new_df.to_csv("./data_temp/"+params.get("flow")+"_"+str(params.get("year"))+".csv", sep = ";")
            return new_df
        else:
            r = "./data_temp/"+params.get("flow")+"_"+str(params.get("year"))+".csv"
            new_df = pd.read_csv(r, sep = ";")
            return new_df

class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        print("TRANSFORM STEP")
        df = prev

        flow = {"EXP":1, "IMP":2}
        df["FLOW"] = flow[params.get("flow")]
        df["date_id"] = df["CO_ANO"].astype("str") + "-" + df["CO_MES"].astype("str").str.zfill(2)
        df = df.drop(columns = {"CO_ANO","CO_MES", "CO_UNID", "CO_URF", "KG_LIQUIDO", "QT_ESTAT"})

        df = df.rename(columns = {"CO_NCM": "product_id", "CO_PAIS": "country_id", "SG_UF_NCM": "state_id", 
                                "CO_VIA": "transport_id", "VL_FOB": "fob"})

        df = df[["date_id", "product_id", "country_id", "state_id", "transport_id", "fob"]]

        states = pd.read_csv("http://www.mdic.gov.br/balanca/bd/tabelas/UF.csv", sep = ";", encoding="latin-1")
        states = dict(zip(states["SG_UF"], states["CO_UF"]))

        df["state_id"] = df["state_id"].map(states)

        return df


class BrazilPipeline(BasePipeline):
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
            Parameter(label="Year", name="year", dtype=str),
            Parameter(label="Flow", name="flow", dtype=str)
        ]

    @staticmethod
    def run(params, **kwargs):
        source_connector = grab_connector(__file__, params.get("source-connector"))
        postgres_connector = grab_connector(__file__, "postgres-local")

        step1 = DownloadStep(connector=source_connector)
        step2 = TransformStep()
        step3 = LoadStep("brazil_ncm", postgres_connector, if_exists="append")

        pipeline = AdvancedPipelineExecutor(params)
        pipeline = pipeline.next(step1).next(step2).next(step3)

        return pipeline.run_pipeline()


def run_brazil_ncm(params, **kwargs):
    pipeline = BrazilPipeline()
    pipeline.run(params)


if __name__ == '__main__':
    run_brazil_ncm({
        "source-connector": "http-local",
        "year": "1997",
        "flow": "EXP"})
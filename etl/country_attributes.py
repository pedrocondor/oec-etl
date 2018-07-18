import os

import pymysql
import pandas as pd

from bamboo_lib.logger import logger
from bamboo_lib.models import PipelineStep, ComplexPipelineExecutor
from bamboo_lib.steps import LoadStep
from bamboo_lib.connectors.models import Connector


class ExtractStep(PipelineStep):
    def run_step(self, df, params):
        return pd.read_sql_query(
            "SELECT * FROM attr_yo", self.connector
        )


class TransformStep(PipelineStep):
    def run_step(self, df, params):
        columns = [
            "neci", "opp_value", "magic", "pc_constant", "pc_current",
            "notpc_constant"
        ]
        df.drop(columns, axis=1, inplace=True)

        return df


def start_pipeline():
    conn = pymysql.connect(
        host=os.environ.get("MIT_OEC_DB_HOST"),
        user=os.environ.get("MIT_OEC_DB_USER"),
        passwd=os.environ.get("MIT_OEC_DB_PASSWORD"),
        db=os.environ.get("MIT_OEC_DB_NAME"),
        port=int(os.environ.get("MIT_OEC_DB_PORT"))
    )

    conn_path = os.path.join(os.environ.get("OEC_BASE_DIR"), "conns.yaml")
    monetdb_oec_conn = Connector.fetch("monetdb-oec", open(conn_path))

    extract_step = ExtractStep(connector=conn)
    transform_step = TransformStep()
    load_step = LoadStep(
        "attr_yo", monetdb_oec_conn, index=True, schema="oec"
    )

    logger.info("* OEC - attr_yo pipeline starting...")

    pp = ComplexPipelineExecutor({})
    pp = pp.next(extract_step).next(transform_step).next(load_step)
    pp.run_pipeline()


if __name__ == "__main__":
    start_pipeline()

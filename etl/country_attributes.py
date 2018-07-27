import os

import pymysql
import pandas as pd

from bamboo_lib.logger import logger
from bamboo_lib.models import PipelineStep, ComplexPipelineExecutor
from bamboo_lib.steps import LoadStep
from bamboo_lib.connectors.models import Connector


class ExtractStep(PipelineStep):
    def run_step(self, df, params):
        attributes_df = pd.read_sql_query(
            "SELECT * FROM attr_yo", self.connector
        )
        countries_df = pd.read_sql_query(
            "SELECT * FROM attr_country", self.connector
        )
        return attributes_df, countries_df


class TransformStep(PipelineStep):
    def run_step(self, prev_result, params):
        attributes_df, countries_df = prev_result

        columns = [
            "neci", "opp_value", "magic", "pc_constant", "pc_current",
            "notpc_constant"
        ]
        attributes_df.drop(columns, axis=1, inplace=True)

        for index, row in attributes_df.iterrows():
            match = countries_df.loc[countries_df["id"] == row["origin_id"]]
            attributes_df.loc[index, "continent_id"] = row["origin_id"][:2]
            attributes_df.loc[index, "origin_id"] = match.iloc[0]["id_num"]

        return attributes_df


def start_pipeline():
    conn = pymysql.connect(
        host=os.environ.get("ORIGINAL_OEC_DB_HOST"),
        user=os.environ.get("ORIGINAL_OEC_DB_USER"),
        passwd=os.environ.get("ORIGINAL_OEC_DB_PASSWORD"),
        db=os.environ.get("ORIGINAL_OEC_DB_NAME"),
        port=int(os.environ.get("ORIGINAL_OEC_DB_PORT"))
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

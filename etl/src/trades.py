import os

import pymysql
import pandas as pd

from bamboo_lib.logger import logger
from bamboo_lib.models import PipelineStep, ComplexPipelineExecutor
from bamboo_lib.steps import LoadStep
from bamboo_lib.connectors.models import Connector


class ExtractStep(PipelineStep):
    def run_step(self, df, params):
        # NOTE: Skipping this now for simplicity
        # df = pd.read_csv(
        #     os.path.join(os.environ.get("OEC_BASE_DIR"), "data", "hs%s_2016.csv" % params["year"])
        # )

        class_name = params["class_name"]

        params["%s_yodp_df" % class_name] = pd.read_sql_query(
            "SELECT * FROM %s_yodp WHERE year = 2016 AND LENGTH(%s_id) = 8 LIMIT 1000" % (
                class_name, class_name
            ), self.connector
        )

        return df


class TransformStep(PipelineStep):
    def run_step(self, df, params):
        # NOTE: Skipping this now for simplicity
        # df.rename(
        #     columns={
        #         "t": "year", "i": "origin_id", "j": "destination_id",
        #         "v": "export_val", "q": "import_val"
        #     },
        #     inplace=True
        # )

        class_name = params["class_name"]

        # We will be calculating growth data on the fly
        columns = [
            "%s_id_len" % class_name, "export_val_growth_pct",
            "export_val_growth_pct_5", "export_val_growth_val",
            "export_val_growth_val_5", "import_val_growth_pct",
            "import_val_growth_pct_5", "import_val_growth_val",
            "import_val_growth_val_5"
        ]
        df.drop(columns, axis=1, inplace=True)

        return df


def start_pipeline(params):
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
        "%s_yearly_data" % params["class_name"], monetdb_oec_conn, index=True,
        schema="oec"
    )

    logger.info("* OEC pipeline starting...")

    pp = ComplexPipelineExecutor(params)
    pp = pp.next(extract_step).next(transform_step).next(load_step)
    pp.run_pipeline()

    logger.info("* OEC pipeline finished.")


if __name__ == "__main__":
    for year in ["92", "96", "02", "07"]:
        start_pipeline({"year": year, "class_name": "hs%s" % year})

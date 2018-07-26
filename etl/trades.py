import os

import pymysql
import pandas as pd

from bamboo_lib.logger import logger
from bamboo_lib.models import PipelineStep, ComplexPipelineExecutor
from bamboo_lib.steps import LoadStep
from bamboo_lib.connectors.models import Connector


class ExtractStep(PipelineStep):
    def run_step(self, df, params):
        class_name = params["class_name"]

        return pd.read_csv(
            os.path.join(
                os.environ.get("OEC_BASE_DIR"),
                "data",
                "%s_2016.csv" % class_name
            ),
            dtype={"hs6": object, "i": object, "j": object}
        )


class TransformStep(PipelineStep):
    def run_step(self, df, params):
        df.rename(
            columns={
                "t": "year", "i": "origin_id", "j": "destination_id",
                "hs6": "%s_id" % params["class_name"], "v": "export_val",
                "q": "import_val"
            },
            inplace=True
        )

        df[["export_val", "import_val"]] = df[["export_val", "import_val"]].apply(lambda x: x * 1000).round(2)

        return df


def start_pipeline(params):
    conn = pymysql.connect(
        host=os.environ.get("ORIGINAL_OEC_DB_HOST"),
        user=os.environ.get("ORIGINAL_OEC_DB_USER"),
        passwd=os.environ.get("ORIGINAL_OEC_DB_PASSWORD"),
        db=os.environ.get("ORIGINAL_OEC_DB_NAME"),
        port=int(os.environ.get("ORIGINAL_OEC_DB_PORT"))
    )

    conn_path = os.path.join(os.environ.get("OEC_BASE_DIR"), "conns.yaml")
    monetdb_oec_conn = Connector.fetch("monetdb-oec", open(conn_path))

    schema_name = "%s_yearly_data" % params["class_name"]

    dtype = {
        "hs6": "VARCHAR(6)",
        "origin_id": "VARCHAR(3)",
        "destination_id": "VARCHAR(3)",
        "export_val": "DECIMAL",
        "import_val": "DECIMAL"
    }

    extract_step = ExtractStep(connector=conn)
    transform_step = TransformStep()
    load_step = LoadStep(
        schema_name, monetdb_oec_conn, index=True, schema="oec", dtype=dtype
    )

    logger.info("* OEC - %s pipeline starting..." % schema_name)

    pp = ComplexPipelineExecutor(params)
    pp = pp.next(extract_step).next(transform_step).next(load_step)
    pp.run_pipeline()


if __name__ == "__main__":
    for year in ["92", "96", "02", "07"]:
        start_pipeline({"year": year, "class_name": "hs%s" % year})

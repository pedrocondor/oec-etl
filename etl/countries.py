import os

import pandas as pd
import pymysql

from bamboo_lib.logger import logger
from bamboo_lib.models import PipelineStep, ComplexPipelineExecutor
from bamboo_lib.steps import LoadStep
from bamboo_lib.connectors.models import Connector


class ExtractStep(PipelineStep):
    def run_step(self, df, params):
        params["country_name_df"] = pd.read_sql_query(
            "SELECT * FROM attr_country_name", self.connector
        )
        params["country_regions_df"] = pd.read_sql_query(
            "SELECT * FROM attr_country_regions", self.connector
        )
        return df


class TransformStep(PipelineStep):
    def run_step(self, df, params):
        df = params["country_regions_df"].copy()

        df["id_region"] = df["id"]

        for index, row in df.iterrows():
            if len(row["id"]) == 7:
                df.loc[index, "id"] = row["id"][:2] + row["id"][4:]

        # After the transformation above, a 2-digit `id` field refers to a
        # continent, 4 digits refer to a region and 5 digits refer to a
        # country. A country's region (if it belongs to one) now lives in the
        # `id_region` field.

        # Create the new columns for each language
        for language in params["country_name_df"]["lang"].unique():
            df["%s_name" % language] = None
            df["%s_gender" % language] = None
            df["%s_plural" % language] = None
            df["%s_article" % language] = None

        # Populate those new columns with the appropriate language data
        for _, row in params["country_name_df"].iterrows():
            language = row["lang"]
            match = df["id"] == row["origin_id"]
            df.loc[match, "%s_name" % language] = row["name"]
            df.loc[match, "%s_gender" % language] = row["gender"]
            df.loc[match, "%s_plural" % language] = row["plural"]
            df.loc[match, "%s_article" % language] = row["article"]

        return df


def start_pipeline():
    # TODO: Change this to use a MySQL connector from bamboo
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
        "countries", monetdb_oec_conn, index=True, schema="c"
    )

    logger.info("* OEC - countries pipeline starting...")

    pp = ComplexPipelineExecutor({})
    pp = pp.next(extract_step).next(transform_step).next(load_step)
    pp.run_pipeline()


if __name__ == "__main__":
    start_pipeline()

import os

import pandas as pd
import pymysql

from bamboo_lib.logger import logger
from bamboo_lib.models import PipelineStep, ComplexPipelineExecutor
from bamboo_lib.steps import LoadStep
from bamboo_lib.connectors.models import Connector


class ExtractStep(PipelineStep):
    def run_step(self, df, params):
        class_name = params["class_name"]

        params["%s_df" % class_name] = pd.read_sql_query(
            "SELECT * FROM attr_%s" % class_name, self.connector
        )
        params["%s_name_df" % class_name] = pd.read_sql_query(
            "SELECT * FROM attr_%s_name" % class_name, self.connector
        )

        return df


class TransformStep(PipelineStep):
    def run_step(self, df, params):
        class_name = params["class_name"]

        # Select only rows with full id
        mask = params["%s_df" % class_name].id.str.len() == 8
        df = params["%s_df" % class_name][mask].copy()

        # Create the new columns for each language
        for language in params["%s_name_df" % class_name]["lang"].unique():
            for depth in ["chapter", "hs2", "hs4", "hs6"]:
                df["%s" % depth] = None
                df["%s_%s_name" % (depth, language)] = None
                df["%s_%s_keywords" % (depth, language)] = None
                df["%s_%s_desc" % (depth, language)] = None
                df["%s_%s_gender" % (depth, language)] = None
                df["%s_%s_plural" % (depth, language)] = None
                df["%s_%s_article" % (depth, language)] = None

        # Populate those new columns with the appropriate language data
        for index, row in params["%s_name_df" % class_name].iterrows():
            if index % 500 == 0:
                logger.info("%s: %d language rows added" % (params["class_name"], index))

            depth = self.get_depth(row["%s_id" % class_name])
            language = row["lang"]
            match = df["id"].str.startswith(row["%s_id" % class_name])

            df.loc[match, "%s" % depth] = row["%s_id" % class_name]
            df.loc[match, "%s_%s_name" % (depth, language)] = row["name"]
            df.loc[match, "%s_%s_keywords" % (depth, language)] = row["keywords"]
            df.loc[match, "%s_%s_desc" % (depth, language)] = row["desc"]
            df.loc[match, "%s_%s_gender" % (depth, language)] = row["gender"]
            df.loc[match, "%s_%s_plural" % (depth, language)] = row["plural"]
            df.loc[match, "%s_%s_article" % (depth, language)] = row["article"]

        return df

    @staticmethod
    def get_depth(hs_id):
        if len(hs_id) == 2:
            return "chapter"
        elif len(hs_id) == 4:
            return "hs2"
        elif len(hs_id) == 6:
            return "hs4"
        return "hs6"


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

    extract_step = ExtractStep(connector=conn)
    transform_step = TransformStep()
    load_step = LoadStep(
        params["class_name"], monetdb_oec_conn, index=True, schema="public"
    )

    logger.info("* OEC - %s pipeline starting..." % params["class_name"])

    pp = ComplexPipelineExecutor(params)
    pp = pp.next(extract_step).next(transform_step).next(load_step)
    pp.run_pipeline()


if __name__ == "__main__":
    for year in ["92", "96", "02", "07"]:
        start_pipeline({"year": year, "class_name": "hs%s" % year})

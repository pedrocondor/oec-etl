import os

import pandas as pd
import pymysql

from bamboo_lib.logger import logger
from bamboo_lib.models import PipelineStep, ComplexPipelineExecutor
from bamboo_lib.steps import LoadStep
from bamboo_lib.connectors.models import Connector


class ExtractStep(PipelineStep):
    def run_step(self, df, params):
        country_name_df = pd.read_sql_query(
            "SELECT * FROM attr_country_name", self.connector
        )
        country_regions_df = pd.read_sql_query(
            "SELECT * FROM attr_country_regions", self.connector
        )
        return country_name_df, country_regions_df


class TransformStep(PipelineStep):
    def run_step(self, prev_result, params):
        country_name_df, country_regions_df = prev_result

        df = country_regions_df
        df["id_full"] = df["id"]

        for index, row in df.iterrows():
            if len(row["id"]) == 7:
                df.loc[index, "id"] = row["id"][:2] + row["id"][4:]

        continents_df = df.loc[df["id"].str.len() == 2]
        regions_df = df.loc[df["id"].str.len() == 4]
        countries_df = df.loc[df["id"].str.len() == 5].copy()

        # create new columns
        countries_df["continent_id"] = None
        countries_df["region_id"] = None
        countries_df["region_name"] = None

        for language in country_name_df["lang"].unique():
            countries_df["%s_name" % language] = None
            countries_df["%s_gender" % language] = None
            countries_df["%s_plural" % language] = None
            countries_df["%s_article" % language] = None
            countries_df["%s_continent_name" % language] = None
            countries_df["%s_continent_gender" % language] = None
            countries_df["%s_continent_plural" % language] = None
            countries_df["%s_continent_article" % language] = None

        # populate region data
        for _, row in regions_df.iterrows():
            match = countries_df["id_full"].str.startswith(row["id"])
            countries_df.loc[match, "region_id"] = row["id"]
            countries_df.loc[match, "region_name"] = row["comtrade_name"]

        # populate continent data
        for _, row in continents_df.iterrows():
            match = countries_df["id_full"].str.startswith(row["id"])
            countries_df.loc[match, "continent_id"] = row["id"]

            continent_names_df = country_name_df.loc[country_name_df["origin_id"] == row["id"]]

            for _, r in continent_names_df.iterrows():
                countries_df.loc[match, "%s_continent_name" % r["lang"]] = r["name"]
                countries_df.loc[match, "%s_continent_gender" % r["lang"]] = r["gender"]
                countries_df.loc[match, "%s_continent_plural" % r["lang"]] = r["plural"]
                countries_df.loc[match, "%s_continent_article" % r["lang"]] = r["article"]

        # populate language data
        for _, row in country_name_df.iterrows():
            language = row["lang"]
            match = countries_df["id"] == row["origin_id"]
            countries_df.loc[match, "%s_name" % language] = row["name"]
            countries_df.loc[match, "%s_gender" % language] = row["gender"]
            countries_df.loc[match, "%s_plural" % language] = row["plural"]
            countries_df.loc[match, "%s_article" % language] = row["article"]

        country_id = countries_df["id_3char"]
        countries_df.drop(labels=["id", "id_3char"], axis=1, inplace=True)
        countries_df.insert(0, "country_id", country_id)

        return countries_df


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
        "countries", monetdb_oec_conn, index=True, schema="oec"
    )

    logger.info("* OEC - countries pipeline starting...")

    pp = ComplexPipelineExecutor({})
    pp = pp.next(extract_step).next(transform_step).next(load_step)
    pp.run_pipeline()


if __name__ == "__main__":
    start_pipeline()

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
            "SELECT * FROM attr_%s LIMIT 500" % class_name, self.connector
        )
        params["%s_name_df" % class_name] = pd.read_sql_query(
            "SELECT * FROM attr_%s_name" % class_name, self.connector
        )

        return df


class TransformStep(PipelineStep):
    def run_step(self, df, params):
        class_name = params["class_name"]

        original_df = params["%s_df" % class_name]
        original_name_df = params["%s_name_df" % class_name]

        chapter_dict = self.get_dict(original_df.loc[original_df["id"].str.len() == 2])
        hs4_dict = self.get_dict(original_df.loc[original_df["id"].str.len() == 6])
        hs6_dict = self.get_dict(original_df.loc[original_df["id"].str.len() == 8])  # this will be the final df

        if class_name == "hs92":
            for k, v in hs6_dict.items():
                for key, value in v.items():
                    if key in ["image_author", "image_link", "palette"] and value is None:
                        hs4 = hs4_dict[k[:6]]

                        v[key] = hs4[key]

                for key, value in v.items():
                    if key in ["image_author", "image_link", "palette"] and value is None:
                        chapter = chapter_dict[k[:2]]

                        v[key] = chapter[key]

        # Add extra columns
        languages = original_name_df["lang"].unique()

        for key, value in hs6_dict.items():
            for language in languages:
                value["chapter"] = key[:2]
                value["hs2"] = key[2:4]
                value["hs4"] = key[2:6]
                value["hs6"] = key[2:]

                for depth in ["chapter", "hs2", "hs4", "hs6"]:
                    value["%s_%s_name" % (depth, language)] = None
                    value["%s_%s_keywords" % (depth, language)] = None
                    value["%s_%s_desc" % (depth, language)] = None
                    value["%s_%s_gender" % (depth, language)] = None
                    value["%s_%s_plural" % (depth, language)] = None
                    value["%s_%s_article" % (depth, language)] = None

        # Name stuff
        id_column = "%s_id" % class_name

        chapter_name_dict = self.get_name_dict(original_name_df.loc[original_name_df[id_column].str.len() == 2], id_column)
        hs4_name_dict = self.get_name_dict(original_name_df.loc[original_name_df[id_column].str.len() == 6], id_column)
        hs6_name_dict = self.get_name_dict(original_name_df.loc[original_name_df[id_column].str.len() == 8], id_column)

        for k, v in chapter_name_dict.items():
            for key, value in hs6_dict.items():
                if key[:2] == k:
                    for language in languages:
                        if language in v:
                            x = v[language]
                            value["chapter_%s_name" % language] = x["name"]
                            value["chapter_%s_keywords" % language] = x["keywords"]
                            value["chapter_%s_desc" % language] = x["desc"]
                            value["chapter_%s_gender" % language] = x["gender"]
                            value["chapter_%s_plural" % language] = x["plural"]
                            value["chapter_%s_article" % language] = x["article"]

        for k, v in hs4_name_dict.items():
            for key, value in hs6_dict.items():
                if key[:6] == k:
                    for language in languages:
                        if language in v:
                            x = v[language]
                            value["hs4_%s_name" % language] = x["name"]
                            value["hs4_%s_keywords" % language] = x["keywords"]
                            value["hs4_%s_desc" % language] = x["desc"]
                            value["hs4_%s_gender" % language] = x["gender"]
                            value["hs4_%s_plural" % language] = x["plural"]
                            value["hs4_%s_article" % language] = x["article"]

        for k, v in hs6_name_dict.items():
            for key, value in hs6_dict.items():
                if key == k:
                    for language in languages:
                        if language in v:
                            x = v[language]
                            value["hs6_%s_name" % language] = x["name"]
                            value["hs6_%s_keywords" % language] = x["keywords"]
                            value["hs6_%s_desc" % language] = x["desc"]
                            value["hs6_%s_gender" % language] = x["gender"]
                            value["hs6_%s_plural" % language] = x["plural"]
                            value["hs6_%s_article" % language] = x["article"]

        final_df = pd.DataFrame()

        for _, value in hs6_dict.items():
            df = pd.DataFrame(value, index=[0])
            final_df = final_df.append(df, sort=False)

        return final_df

    @staticmethod
    def get_dict(df):
        d = dict()
        h = df.to_dict(orient="records")

        index = 0

        for _, row in df.iterrows():
            d[row["id"]] = h[index]
            index += 1

        return d

    @staticmethod
    def get_name_dict(df, id_column):
        d = dict()
        h = df.to_dict(orient="records")

        index = 0

        for _, row in df.iterrows():
            if not row[id_column] in d:
                d[row[id_column]] = {}
            d[row[id_column]][h[index]["lang"]] = h[index]
            index += 1

        return d


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
        params["class_name"], monetdb_oec_conn, index=True, schema="oec"
    )

    logger.info("* OEC - %s pipeline starting..." % params["class_name"])

    pp = ComplexPipelineExecutor(params)
    pp = pp.next(extract_step).next(transform_step).next(load_step)
    pp.run_pipeline()


if __name__ == "__main__":
    for year in ["92"]:
        start_pipeline({"year": year, "class_name": "hs%s" % year})

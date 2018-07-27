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

        df = pd.read_sql_query(
            "SELECT * FROM attr_%s" % class_name, self.connector
        )
        name_df = pd.read_sql_query(
            "SELECT * FROM attr_%s_name" % class_name, self.connector
        )

        return df, name_df


class TransformStep(PipelineStep):
    def run_step(self, prev_result, params):
        class_name = params["class_name"]

        df, name_df = prev_result

        chapter_dict = self.df_to_dict(df.loc[df["id"].str.len() == 2])
        hs4_dict = self.df_to_dict(df.loc[df["id"].str.len() == 6])
        # this next dict will be converted into the final df
        hs6_dict = self.df_to_dict(df.loc[df["id"].str.len() == 8])

        # the hs92 table contains image and palette information
        # not all products have that information, but if they don't we want to
        # try to populate these columns based on the parent classification row
        if class_name == "hs92":
            hs6_dict = self.populate_image_info(chapter_dict, hs4_dict, hs6_dict)

        languages = ["ar", "de", "el", "en", "es", "fr", "he", "hi", "it",
                     "ja", "ko", "mn", "nl", "pt", "ru", "tr", "vi", "zh"]

        # add extra columns
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

        id_column = "%s_id" % class_name

        chapter_name_dict = self.df_to_dict(
            name_df.loc[name_df[id_column].str.len() == 2], id_column
        )
        hs4_name_dict = self.df_to_dict(
            name_df.loc[name_df[id_column].str.len() == 6], id_column
        )
        hs6_name_dict = self.df_to_dict(
            name_df.loc[name_df[id_column].str.len() == 8], id_column
        )

        # populate language data from chapter, hs4 and hs6
        hs6_dict = self.populate_language_info(
            chapter_name_dict, hs6_dict, languages, 0, 2, "chapter"
        )
        hs6_dict = self.populate_language_info(
            hs4_name_dict, hs6_dict, languages, 0, 6, "hs4"
        )
        hs6_dict = self.populate_language_info(
            hs6_name_dict, hs6_dict, languages, 0, 8, "hs6"
        )

        final_df = pd.DataFrame()

        for _, value in hs6_dict.items():
            df = pd.DataFrame(value, index=[0])
            final_df = final_df.append(df, sort=False)

        return final_df

    @staticmethod
    def df_to_dict(df, id_column=None):
        """ Helper method to convert a dataframe into a dictionary for optimal
        access and update.
        If this method receives the `id_column` argument, it means it's trying
        to convert a name dict, which contains language information. Therefore
        the conversion happens differently. """
        d = dict()
        h = df.to_dict(orient="records")

        index = 0

        for _, row in df.iterrows():
            if id_column:
                if not row[id_column] in d:
                    d[row[id_column]] = {}
                d[row[id_column]][h[index]["lang"]] = h[index]
            else:
                d[row["id"]] = h[index]
            index += 1

        return d

    @staticmethod
    def populate_image_info(chapter_dict, hs4_dict, hs6_dict):
        for k, v in hs6_dict.items():
            for key, value in v.items():
                if key in ["image_author", "image_link", "palette"] and value is None:
                    chapter = chapter_dict[k[:2]]

                    v[key] = chapter[key]

            for key, value in v.items():
                if key in ["image_author", "image_link", "palette"] and value is None:
                    hs4 = hs4_dict[k[:6]]

                    v[key] = hs4[key]

        return hs6_dict

    @staticmethod
    def populate_language_info(d, hs6_dict, languages, start, end, prefix):
        for k, v in d.items():
            for key, value in hs6_dict.items():
                if key[start:end] == k:
                    for language in languages:
                        if language in v:
                            language_data = v[language]
                            value["%s_%s_name" % (prefix, language)] = language_data["name"]
                            value["%s_%s_keywords" % (prefix, language)] = language_data["keywords"]
                            value["%s_%s_desc" % (prefix, language)] = language_data["desc"]
                            value["%s_%s_gender" % (prefix, language)] = language_data["gender"]
                            value["%s_%s_plural" % (prefix, language)] = language_data["plural"]
                            value["%s_%s_article" % (prefix, language)] = language_data["article"]

        return hs6_dict


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
    for year in ["96", "02", "07"]:
        start_pipeline({"year": year, "class_name": "hs%s" % year})

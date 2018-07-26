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

        original_df = params["%s_df" % class_name]
        name_df = params["%s_name_df" % class_name]
        final_df = pd.DataFrame()

        languages = name_df["lang"].unique()
                
        for index, o_row in original_df.iterrows():
            if index % 1000 == 0:
                logger.info("%d rows added" % index)
                
            matches_df = name_df.loc[name_df["%s_id" % class_name] == o_row["id"]]
            
            data = {
                "id": o_row["id"],
                class_name: o_row[class_name],
                "conversion": o_row["conversion"],
                "color": o_row["color"],
                "id_old": o_row["id_old"],
                "id_full": None
            }

            if class_name == "hs92":
                data["image_author"] = o_row["image_author"]
                data["image_link"] = o_row["image_link"]
                data["palette"] = o_row["palette"]
            
            # Create the new columns for each language
            for language in languages:
                for depth in ["chapter", "hs2", "hs4", "hs6"]:
                    data["%s" % depth] = None
                    data["%s_%s_name" % (depth, language)] = None
                    data["%s_%s_keywords" % (depth, language)] = None
                    data["%s_%s_desc" % (depth, language)] = None
                    data["%s_%s_gender" % (depth, language)] = None
                    data["%s_%s_plural" % (depth, language)] = None
                    data["%s_%s_article" % (depth, language)] = None
            
            for _, m_row in matches_df.iterrows():
                depth = self.get_depth(m_row["%s_id" % class_name])
                language = m_row["lang"]

                hs_id = m_row["%s_id" % class_name]

                if depth != "chapter":
                    hs_id = hs_id[2:]
                
                data["%s" % depth] = hs_id
                data["id_full"] = m_row["%s_id" % class_name]
                data["%s_%s_name" % (depth, language)] = m_row["name"]
                data["%s_%s_keywords" % (depth, language)] = m_row["keywords"]
                data["%s_%s_desc" % (depth, language)] = m_row["desc"]
                data["%s_%s_gender" % (depth, language)] = m_row["gender"]
                data["%s_%s_plural" % (depth, language)] = m_row["plural"]
                data["%s_%s_article" % (depth, language)] = m_row["article"]
                
            df = pd.DataFrame(data, index=[0])
            final_df = final_df.append(df, sort=False)

        return final_df

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
        params["class_name"], monetdb_oec_conn, index=True, schema="oec"
    )

    logger.info("* OEC - %s pipeline starting..." % params["class_name"])

    pp = ComplexPipelineExecutor(params)
    pp = pp.next(extract_step).next(transform_step).next(load_step)
    pp.run_pipeline()


if __name__ == "__main__":
    # for year in ["92", "96", "02", "07"]:
    for year in ["92"]:
        start_pipeline({"year": year, "class_name": "hs%s" % year})

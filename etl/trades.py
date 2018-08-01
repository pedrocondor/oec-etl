import logging
import os
import zipfile

import numpy as np
import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.logger import logger
from bamboo_lib.models import PipelineStep, ComplexPipelineExecutor
from bamboo_lib.steps import LoadStep


class DownloadStep(PipelineStep):
    def run_step(self, prev_result, params):
        if not os.path.isfile(params["file_path"]):
            return self.connector.download(params=params)

        return None


class ExtractStep(PipelineStep):
    def run_step(self, prev_result, params):
        if not os.path.isfile(params["file_path"]):
            downloaded_file = prev_result

            zip_ref = zipfile.ZipFile(downloaded_file, "r")
            zip_ref.extractall("data")
            zip_ref.close()

        return pd.read_csv(
            os.path.join(
                os.environ.get("OEC_BASE_DIR"),
                "data",
                "baci%s_2016.csv" % params["year"]
            ),
            dtype={"t": object, "hs6": object, "v": object, "q": object}
        )


class TransformStep(PipelineStep):
    def run_step(self, df, params):
        product_id_column = "%s_id" % params["class_name"]

        df.drop("q", axis=1, inplace=True)

        df.rename(
            columns={
                "t": "year", "i": "origin_id", "j": "destination_id",
                "hs6": product_id_column, "v": "trade_val",
            },
            inplace=True
        )

        df = df.sort_values(product_id_column)

        dic = dict()
        # Sorting the dataframe messes up the indexes, so we need a separate
        # count
        count = 0
        prev_product_class = "0"
        columns = [
            "year", product_id_column, "origin_id", "destination_id",
            "export_val", "import_val"
        ]

        for _, row in df.iterrows():
            current_product_class = row[product_id_column][0]

            # This allows the data injection to happen in parts, instead of
            # trying to uploads ~10 million rows all at once
            if current_product_class != prev_product_class:
                data = list(dic.values())
                final_df = pd.DataFrame(data, columns=columns)

                dic = dict()
                prev_product_class = current_product_class

                yield final_df

            if count % 500000 == 0 and count != 0:
                logging.info("added %f million rows so far" % round((count / 1000000.0), 2))

            pod_key = self._get_pod_key(row, product_id_column)
            pdo_key = self._get_pdo_key(row, product_id_column)

            trade_val = round(float(row["trade_val"]) * 1000, 2)

            for key in [pod_key, pdo_key]:
                try:
                    dic[key]["export_val"] = trade_val
                except KeyError:
                    dic[key] = {
                        "year": row["year"],
                        "origin_id": row["origin_id"] if key == pod_key else row["destination_id"],
                        "destination_id": row["destination_id"] if key == pod_key else row["origin_id"],
                        product_id_column: row[product_id_column],
                        "export_val": trade_val if key == pod_key else np.NaN,
                        "import_val": np.NaN if key == pod_key else trade_val
                    }

            count += 1

        data = list(dic.values())
        final_df = pd.DataFrame(data, columns=columns)

        yield final_df

    @staticmethod
    def _get_pod_key(row, col_name):
        return "%s-%s-%s" % (row[col_name], row["origin_id"], row["destination_id"])

    @staticmethod
    def _get_pdo_key(row, col_name):
        return "%s-%s-%s" % (row[col_name], row["destination_id"], row["origin_id"])


def start_pipeline(params):
    conn_path = os.path.join(os.environ.get("OEC_BASE_DIR"), "conns.yaml")
    conn = Connector.fetch("hs-data", open(conn_path))
    monetdb_oec_conn = Connector.fetch("monetdb-oec", open(conn_path))

    schema_name = "%s_yearly_data" % params["class_name"]

    dtype = {
        "hs6": "VARCHAR(6)",
        "export_val": "DECIMAL",
        "import_val": "DECIMAL"
    }
    params["file_path"] = os.path.join(
        os.environ.get("OEC_BASE_DIR"),
        "data",
        "baci%s_2016.csv" % params["year"]
    )

    download_step = DownloadStep(connector=conn)
    extract_step = ExtractStep()
    transform_step = TransformStep()
    load_step = LoadStep(
        schema_name, monetdb_oec_conn, index=True, schema="oec", dtype=dtype
    )

    logger.info("* OEC - %s pipeline starting..." % schema_name)

    pp = ComplexPipelineExecutor(params)
    pp = pp.next(download_step).next(extract_step).foreach(transform_step).next(load_step).endeach()
    pp.run_pipeline()


if __name__ == "__main__":
    for year in ["92", "96", "02", "07"]:
        start_pipeline({"year": year, "class_name": "hs%s" % year})

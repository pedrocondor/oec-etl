import os
import zipfile

import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.logger import logger
from bamboo_lib.models import PipelineStep, ComplexPipelineExecutor
from bamboo_lib.steps import LoadStep


class DownloadStep(PipelineStep):
    def run_step(self, prev_result, params):
        return self.connector.download(params=params)


class ExtractStep(PipelineStep):
    def run_step(self, prev_result, params):
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
    conn_path = os.path.join(os.environ.get("OEC_BASE_DIR"), "conns.yaml")
    conn = Connector.fetch("hs-data", open(conn_path))
    monetdb_oec_conn = Connector.fetch("monetdb-oec", open(conn_path))

    schema_name = "%s_yearly_data" % params["class_name"]

    dtype = {
        "hs6": "VARCHAR(6)",
        "origin_id": "VARCHAR(3)",
        "destination_id": "VARCHAR(3)",
        "export_val": "DECIMAL",
        "import_val": "DECIMAL"
    }

    download_step = DownloadStep(connector=conn)
    extract_step = ExtractStep()
    transform_step = TransformStep()
    load_step = LoadStep(
        schema_name, monetdb_oec_conn, index=True, schema="oec", dtype=dtype
    )

    logger.info("* OEC - %s pipeline starting..." % schema_name)

    pp = ComplexPipelineExecutor(params)
    pp = pp.next(download_step).next(extract_step).next(transform_step).next(load_step)
    pp.run_pipeline()


if __name__ == "__main__":
    for year in ["92", "96", "02", "07"]:
        start_pipeline({"year": year, "class_name": "hs%s" % year})

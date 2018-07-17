import os

import pandas as pd

from bamboo_lib.logger import logger
from bamboo_lib.models import PipelineStep, ComplexPipelineExecutor
from bamboo_lib.steps import LoadStep
from bamboo_lib.connectors.models import Connector


class ExtractStep(PipelineStep):
    def run_step(self, df, params):
        df = pd.read_csv(
            os.path.join(os.environ.get("OEC_BASE_DIR"), "data", "hs%s_2016.csv" % params["year"])
        )
        return df


class TransformStep(PipelineStep):
    def run_step(self, df, params):
        df.rename(
            columns={
                "t": "year", "i": "origin_id", "j": "destination_id",
                "v": "export_val", "q": "import_val"
            },
            inplace=True
        )
        # Scale export and import values
        # Substitute origin and destination ids with country codes

        return df


def start_pipeline(params):
    # conn_path = os.path.join(os.environ.get("OEC_BASE_DIR"), "conns.yaml")
    # monetdb_oec_conn = Connector.fetch("monetdb-oec", open(conn_path))

    extract_step = ExtractStep()
    transform_step = TransformStep()
    # load_step = LoadStep(
    #     "hs92_yearly_data", monetdb_oec_conn, index=True, schema="oec"
    # )

    logger.info("* OEC pipeline starting...")

    pp = ComplexPipelineExecutor(params)
    pp = pp.next(extract_step).next(transform_step)  # .next(load_step)
    pp.run_pipeline()

    logger.info("* OEC pipeline finished.")


if __name__ == "__main__":
    for year in ["92", "96", "02", "07"]:
        start_pipeline({"year": year})

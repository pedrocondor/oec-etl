import os

from bamboo_lib.helpers import grab_connector
from bamboo_lib.models import AdvancedPipelineExecutor
from bamboo_lib.steps import LoadStep

from etl.countries.russia.shared import DownloadStep
from etl.countries.russia.shared import ExtractStep
from etl.countries.russia.shared import RussiaSubnationalPipeline


class RussiaSubnationalMetaCountryPipeline(RussiaSubnationalPipeline):
    @staticmethod
    def run(params, **kwargs):
        path_to_conns = os.path.join(__file__, "..")
        source_connector = grab_connector(path_to_conns, params.get("source_connector"))
        db_connector = grab_connector(path_to_conns, params.get("db_connector"))

        download_data = DownloadStep(connector=source_connector)
        extract_step = ExtractStep()
        # TODO: What are all the other options
        load_step = LoadStep("rus_meta_country", connector, if_exists="append", pk=["code"])

        pp = AdvancedPipelineExecutor(params)
        pp = pp.next(download_data).next(extract_step).next(load_step)

        return pp.run_pipeline()


if __name__ == '__main__':
    pipeline = RussiaSubnationalMetaCountryPipeline()

    # TODO: Where to put the connector to the remote database?
    pipeline.run({
        'source_connector': 'russia-meta-country',
        'db_connector': ''
    })

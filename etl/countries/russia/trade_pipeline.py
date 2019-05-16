import os

from bamboo_lib.helpers import grab_connector
from bamboo_lib.models import AdvancedPipelineExecutor
from bamboo_lib.steps import LoadStep

from etl.countries.russia.shared import DownloadStep
from etl.countries.russia.shared import ExtractStep
from etl.countries.russia.shared import RussiaSubnationalPipeline


class RussiaSubnationalTradePipeline(RussiaSubnationalPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Source connector", name="source_connector", dtype=str, source=Connector),
            Parameter(label="DB connector", name="db_connector", dtype=str, source=Connector),
            Parameter(label="Year", name="year", dtype=str),
            Parameter(label="Month", name="month", dtype=str),
        ]

    @staticmethod
    def run(params, **kwargs):
        path_to_conns = os.path.join(__file__, "..")
        source_connector = grab_connector(path_to_conns, params.get("source_connector"))
        db_connector = grab_connector(path_to_conns, params.get("db_connector"))

        download_data = DownloadStep(connector=source_connector)
        extract_step = ExtractStep()
        # TODO: What are all the other options
        load_step = LoadStep("rus_trade", connector, if_exists="append", pk=["period"])

        pp = AdvancedPipelineExecutor(params)
        pp = pp.next(download_data).next(extract_step).next(load_step)

        return pp.run_pipeline()


if __name__ == '__main__':
    pipeline = RussiaSubnationalTradePipeline()

    for year in [2015, 2016, 2017, 2018]:
        for month in ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:
            # TODO: Where to put the connector to the remote database?
            pipeline.run({
                'source_connector': 'russia-trade',
                'db_connector': '',
                'year': year,
                'month': month
            })

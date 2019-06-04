import pandas as pd
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import AdvancedPipelineExecutor
from bamboo_lib.models import BasePipeline
from bamboo_lib.models import Parameter
from bamboo_lib.models import PipelineStep
from bamboo_lib.steps import LoadStep


class DownloadStep(PipelineStep):
    def run_step(self, prev, params):
        return self.connector.download(params=params)


class ExtractStep(PipelineStep):
    def run_step(self, prev, params):
        names = [
            'geo_id', 'short_name', 'table_name', 'long_name',
            'two_alpha_code', 'currency_unit', 'special_notes', 'region',
            'income_group', 'wb_2_code', 'national_accounts_base_year',
            'national_accounts_reference_year', 'sna_price_valuation',
            'lending_category', 'other_groups', 'system_of_national_accounts',
            'alternative_conversion_factor', 'ppp_survey_year',
            'balance_of_payments_manual_in_use', 'external_debt_reporting_status',
            'system_of_trade', 'government_accounting_concept',
            'imf_data_dissemination_standard', 'latest_population_census',
            'latest_household_survey', 'income_and_expenditure_source',
            'vital_registration_complete', 'latest_agricultural_census',
            'latest_industrial_data', 'latest_trade_data', 'empty_column'
        ]

        df = pd.read_csv(prev, header=0, names=names)

        df.drop('empty_column', axis=1, inplace=True)

        df['geo_id'] = df['geo_id'].str.lower()
        df['two_alpha_code_lower'] = df['two_alpha_code'].str.lower()
        df['national_accounts_reference_year'] = df['national_accounts_reference_year'].astype(str)

        return df


class DimWDIGeoPipeline(BasePipeline):
    @staticmethod
    def pipeline_id():
        return 'wdi-geo-dimension-pipeline'

    @staticmethod
    def name():
        return 'WDI Geo Dimension Pipeline'

    @staticmethod
    def description():
        return 'Processes WDI geography data'

    @staticmethod
    def website():
        return 'http://datawheel.us'

    @staticmethod
    def parameter_list():
        return [
            Parameter(label="Source connector", name="source_connector", dtype=str, source=Connector),
            Parameter(label="DB connector", name="db_connector", dtype=str, source=Connector),
        ]

    @staticmethod
    def run(params, **kwargs):
        source_connector = Connector.fetch(params.get("source_connector"), open("etl/conns.yaml"))
        db_connector = Connector.fetch(params.get("db_connector"), open("etl/conns.yaml"))

        dtype = {
            'geo_id':                               'String',
            'short_name':                           'String',
            'table_name':                           'String',
            'long_name':                            'String',
            'two_alpha_code':                       'String',
            'two_alpha_code_lower':                 'String',
            'currency_unit':                        'String',
            'special_notes':                        'String',
            'region':                               'String',
            'income_group':                         'String',
            'wb_2_code':                            'String',
            'national_accounts_base_year':          'String',
            'national_accounts_reference_year':     'String',
            'sna_price_valuation':                  'String',
            'lending_category':                     'String',
            'other_groups':                         'String',
            'system_of_national_accounts':          'String',
            'alternative_conversion_factor':        'String',
            'ppp_survey_year':                      'String',
            'balance_of_payments_manual_in_use':    'String',
            'external_debt_reporting_status':       'String',
            'system_of_trade':                      'String',
            'government_accounting_concept':        'String',
            'imf_data_dissemination_standard':      'String',
            'latest_population_census':             'String',
            'latest_household_survey':              'String',
            'income_and_expenditure_source':        'String',
            'vital_registration_complete':          'String',
            'latest_agricultural_census':           'String',
            'latest_industrial_data':               'UInt32',
            'latest_trade_data':                    'UInt32',
        }

        nullable_list = list(dtype.keys())[3:]

        download_data = DownloadStep(connector=source_connector)
        extract_step = ExtractStep()
        load_step = LoadStep(
            "dim_shared_geo", db_connector, if_exists="append", dtype=dtype,
            pk=['geo_id'], nullable_list=nullable_list
        )

        pp = AdvancedPipelineExecutor(params)
        pp = pp.next(download_data).next(extract_step).next(load_step)

        return pp.run_pipeline()


if __name__ == '__main__':
    pipeline = DimWDIGeoPipeline()
    pipeline.run({
        'source_connector': 'wdi-country',
        'db_connector': 'clickhouse-remote',
    })

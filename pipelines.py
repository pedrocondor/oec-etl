from etl.countries import start_pipeline as start_countries_pipeline
from etl.country_attributes import start_pipeline as start_country_attributes_pipeline
from etl.hs_pci import start_pipeline as start_hs_pci_pipeline


if __name__ == "__main__":
    start_countries_pipeline()
    start_country_attributes_pipeline()
    start_hs_pci_pipeline()

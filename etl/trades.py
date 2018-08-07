import logging
import os
import zipfile

import numpy as np
import pandas as pd
import pymysql
from bamboo_lib.connectors.models import Connector
from bamboo_lib.logger import logger
from bamboo_lib.models import PipelineStep, ComplexPipelineExecutor
from bamboo_lib.steps import LoadStep


COUNTRIES_ID_MAP = {
    '24': 'ago', '108': 'bdi', '204': 'ben', '854': 'bfa', '72': 'bwa',
    '140': 'caf', '384': 'civ', '120': 'cmr', '180': 'cod', '178': 'cog',
    '174': 'com', '132': 'cpv', '262': 'dji', '12': 'dza', '818': 'egy',
    '232': 'eri', '732': 'esh', '231': 'eth', '266': 'gab', '288': 'gha',
    '324': 'gin', '270': 'gmb', '624': 'gnb', '226': 'gnq', '404': 'ken',
    '430': 'lbr', '434': 'lby', '426': 'lso', '504': 'mar', '450': 'mdg',
    '466': 'mli', '508': 'moz', '478': 'mrt', '480': 'mus', '454': 'mwi',
    '175': 'myt', '516': 'nam', '562': 'ner', '566': 'nga', '638': 'reu',
    '646': 'rwa', '729': 'sdn', '736': 'sdn', '686': 'sen', '654': 'shn',
    '694': 'sle', '706': 'som', '728': 'ssd', '678': 'stp', '748': 'swz',
    '690': 'syc', '148': 'tcd', '768': 'tgo', '788': 'tun', '834': 'tza',
    '800': 'uga', '577': None, '290': None, '710': 'zaf', '711': 'zaf',
    '894': 'zmb', '716': 'zwe', '10': 'ata', '260': 'atf', '74': 'bvt',
    '334': 'hmd', '239': 'sgs', '80': None, '4': 'afg', '784': 'are',
    '51': 'arm', '31': 'aze', '50': 'bgd', '48': 'bhr', '96': 'brn',
    '64': 'btn', '166': 'cck', '156': 'chn', '162': 'cxr', '196': 'cyp',
    '268': 'geo', '344': 'hkg', '360': 'idn', '699': 'ind', '356': 'ind',
    '86': 'iot', '364': 'irn', '368': 'irq', '376': 'isr', '400': 'jor',
    '392': 'jpn', '398': 'kaz', '417': 'kgz', '116': 'khm', '410': 'kor',
    '414': 'kwt', '418': 'lao', '422': 'lbn', '144': 'lka', '446': 'mac',
    '462': 'mdv', '488': 'mid', '104': 'mmr', '496': 'mng', '458': 'mys',
    '524': 'npl', '512': 'omn', '586': 'pak', '608': 'phl', '408': 'prk',
    '275': 'pse', '634': 'qat', '682': 'sau', '702': 'sgp', '760': 'syr',
    '764': 'tha', '762': 'tjk', '795': 'tkm', '626': 'tls', '792': 'tur',
    '158': 'twn', '490': None, '860': 'uzb', '704': 'vnm', '872': None,
    '879': None, '886': 'yar', '887': 'yem', '720': 'ymd', '8': 'alb',
    '20': 'and', '40': 'aut', '56': 'bel', '100': 'bgr', '70': 'bih',
    '112': 'blr', '58': 'blx', '756': 'che', '757': 'che', '200': 'cze',
    '203': 'cze', '278': 'ddr', '276': 'deu', '208': 'dnk', '724': 'esp',
    '233': 'est', '280': 'fdr', '246': 'fin', '250': 'fra', '251': 'fra',
    '1251': 'fra', '234': 'fro', '826': 'gbr', '292': 'gib', '300': 'grc',
    '191': 'hrv', '348': 'hun', '833': 'imn', '372': 'irl', '352': 'isl',
    '380': 'ita', '381': 'ita', '1381': 'ita', '412': 'ksv', '438': 'lie',
    '440': 'ltu', '442': 'lux', '428': 'lva', '492': None, '498': 'mda',
    '807': 'mkd', '470': 'mlt', '499': 'mne', '528': 'nld', '578': 'nor',
    '579': 'nor', '616': 'pol', '620': 'prt', '642': 'rou', '643': 'rus',
    '744': 'sjm', '674': 'smr', '688': 'srb', '810': 'sun', '703': 'svk',
    '705': 'svn', '752': 'swe', '804': 'ukr', '336': 'vat', '221': None,
    '568': None, '697': None, '890': 'yug', '891': 'yug', '533': 'abw',
    '660': 'aia', '530': 'ant', '28': 'atg', '535': 'bes', '44': 'bhs',
    '652': 'blm', '84': 'blz', '60': 'bmu', '52': 'brb', '124': 'can',
    '188': 'cri', '192': 'cub', '531': 'cuw', '136': 'cym', '212': 'dma',
    '214': 'dom', '308': 'grd', '304': 'grl', '320': 'gtm', '340': 'hnd',
    '332': 'hti', '388': 'jam', '658': 'kna', '659': 'kna', '662': 'lca',
    '534': 'maf', '484': 'mex', '500': 'msr', '474': 'mtq', '532': 'naa',
    '558': 'nic', '591': 'pan', '582': 'pci', '592': 'pcz', '630': 'pri',
    '222': 'slv', '666': 'spm', '796': 'tca', '780': 'tto', '581': None,
    '849': 'umi', '840': 'usa', '841': 'usa', '842': 'usa', '670': 'vct',
    '92': 'vgb', '850': 'vir', '129': None, '471': None, '636': None,
    '637': None, '16': 'asm', '36': 'aus', '184': 'cok', '242': 'fji',
    '583': 'fsm', '312': 'glp', '316': 'gum', '296': 'kir', '584': 'mhl',
    '580': 'mnp', '540': 'ncl', '574': 'nfk', '570': 'niu', '520': 'nru',
    '554': 'nzl', '612': 'pcn', '585': 'plw', '598': 'png', '258': 'pyf',
    '90': 'slb', '772': 'tkl', '776': 'ton', '798': 'tuv', '548': 'vut',
    '876': 'wlf', '882': 'wsm', '527': None, '32': 'arg', '68': 'bol',
    '76': 'bra', '152': 'chl', '170': 'col', '218': 'ecu', '238': 'flk',
    '254': 'guf', '328': 'guy', '604': 'per', '600': 'pry', '740': 'sur',
    '858': 'ury', '862': 'ven', '473': None, '0': 'wld', '536': None,
    '838': None, '837': None, '899': 'xxa', '839': None}


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
            params["file_path"],
            dtype={"t": object, "hs6": object, "v": object}
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

            if count % 200000 == 0 and count != 0:
                logging.info("added %f million rows so far" % round((count / 1000000.0), 2))

            pod_key = self._get_pod_key(row, product_id_column)
            pdo_key = self._get_pdo_key(row, product_id_column)

            trade_val = round(float(row["trade_val"]) * 1000, 2)

            origin_id = COUNTRIES_ID_MAP[str(row["origin_id"])]
            destination_id = COUNTRIES_ID_MAP[str(row["destination_id"])]

            for key in [pod_key, pdo_key]:
                try:
                    col = "export_val" if key == pod_key else "import_val"
                    dic[key][col] = trade_val
                except KeyError:
                    dic[key] = {
                        "year": row["year"],
                        "origin_id": origin_id if key == pod_key else destination_id,
                        "destination_id": destination_id if key == pod_key else origin_id,
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
        "%s_id" % params["class_name"]: "VARCHAR(6)",
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

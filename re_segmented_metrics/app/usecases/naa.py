# pylint: disable=no-member
# utf-8

import logging
from infraestructure.psql import Database
from utils.query import NewApprovedAdsQuery
from utils.read_params import ReadParams


class NewApprovedAds(NewApprovedAdsQuery):
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    # Query data from data warehouse
    @property
    def data_naa(self):
        return self.__data_naa

    @data_naa.setter
    def data_naa(self, config):
        db_source = Database(conf=config)
        data_naa_ = db_source.select_to_dict(self.get_new_approved_ads)
        data_naa_clean = data_naa_\
            .dropna(subset=['list_id'])\
            .reset_index(drop=True)\
            .astype({'list_id': 'int'})
        db_source.close_connection()
        self.__data_naa = data_naa_clean

    def insert_naa(self):
        dwh = Database(conf=self.config.db)
        dwh.insert_copy(self.data_naa, "dm_analysis", "re_segmented_naa")

    def generate(self):
        self.data_naa = self.config.db
        self.insert_naa()

        return True

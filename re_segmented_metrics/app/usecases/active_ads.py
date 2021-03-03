# pylint: disable=no-member
# utf-8

import logging
from infraestructure.psql import Database
from utils.query import ActiveAdsQuery
from utils.read_params import ReadParams


class ActiveAds(ActiveAdsQuery):
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    # Query data from data warehouse
    @property
    def data_active_ads(self):
        return self.__data_active_ads

    @data_active_ads.setter
    def data_active_ads(self, config):
        db_source = Database(conf=config)
        data_active_ads_ = db_source.select_to_dict(self.get_active_ads())
        data_active_ads_clean = data_active_ads_\
            .dropna(subset=['list_id'])\
            .reset_index(drop=True)\
            .astype({'list_id': 'int'})
        db_source.close_connection()
        self.__data_active_ads = data_active_ads_clean

    def insert_active_ads(self):
        dwh = Database(conf=self.config.db)
        dwh.insert_copy(self.data_active_ads, "dm_analysis", "re_segmented_active_ads_dev")

    def generate(self):
        self.data_active_ads = self.config.db
        self.insert_active_ads()

        return True

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
        self.logger.info(f"Active Ads dwh dataframe shape: {data_active_ads_.shape}")
        data_active_ads_clean = data_active_ads_\
            .dropna(subset=['list_id'])\
            .reset_index(drop=True)\
            .astype({'list_id': 'int'})
        self.logger.info(f"Active Ads clean dataframe shape: {data_active_ads_clean.shape}")
        db_source.close_connection()
        self.__data_active_ads = data_active_ads_clean

    def insert_active_ads(self):
        dwh = Database(conf=self.config.db)
        dwh.insert_copy("dm_analysis", "re_segmented_active_ads", self.data_active_ads)

    def generate(self):
        self.data_active_ads = self.config.db
        self.logger.info(f'Active Ads dataframe to insert columns/dtypes:\n {self.data_active_ads.dtypes}')
        self.insert_active_ads()

        return True

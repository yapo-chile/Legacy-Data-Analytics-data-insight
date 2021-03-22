# pylint: disable=no-member
# utf-8

import logging
from infraestructure.psql import Database
from utils.query import DeletedAdsQuery
from utils.read_params import ReadParams


class DeletedAds(DeletedAdsQuery):
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    # Query data from data warehouse
    @property
    def data_deleted_ads(self):
        return self.__data_deleted_ads

    @data_deleted_ads.setter
    def data_deleted_ads(self, config):
        db_source = Database(conf=config)
        data_deleted_ads_ = db_source.select_to_dict(self.get_deleted_ads())
        self.logger.info(f"Deleted Ads dwh dataframe shape: {data_deleted_ads_.shape}")
        data_deleted_ads_clean = data_deleted_ads_\
            .dropna(subset=['list_id'])\
            .reset_index(drop=True)\
            .astype({'list_id': 'int'})\
            .sort_values(by=['deletion_date', 'reason_removed', 'sold_on_site'
                             'price_interval', 'pri_pro'])
        self.logger.info(f"Deleted Ads clean dataframe shape: {data_deleted_ads_clean.shape}")
        db_source.close_connection()
        self.__data_deleted_ads = data_deleted_ads_clean

    def insert_deleted_ads(self):
        dwh = Database(conf=self.config.db)
        dwh.insert_copy("dm_analysis", "re_segmented_deleted_ads", self.data_deleted_ads)

    def generate(self):
        self.data_deleted_ads = self.config.db
        self.logger.info(f'Deleted Ads dataframe to insert columns/dtypes:\n {self.data_deleted_ads.dtypes}')
        self.insert_deleted_ads()

        return True

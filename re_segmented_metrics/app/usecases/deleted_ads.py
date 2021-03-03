# pylint: disable=no-member
# utf-8

import logging
from infraestructure.psql import Database
from utils.deleted_ads import DeletedAdsQuery
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
        data_deleted_ads_ = db_source.select_to_dict(self.get_deleted_ads)
        data_deleted_ads_clean = data_deleted_ads_\
            .dropna(subset=['list_id'])\
            .reset_index(drop=True)\
            .astype({'list_id': 'int'})
        db_source.close_connection()
        self.__data_deleted_ads = data_deleted_ads_clean

    def insert_deleted_ads(self):
        dwh = Database(conf=self.config.db)
        dwh.insert_copy(self.data_deleted_ads, "dm_analysis", "re_segmented_deleted_ads_dev")

    def generate(self):
        self.data_deleted_ads = self.config.db
        self.insert_deleted_ads()

        return True

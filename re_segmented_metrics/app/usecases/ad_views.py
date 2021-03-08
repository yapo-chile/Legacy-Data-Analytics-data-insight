# pylint: disable=no-member
# utf-8

import logging
import pandas as pd
from infraestructure.athena import Athena
from infraestructure.psql import Database
from utils.query import AdViewsQuery
from utils.read_params import ReadParams


class AdViews(AdViewsQuery):
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    # Query the RE Ads from data warehouse
    @property
    def data_segmented_ads(self):
        return self.__data_segmented_ads

    @data_segmented_ads.setter
    def data_segmented_ads(self, config):
        db_source = Database(conf=config)
        data_segmented_ads_ = db_source.select_to_dict(self.get_segmented_ads())
        self.logger.info(f"RE ads dwh dataframe shape: {data_segmented_ads_.shape}")
        data_segmented_ads_clean = data_segmented_ads_\
            .dropna(subset=['list_id'])\
            .reset_index(drop=True)\
            .astype({'list_id': 'int'})
        self.logger.info(f"RE ads clean dataframe shape: {data_segmented_ads_clean.shape}")
        db_source.close_connection()
        self.__data_segmented_ads = data_segmented_ads_clean

    # Query Ad-Views events data from Pulse bucket
    @property
    def data_ad_views(self):
        return self.__data_ad_views

    @data_ad_views.setter
    def data_ad_views(self, config):
        athena = Athena(conf=config)
        data_ad_views_ = athena.get_data(self.get_ad_views())
        self.logger.info(f"Ad-Views RE Athena dataframe shape: {data_ad_views_.shape}")
        data_ad_views_clean = data_ad_views_\
            .dropna(subset=['list_id'])\
            .query("list_id!= 'https'")\
            .reset_index(drop=True) \
            .astype({'list_id': 'int64',
                     'ad_views': 'int64'})
        self.logger.info(f"Ad-Views RE clean dataframe shape: {data_ad_views_clean.shape}")
        athena.close_connection()
        self.__data_ad_views = data_ad_views_clean

    def insert_ad_views(self):
        dwh = Database(conf=self.config.db)
        dwh.insert_copy(self.ad_views_data, "dm_analysis", "re_segmented_ad_views_dev")

    def generate(self):
        self.data_segmented_ads = self.config.db
        self.data_ad_views = self.config.athenaConf

        # Ad Views
        ad_views_merge = pd.merge(left=self.data_ad_views,
                                  right=self.data_segmented_ads[['list_id', 'category', 'region', 'commune',
                                                                 'price_interval', 'estate_type', 'pri_pro']],
                                  how="inner",
                                  on='list_id')
        self.logger.info(f"Ad-Views merge dataframe shape: {ad_views_merge.shape}")
        self.ad_views_data = ad_views_merge
        self.insert_ad_views()

        return True

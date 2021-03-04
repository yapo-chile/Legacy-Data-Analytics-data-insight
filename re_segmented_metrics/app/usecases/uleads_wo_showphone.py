# pylint: disable=no-member
# utf-8

import logging
import pandas as pd
from infraestructure.athena import Athena
from infraestructure.psql import Database
from utils.query import UniqueLeadsWithoutShowPhoneQuery
from utils.read_params import ReadParams


class UniqueLeadsWithoutShowPhone(UniqueLeadsWithoutShowPhoneQuery):
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
        data_segmented_ads_clean = data_segmented_ads_ \
            .dropna(subset=['list_id']) \
            .reset_index(drop=True) \
            .astype({'list_id': 'int'})
        db_source.close_connection()
        self.__data_segmented_ads = data_segmented_ads_clean

    # Query Unique Leads without ShowPhone RE events data from Pulse bucket
    @property
    def data_uleads_wo_showphone(self):
        return self.__data_uleads_wo_showphone

    @data_uleads_wo_showphone.setter
    def data_uleads_wo_showphone(self, config):
        athena = Athena(conf=config)
        data_uleads_ = athena.get_data(self.get_unique_leads())
        data_uleads_clean = data_uleads_\
            .dropna(subset=['list_id'])\
            .query("list_id!= 'https'")\
            .reset_index(drop=True) \
            .astype({'list_id': 'int64',
                     'unique_leads': 'int64'})
        athena.close_connection()
        self.__data_uleads_wo_showphone = data_uleads_clean

    def insert_uleads_wo_showphone(self):
        dwh = Database(conf=self.config.db)
        dwh.insert_copy(self.uleads_data, "dm_analysis", "re_segmented_unique_leads_wo_showphone_dev")

    def generate(self):
        self.data_segmented_ads = self.config.db
        self.data_ad_views = self.config.athenaConf

        # Unique Leads
        uleads_merge = pd.merge(left=self.data_uleads_wo_showphone,
                                right=self.data_segmented_ads,
                                how="inner",
                                on='list_id')
        self.uleads_data = uleads_merge
        self.insert_uleads_wo_showphone()

        return True

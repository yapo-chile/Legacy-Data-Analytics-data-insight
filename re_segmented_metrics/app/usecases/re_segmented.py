# pylint: disable=no-member
# utf-8
import logging
import pandas as pd
from datetime import date, timedelta
from infraestructure.athena import Athena
from infraestructure.psql import Database
from utils.query import RESegmentedQuery
from utils.read_params import ReadParams


class RESegmentedQuery(RESegmentedQuery):
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    # Query data from data warehouse
    @property
    def data_segmented_ads(self):
        return self.__data_segmented_ads

    @data_segmented_ads.setter
    def data_segmented_ads(self, config):
        db_source = Database(conf=config)
        data_segmented_ads_ = db_source.select_to_dict(self.get_segmented_ads)
        data_segmented_ads_clean = data_segmented_ads_\
            .dropna(subset=['list_id'])\
            .reset_index(drop=True)\
            .astype({'list_id': 'int'})
        db_source.close_connection()
        self.__data_segmented_ads = data_segmented_ads_clean

    # Query data from Pulse bucket
    @property
    def data_ad_views(self):
        return self.__data_ad_views

    @data_ad_views.setter
    def data_ad_views(self, config):
        athena = Athena(conf=config)
        data_ad_views_ = athena.get_data(self.get_ad_views)
        data_ad_views_clean = data_ad_views_\
            .dropna(subset=['list_id'])\
            .query("list_id!= 'https'")\
            .reset_index(drop=True) \
            .astype({'list_id': 'int64',
                     'ad_views': 'int64'})
        athena.close_connection()
        self.__data_ad_views = data_ad_views_clean

    # Query data from Pulse bucket
    @property
    def data_uleads_wo_showphone(self):
        return self.__data_uleads_wo_showphone

    @data_uleads_wo_showphone.setter
    def data_uleads_wo_showphone(self, config):
        athena = Athena(conf=config)
        data_uleads_ = athena.get_data(self.get_unique_leads)
        data_uleads_clean = data_uleads_\
            .dropna(subset=['list_id'])\
            .query("list_id!= 'https'")\
            .reset_index(drop=True) \
            .astype({'list_id': 'int64',
                     'unique_leads': 'int64'})
        athena.close_connection()
        self.__data_uleads_wo_showphone = data_uleads_clean

    def insert_naa(self):
        dwh = Database(conf=self.config.db)
        dwh.insert_copy(self.naa_data, "dm_analysis", "re_segmented_naa")

    def insert_deleted_ads(self):
        dwh = Database(conf=self.config.db)
        dwh.insert_copy(self.deleted_ads_data, "dm_analysis", "re_segmented_deleted_ads")

    def insert_uleads_wo_showphone(self):
        dwh = Database(conf=self.config.db)
        dwh.insert_copy(self.uleads_data, "dm_analysis", "re_segmented_unique_leads_wo_showphone")

    def insert_ad_views(self):
        dwh = Database(conf=self.config.db)
        dwh.insert_copy(self.ad_views_data, "dm_analysis", "re_segmented_ad_views")

    def generate(self):
        self.data_segmented_ads = self.config.db
        self.data_uleads_wo_showphone = self.config.athenaConf
        self.data_ad_views = self.config.athenaConf
        update_date = date.today() - timedelta(1)

        # NAA
        naa_data = self.data_segmented_ads[self.data_segmented_ads['date'] == update_date]\
            .groupby(['date', 'price_interval', 'platform', 'pri_pro'])\
            .agg({'list_id': 'count'})\
            .reset_index(drop=False)\
            .rename(columns={'list_id': 'naa'})
        self.naa_data = naa_data
        self.insert_naa()

        # Deleted ads and SOS
        deleted_ads_data = self.data_segmented_ads[self.data_segmented_ads['deletion_date'] == update_date]\
            .groupby(['deletion_date', 'price_interval', 'platform', 'pri_pro', 'sold_on_site'])\
            .agg({'list_id': 'count'})\
            .reset_index(drop=False)\
            .rename(columns={'list_id': 'deleted_ads'})
        self.deleted_ads_data = deleted_ads_data
        self.insert_deleted_ads()

        # Unique Leads
        uleads_merge = pd.merge(left=self.data_uleads_wo_showphone,
                                right=self.data_segmented_ads[['list_id', 'category', 'region', 'commune',
                                                               'price_interval', 'estate_type', 'pri_pro']],
                                how="inner",
                                on='list_id')
        self.uleads_data = uleads_merge
        self.insert_uleads_wo_showphone()

        # Ad Views
        ad_views_merge = pd.merge(left=self.data_ad_views,
                               right=self.data_segmented_ads[['list_id', 'category', 'region', 'commune',
                                                              'price_interval', 'estate_type', 'pri_pro']],
                               how="inner",
                               on='list_id')
        self.ad_views_data = ad_views_merge
        self.insert_ad_views()

        return True

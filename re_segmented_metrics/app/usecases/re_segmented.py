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
            .astype({'list_id': 'int'})
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
            .astype({'list_id': 'int'})
        athena.close_connection()
        self.__data_uleads_wo_showphone = data_uleads_clean


    # Write data to data warehouse
    def save(self) -> None:
        query = Query(self.config, self.params)
        db = Database(conf=self.config.db)
        db.execute_command(query.delete_base())
        db.insert_data(self.data_athena)
        db.insert_data(self.data_dwh)
        db.close_connection()

    def insert_to_stg(self):
        cleaned_data = self.blocket_data_reply
        astypes = {"mail_queue_id": "Int64",
                   "list_id": "Int64",
                   "rule_id": "Int64",
                   "ad_id": "Int64"}
        cleaned_data = cleaned_data.astype(astypes)
        dwh = Database(conf=self.config.db)
        self.logger.info("First records as evidence to STG")
        self.logger.info(cleaned_data.head())
        dwh.execute_command(self.clean_stg_ad_reply())
        dwh.insert_copy(cleaned_data, "stg", "ad_reply")

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

        # Deleted ads and SOS
        sos_data = self.data_segmented_ads[self.data_segmented_ads['deletion_date'] == update_date]\
            .groupby(['date', 'price_interval', 'platform', 'pri_pro', 'sold_on_site'])\
            .agg({'list_id': 'count'})\
            .reset_index(drop=False)\
            .rename(columns={'list_id': 'deleted_ads'})

        # Unique Leads
        uleads_merge = pd.merge(left=self.data_uleads_wo_showphone,
                                right=self.data_segmented_ads[['list_id', 'category', 'region', 'commune',
                                                               'price_interval', 'estate_type']],
                                how="inner",
                                on='list_id')

        # Ad Views
        views_merge = pd.merge(left=self.data_ad_views,
                               right=self.data_segmented_ads[['list_id', 'category', 'region', 'commune',
                                                              'price_interval', 'estate_type']],
                               how="inner",
                               on='list_id')

        #self.save()

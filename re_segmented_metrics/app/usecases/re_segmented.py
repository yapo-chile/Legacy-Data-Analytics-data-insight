# pylint: disable=no-member
# utf-8
import logging
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



    def generate(self):
        self.data_dwh = self.config.db
        self.data_athena = self.config.athenaConf
        self.save()

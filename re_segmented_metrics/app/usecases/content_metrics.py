# pylint: disable=no-member
# utf-8
import logging
from infraestructure.athena import Athena
from infraestructure.psql import Database
from utils.query import SegmentedAdsREQuery
from utils.read_params import ReadParams

class SegmentedAdsRE(SegmentedAdsREQuery):
    def __init__(self,
                 config,
                 params: ReadParams
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
        self.__data_segmented_ads= data_segmented_ads_clean

    # Write data to data warehouse
    def save(self) -> None:
        query = Query(self.config, self.params)
        db = Database(conf=self.config.db)
        db.execute_command(query.delete_base())
        db.insert_data(self.data_athena)
        db.insert_data(self.data_dwh)
        db.close_connection()



    # Query data from Pulse bucket
    @property
    def data_athena(self):
        return self.__data_athena

    @data_athena.setter
    def data_athena(self, config):
        athena = Athena(conf=config)
        query = Query(config, self.params)
        data_athena = athena.get_data(query.query_base_athena())
        athena.close_connection()
        self.__data_athena = data_athena

    def generate(self):
        self.data_dwh = self.config.db
        self.data_athena = self.config.athenaConf
        self.save()

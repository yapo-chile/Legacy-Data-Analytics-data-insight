# pylint: disable=no-member
# utf-8
#import logging
# import pandas as pd
from infraestructure.psql import Database
from utils.query import CarsPyramidsYapoQuery
from utils.read_params import ReadParams


class CarsPyramidsYapo(CarsPyramidsYapoQuery):
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    # Query data from data warehouse
    @property
    def dwh_cars_data_yapo_pyramid(self):
        return self.__dwh_cars_data_yapo_pyramid

    @dwh_cars_data_yapo_pyramid.setter
    def dwh_cars_data_yapo_pyramid(self, config):
        db_source = Database(conf=config)
        dwh_cars_data_yapo_pyramid = db_source.select_to_dict(self.cars_segment_pyramid_yapo())
        db_source.close_connection()
        self.__dwh_cars_data_yapo_pyramid = dwh_cars_data_yapo_pyramid

    def insert_to_dwh(self):
        cleaned_data=self.dwh_cars_data_yapo_pyramid
        astypes= {'ad_id_nk':'Int64',
                  'price':'Int64',
                  'doc_num':'Int64'}
        cleaned_data=cleaned_data.astype(astypes)
        dwh= Database(conf=self.config.db)
        self.logger.info("First record as evidence to dm_analysis")
        self.logger.info(cleaned_data.head())
        dwh.insert_copy(cleaned_data,'dm_analysis','cars_pyramids_yapo')

    def generate(self):
        self.dwh_cars_data_yapo_pyramid = self.config.db
        self.insert_to_dwh()
        self.logger.info('Succesfully saved')
        return True


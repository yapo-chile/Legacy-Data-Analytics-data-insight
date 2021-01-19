# pylint: disable=no-member
# utf-8
import logging
import pandas as pd
import datetime from datetime
from infraestructure.psql import Database
from utils.query import PortalSegmentQuery
from utils.read_params import ReadParams


class PortalSegment(PortalSegmentQuery):
    def __init__(self,
                 config,
                 params: ReadParams,
                 logger) -> None:
        self.config = config
        self.params = params
        self.logger = logger

    # Query data from data blocket
    @property
    def dwh_re_data_yapo_pyramid(self):
        return self.__dwh_re_data_yapo_pyramid

    @dwh_re_data_yapo_pyramid.setter
    def dwh_re_data_yapo_pyramid(self, config):
        db_source = Database(conf=config)
        dwh_re_data_yapo_pyramid = db_source.select_to_dict(self.re_segment_pyramid_yapo(
            datetime.datetime.now().date()
            )
        )
        db_source.close_connection()
        self.__dwh_re_data_yapo_pyramid = dwh_re_data_yapo_pyramid

    def insert_to_dwh(self):
        cleaned_data = self.dwh_re_data_yapo_pyramid

        astypes = {"ad_id_nk": "Int64",
                   "price": "Int64",
                   "uf_price": "Int64",
                   "doc_num": "Int64",
                   "category_id_fk": "Int64"}
        cleaned_data = cleaned_data.astype(astypes)
        dwh = Database(conf=self.config.db)
        self.logger.info("First records as evidence to DM ANALISYS")
        self.logger.info(cleaned_data.head())
        dwh.insert_copy(cleaned_data, "dm_analysis", "real_estate_pyramids_yapo")

    def generate(self):
        self.dwh_re_data_yapo_pyramid = self.config.db
        self.insert_to_dwh()
        self.logger.info("Succesfully saved")
        return True




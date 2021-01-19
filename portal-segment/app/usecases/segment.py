# pylint: disable=no-member
# utf-8
import logging
import pandas as pd
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
    def blocket_data_reply(self):
        return self.__blocket_data_reply

    @blocket_data_reply.setter
    def blocket_data_reply(self, config):
        db_source = Database(conf=config)
        blocket_data_reply = db_source.select_to_dict(self.blocket_ad_reply())
        db_source.close_connection()
        self.__blocket_data_reply = blocket_data_reply

    def insert_to_dwh(self):
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
        self.blocket_data_reply = self.config.blocket
        self.insert_to_dwh()
        self.logger.info("Succesfully saved")
        return True



